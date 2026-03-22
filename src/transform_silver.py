from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.common import configure_storage, PATHS

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_bronze = spark.read.format("delta").load(PATHS["bronze"])

    df_silver = df_bronze \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("data_quality_flag",
            F.when((F.col("temperature") < -50) | (F.col("temperature") > 60), "invalid")
             .when((F.col("humidity") < 0) | (F.col("humidity") > 100), "invalid")
             .when(F.col("wind_speed") < 0, "invalid")
             .when((F.col("temperature") < -20) | (F.col("temperature") > 50), "suspect")
             .otherwise("valid")
        ) \
        .withColumn("data_quality_notes",
            F.when((F.col("temperature") < -50) | (F.col("temperature") > 60),
                "Temperature out of physical range")
             .when((F.col("humidity") < 0) | (F.col("humidity") > 100),
                "Humidity out of range")
             .when(F.col("wind_speed") < 0, "Negative wind speed")
             .when((F.col("temperature") < -20) | (F.col("temperature") > 50),
                "Temperature suspect for Australian conditions")
             .otherwise(None)
        ) \
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("timestamp"))) \
        .withColumn("row_num",
            F.row_number().over(
                Window.partitionBy("city", "hour_bucket").orderBy("timestamp")
            )
        ) \
        .filter(F.col("row_num") == 1) \
        .drop("row_num", "hour_bucket")

    if DeltaTable.isDeltaTable(spark, PATHS["silver"]):
        DeltaTable.forPath(spark, PATHS["silver"]).alias("target").merge(
            df_silver.alias("source"),
            "target.surrogate_key = source.surrogate_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Silver merge complete")
    else:
        df_silver.write.format("delta").mode("overwrite").save(PATHS["silver"])
        print(f"Silver initial write: {df_silver.count()} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)