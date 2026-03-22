from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from utils.common import configure_storage, PATHS

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_gold = spark.read.format("delta").load(PATHS["gold"])

    df_bi = df_gold \
        .withColumn("temperature_range",
            F.round(F.col("max_temperature") - F.col("min_temperature"), 2)) \
        .withColumn("temperature_band",
            F.when(F.col("avg_temperature") < 10, "cold")
             .when(F.col("avg_temperature") < 20, "mild")
             .when(F.col("avg_temperature") < 28, "warm")
             .otherwise("hot")) \
        .withColumn("humidity_band",
            F.when(F.col("avg_humidity") < 30, "dry")
             .when(F.col("avg_humidity") < 60, "comfortable")
             .when(F.col("avg_humidity") < 80, "humid")
             .otherwise("very_humid")) \
        .withColumn("data_freshness_days",
            F.datediff(F.current_date(), F.col("date"))) \
        .withColumn("created_at", F.current_timestamp())

    if DeltaTable.isDeltaTable(spark, PATHS["bi"]):
        DeltaTable.forPath(spark, PATHS["bi"]).alias("target").merge(
            df_bi.alias("source"),
            "target.surrogate_key = source.surrogate_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("BI merge complete")
    else:
        df_bi.write.format("delta").mode("overwrite").save(PATHS["bi"])
        print(f"BI initial write: {df_bi.count()} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)