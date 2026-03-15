from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from utils.common import configure_storage, PATHS, gold_key_udf

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_silver = spark.read.format("delta").load(PATHS["silver"])
    df_valid = df_silver.filter(F.col("data_quality_flag") == "valid")

    df_gold = df_valid \
        .withColumn("date", F.to_date(F.col("timestamp"))) \
        .groupBy("city", "country", "date") \
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.round(F.max("temperature"), 2).alias("max_temperature"),
            F.round(F.min("temperature"), 2).alias("min_temperature"),
            F.round(F.avg("feels_like"), 2).alias("avg_feels_like"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
            F.round(F.avg("pressure"), 2).alias("avg_pressure"),
            F.count("*").alias("total_readings"),
            F.sum(F.when(F.col("data_quality_flag") == "valid", 1)
                   .otherwise(0)).alias("valid_readings")
        ) \
        .withColumn("surrogate_key", gold_key_udf(F.col("city"), F.col("date"))) \
        .withColumn("aggregated_at", F.current_timestamp()) \
        .select(
            "surrogate_key", "city", "country", "date",
            "avg_temperature", "max_temperature", "min_temperature",
            "avg_feels_like", "avg_humidity", "avg_wind_speed", "avg_pressure",
            "total_readings", "valid_readings", "aggregated_at"
        )

    if DeltaTable.isDeltaTable(spark, PATHS["gold"]):
        DeltaTable.forPath(spark, PATHS["gold"]).alias("target").merge(
            df_gold.alias("source"),
            "target.surrogate_key = source.surrogate_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Gold merge complete")
    else:
        df_gold.write.format("delta").mode("overwrite").save(PATHS["gold"])
        print(f"Gold initial write: {df_gold.count()} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)