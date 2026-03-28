from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from utils.common import configure_storage, PATHS, gold_key_udf

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_silver = spark.read.format("delta").load(PATHS["silver"])
    df_valid = df_silver.filter(F.col("data_quality_flag") == "valid")

    # ── CHANGED: timezone conversion + 6am business day boundary ─────────────
    # UTC is preserved in bronze/silver; business day logic belongs here in gold.
    # convert_timezone() handles DST automatically via IANA rules.
    df_valid = df_valid.withColumn(
        "city_tz",
        F.when(F.col("city") == "Brisbane",  "Australia/Brisbane")
         .when(F.col("city") == "Sydney",    "Australia/Sydney")
         .when(F.col("city") == "Melbourne", "Australia/Melbourne")
         .when(F.col("city") == "Perth",     "Australia/Perth")
         .when(F.col("city") == "Adelaide",  "Australia/Adelaide")
         .otherwise("Australia/Sydney")
    ).withColumn(
        "local_ts",
        F.expr("convert_timezone('UTC', city_tz, timestamp)")
    ).withColumn(
        "reporting_day",
        F.to_date(F.col("local_ts") - F.expr("INTERVAL 6 HOURS"))
    )

    df_gold = df_valid \
        .groupBy("city", "country", "reporting_day") \
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
        .withColumn("surrogate_key", gold_key_udf(F.col("city"), F.col("reporting_day"))) \
        .withColumn("aggregated_at", F.current_timestamp()) \
        .select(
            "surrogate_key", "city", "country", "reporting_day",    # ── CHANGED: was "date"
            "avg_temperature", "max_temperature", "min_temperature",
            "avg_feels_like", "avg_humidity", "avg_wind_speed", "avg_pressure",
            "total_readings", "valid_readings", "aggregated_at"
        )
    
    # ── CHANGED: full overwrite for one-time historical restate ──────────────
    # Swap back to MERGE once rebuild is confirmed correct.
    df_gold.write.format("delta").mode("overwrite").save(PATHS["gold"])
    print(f"Gold full rewrite complete: {df_gold.count()} rows")

    '''
    if DeltaTable.isDeltaTable(spark, PATHS["gold"]):
        DeltaTable.forPath(spark, PATHS["gold"]).alias("target").merge(
            df_gold.alias("source"),
            "target.surrogate_key = source.surrogate_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Gold merge complete")
    else:
        df_gold.write.format("delta").mode("overwrite").save(PATHS["gold"])
        print(f"Gold initial write: {df_gold.count()} rows")
        '''

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)