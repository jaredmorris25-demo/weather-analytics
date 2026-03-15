from pyspark.sql import SparkSession, functions as F
from utils.common import configure_storage, PATHS

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_gold = spark.read.format("delta").load(PATHS["gold"])

    df_city_summary = df_gold \
        .groupBy("city", "country") \
        .agg(
            F.round(F.avg("avg_temperature"), 2).alias("overall_avg_temp"),
            F.max("max_temperature").alias("period_max_temp"),
            F.min("min_temperature").alias("period_min_temp"),
            F.round(F.avg("avg_humidity"), 2).alias("overall_avg_humidity"),
            F.round(F.avg("avg_wind_speed"), 2).alias("overall_avg_wind"),
            F.sum("total_readings").alias("total_readings"),
            F.min("date").alias("period_start"),
            F.max("date").alias("period_end"),
            F.count("*").alias("days_of_data")
        ) \
        .withColumn("report_type", F.lit("city_summary")) \
        .withColumn("generated_at", F.current_timestamp())

    df_weekly = df_gold \
        .withColumn("year_week",
            F.concat(
                F.year("date").cast("string"),
                F.lit("-W"),
                F.lpad(F.weekofyear("date").cast("string"), 2, "0")
            )
        ) \
        .groupBy("city", "country", "year_week") \
        .agg(
            F.round(F.avg("avg_temperature"), 2).alias("weekly_avg_temp"),
            F.max("max_temperature").alias("weekly_max_temp"),
            F.min("min_temperature").alias("weekly_min_temp"),
            F.round(F.avg("avg_humidity"), 2).alias("weekly_avg_humidity"),
            F.sum("total_readings").alias("weekly_readings"),
            F.count("*").alias("days_in_week")
        ) \
        .withColumn("report_type", F.lit("weekly_rollup")) \
        .withColumn("generated_at", F.current_timestamp())

    df_city_summary.write.format("delta").mode("overwrite") \
        .save(PATHS["rpt"] + "city_summary/")
    df_weekly.write.format("delta").mode("overwrite") \
        .save(PATHS["rpt"] + "weekly_rollup/")

    print(f"City summary: {df_city_summary.count()} rows")
    print(f"Weekly rollup: {df_weekly.count()} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)