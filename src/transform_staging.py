from pyspark.sql import SparkSession, functions as F
from utils.common import configure_storage, PATHS, STORAGE_ACCOUNT
from datetime import datetime, timezone

def run(spark, dbutils, run_date=None):
    configure_storage(spark, dbutils)

    # Use provided run_date or default to today UTC
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Build path to today's dated snapshot
    dated_path = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/{run_date}/dbo.weather_records.parquet"
    print(f"Reading from: {dated_path}")

    # Read the specific dated file
    df_raw = spark.read.parquet(dated_path)
    print(f"Raw rows read: {df_raw.count()}")

    df_staging = df_raw \
        .withColumn("loaded_at", F.current_timestamp()) \
        .withColumn("source_file", F.input_file_name()) \
        .withColumn("run_date", F.lit(run_date)) \
        .withColumn("load_status", F.lit("loaded"))

    # Show exactly what file we ingested
    print("Source file breakdown:")
    df_staging.groupBy("source_file").count().show(truncate=False)

    df_staging.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(PATHS["staging"])

    print(f"Staging written: {df_staging.count()} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    import sys
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(spark, dbutils, run_date)