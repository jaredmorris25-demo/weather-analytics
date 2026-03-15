from pyspark.sql import SparkSession, functions as F
from utils.common import configure_storage, PATHS

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_raw = spark.read.parquet(PATHS["raw"])
    print(f"Raw rows read: {df_raw.count()}")

    df_staging = df_raw \
        .withColumn("loaded_at", F.current_timestamp()) \
        .withColumn("source_file", F.input_file_name()) \
        .withColumn("load_status", F.lit("loaded"))

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
    run(spark, dbutils)