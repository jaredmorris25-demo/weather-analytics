from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from utils.common import configure_storage, PATHS, surrogate_key_udf

def run(spark, dbutils):
    configure_storage(spark, dbutils)

    df_staging = spark.read.format("delta").load(PATHS["staging"])

    df_bronze = df_staging \
        .drop("id", "load_status", "source_file", "loaded_at") \
        .withColumn("surrogate_key", surrogate_key_udf(F.col("city"), F.col("timestamp"))) \
        .withColumn("ingested_at", F.current_timestamp()) \
        .withColumn("source_system", F.lit("weather_api_sql")) \
        .select(
            "surrogate_key", "city", "country", "temperature", "feels_like",
            "humidity", "description", "wind_speed", "wind_direction",
            "pressure", "visibility", "weather_category", "timestamp",
            "ingested_at", "source_system"
        )

    if DeltaTable.isDeltaTable(spark, PATHS["bronze"]):
        DeltaTable.forPath(spark, PATHS["bronze"]).alias("target").merge(
            df_bronze.alias("source"),
            "target.surrogate_key = source.surrogate_key"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Bronze merge complete")
    else:
        df_bronze.write.format("delta").mode("overwrite").save(PATHS["bronze"])
        print(f"Bronze initial write: {df_bronze.count()} rows")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    run(spark, dbutils)