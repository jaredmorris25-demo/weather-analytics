import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


STORAGE_ACCOUNT = "adlsweatherjm1535"

PATHS = {
    "raw":     f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
    "staging": f"abfss://staging@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
    "bronze":  f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
    "silver":  f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
    "gold":    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
    "bi":      f"abfss://bi-analytics@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
    "rpt":     f"abfss://rpt@{STORAGE_ACCOUNT}.dfs.core.windows.net/weather_records/",
}


def configure_storage(spark, dbutils):
    """Configure ADLS access. Call at the top of every script."""
    account_key = dbutils.secrets.get(
        scope="weather_scope",
        key="adls_account_key"
    )
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        account_key
    )
    spark.conf.set(
        "fs.azure.account.auth.type",
        "SharedKey"
    )
    spark.conf.set(
        f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
        "SharedKey"
    )


def make_surrogate_key(city, timestamp):
    """Deterministic MD5 hash from natural key components."""
    if city is None or timestamp is None:
        return None
    return hashlib.md5(f"{city}|{str(timestamp)}".encode()).hexdigest()


def make_gold_surrogate_key(city, date):
    """Deterministic MD5 hash for gold layer (city + date)."""
    if city is None or date is None:
        return None
    return hashlib.md5(f"{city}|{str(date)}".encode()).hexdigest()


surrogate_key_udf = F.udf(make_surrogate_key, StringType())
gold_key_udf = F.udf(make_gold_surrogate_key, StringType())