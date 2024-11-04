import logging
from pyspark.sql.functions import col, from_json, to_json, ArrayType, StringType


def apply_custom_rules(df):
    logging.info("applying custom rules to df")
    df = df.withColumn("cveTags", from_json(col("cveTags").cast("string"), ArrayType(StringType(), True)))
    df = df.withColumn("configurations", from_json(col("configurations").cast("string"), ArrayType(StringType(), True)))
    df = df.withColumn("metrics", to_json(col("metrics")))
    return df