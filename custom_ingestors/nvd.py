import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):
    logging.info(f" - formated column:'cveTags' -> StringType()")

    df = df.withColumn("cveTags", from_json(col("cveTags").cast("string"), ArrayType(StringType(), True)))

    return df