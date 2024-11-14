import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):
    logging.info(f" - formated column:'cveTags' -> StringType()")
    df = df.withColumn("definition.Indicator.IndicatorItem", to_json(col("definition.Indicator.IndicatorItem")))
    return df