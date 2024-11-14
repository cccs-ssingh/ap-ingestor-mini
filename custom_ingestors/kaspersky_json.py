import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):
    logging.info(f" - formatting column: 'iocs' -> StringType()")

    df = df.withColumn("iocs.definition.Indicator.IndicatorItem", to_json(col("iocs.definition.Indicator.IndicatorItem")))

    return df