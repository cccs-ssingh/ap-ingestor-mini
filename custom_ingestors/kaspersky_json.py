import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):
    logging.info(f" - formatting column: 'iocs.definition.Indicator.IndicatorItem' -> StringType()")

    # df = df.withColumn("iocs.definition.Indicator.IndicatorItem", to_json(col("iocs.definition.Indicator.IndicatorItem")))
    sub_field = "iocs.definition.Indicator.IndicatorItem"
    df = df.withColumn(sub_field, from_json(col(sub_field).cast("string"), ArrayType(StringType(), True)))

    return df