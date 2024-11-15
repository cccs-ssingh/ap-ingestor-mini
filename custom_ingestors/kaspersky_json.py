import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):
    field_name = "iocs.definition.Indicator.IndicatorItem"
    logging.info(f" - formatting column: '{field_name}' -> StringType()")
    # df = df.withColumn("iocs.definition.Indicator.IndicatorItem", to_json(col("iocs.definition.Indicator.IndicatorItem")))
    # df = df.withColumn(field_name, from_json(col(field_name).cast("string"), ArrayType(StringType(), True)))
    df = df.withColumn(field_name, to_json(col(field_name)))
    return df