import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):
    df.printSchema()

    field_name = "iocs.definition.Indicator.IndicatorItem"
    logging.info(f" - formatting column: '{field_name}' -> StringType()")

    # Convert `IndicatorItem` to a JSON string if it's a complex type
    df = df.withColumn(
        field_name,
        to_json(
            col(field_name)
        )
    )

    return df