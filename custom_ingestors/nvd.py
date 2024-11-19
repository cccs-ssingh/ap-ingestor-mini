import logging
from pyspark.sql.functions import *


def apply_custom_rules(df):

    field_name = "cveTags"

    logging.info(f" - formated column:'{field_name}' -> StringType()")

    df = df.withColumn(
        field_name,
        from_json(col("cveTags").cast("string"), ArrayType(StringType(), True))
    )

    return df