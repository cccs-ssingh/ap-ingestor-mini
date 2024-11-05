import logging
from pyspark.sql.functions import col, from_json, to_json, ArrayType, StringType


def apply_custom_rules(df):
    # df = df.withColumn("cveTags", from_json(col("cveTags").cast("string"), ArrayType(StringType(), True)))
    # df = df.withColumn("configurations", from_json(col("configurations").cast("string"), ArrayType(StringType(), True)))
    # df = df.withColumn("metrics", to_json(col("metrics")))
    logging.info(f' - formatting column:type')
    for column_name in ['configurations', 'cveTags', 'metrics']:
        logging.info(f' - {column_name} -> StringType()')
        df = df.withColumn(column_name, to_json(col(column_name)))
    return df