import logging
from pyspark.sql.functions import col, to_json


def apply_custom_rules(df):
    logging.info(f' - formatting type -> column')
    for column_name in ['configurations', 'cveTags', 'metrics']:
        logging.info(f' - StringType() -> {column_name}')
        df = df.withColumn(column_name, to_json(col(column_name)))
    return df