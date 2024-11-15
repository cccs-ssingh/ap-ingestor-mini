import logging
from pyspark.sql.functions import *

# Function to get the data type of a nested field
def get_nested_field_type(schema, field_path):
    fields = field_path.split(".")
    for field in fields:
        schema = schema[field].dataType
    return schema

def apply_custom_rules(df):
    field_name = "iocs.definition.Indicator.IndicatorItem"
    logging.info(f" - formatting column: '{field_name}' -> StringType()")
    # df = df.withColumn("iocs.definition.Indicator.IndicatorItem", to_json(col("iocs.definition.Indicator.IndicatorItem")))
    # df = df.withColumn(field_name, from_json(col(field_name).cast("string"), ArrayType(StringType(), True)))
    # df = df.withColumn(field_name, to_json(col(field_name)))

    # Check if the field is not StringType and convert if necessary
    nested_field_type = get_nested_field_type(df.schema, field_name)

    if not isinstance(nested_field_type, StringType):
        df = df.withColumn(field_name, to_json(col(field_name)))

    return df