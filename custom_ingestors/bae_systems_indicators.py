import logging
from pyspark.sql.functions import *
from pyspark.sql.types import * 


def apply_custom_rules(df):
    
    # Get all sub-fields from Event
    df = df.select("Event.*", "timeperiod_loaded_by")
    
    # Convert all columns into StringType
    for column in df.columns:
        if isinstance(df.schema[column].dataType, (ArrayType, StructType)):
            df = df.withColumn(column, to_json(df[column]))
        
        elif column == "timeperiod_loaded_by":
            continue
        
        elif not isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column, df[column].cast(StringType()))
    
    # convert root level date or timestamp columns
    df = df.withColumn("date", to_date("date"))
    df = df.withColumn("timestamp", to_timestamp(from_unixtime("timestamp")))
    df = df.withColumn("publish_timestamp", to_timestamp(from_unixtime("publish_timestamp")))
    
    return df