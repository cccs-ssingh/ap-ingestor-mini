import logging
from pyspark.sql.functions import *
from pyspark.sql.types import * 


def apply_custom_rules(df):
    
    # Get all sub-fields from Event
    df = df.select("Event.*")
    df = df.withColumn("timestamp", to_timestamp(from_unixtime("timestamp")))
    
    return df