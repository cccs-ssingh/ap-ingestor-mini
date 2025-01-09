from pyspark.sql.functions import to_timestamp, col, explode

def apply_custom_rules(df):
    # Explode the nested list to handle individual elements
    df = df.withColumn("temporal_data", explode("raw_data.temporal_data"))
    
    # Convert the nested timestamp fields to timestamp type
    df = df.withColumn("temporal_data.window.start", to_timestamp(col("temporal_data.window.start")))
    df = df.withColumn("temporal_data.window.end", to_timestamp(col("temporal_data.window.end")))
    
    return df