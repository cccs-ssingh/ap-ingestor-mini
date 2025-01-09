from pyspark.sql.functions import to_json, col

def apply_custom_rules(df):
    df = df.withColumn("temporal_data_string", to_json(col("raw_data.temporal_data")))
    
    return df