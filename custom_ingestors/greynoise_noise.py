from pyspark.sql.functions import to_json, col

def apply_custom_rules(df):
    # Explode the nested list to handle individual elements
    df = df.withColumn("temporal_data", to_json(col("raw_data.temporal_data")))

    return df