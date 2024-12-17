from pyspark.sql.functions import col, from_utc_timestamp, date_format

def apply_custom_rules(df):
    df = df.withColumn(
        "raw_data.temporal_data.element.window.end", 
        date_format(
            from_utc_timestamp(col("raw_data.temporal_data.element.window.end"), "America/Toronto"),  
            "yyyy-MM-dd HH:mm:ss"  
        )
    )

    return df
