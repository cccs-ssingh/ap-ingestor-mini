from pyspark.sql.functions import col, when

def apply_custom_rules(df):

    df = df.withColumn(
        "raw_data.temporal_data_str",
        when(
            col("raw_data.temporal_data").isNotNull(),
            col("raw_data.temporal_data").cast("string")
        ).otherwise(None)
    )

    return df