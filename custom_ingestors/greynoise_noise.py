from pyspark.sql.functions import col, to_timestamp, explode


def apply_custom_rules(df):
    df_exploded = df.withColumn(
        "temporal_data_exploded", explode(col("raw_data.temporal_data"))
    )

    df_transformed = df_exploded.withColumn(
        "raw_data.temporal_data.element.window.end",
        to_timestamp(col("temporal_data_exploded.window.end")),
    )

    df_final = df_transformed.drop("temporal_data_exploded")

    return df_final
