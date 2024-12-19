from pyspark.sql.functions import col, to_timestamp, explode, struct, array


def apply_custom_rules(df):
    df_exploded = df.withColumn(
        "temporal_data_exploded", explode(col("raw_data.temporal_data"))
    )

    df_transformed = df_exploded.withColumn(
        "temporal_data_exploded_window_end",
        to_timestamp(col("temporal_data_exploded.window.end")),
    )

    df_final = df_transformed.withColumn(
        "raw_data.temporal_data",
        array(
            struct(
                col("temporal_data_exploded.scan").alias("scan"),
                struct(
                    col("temporal_data_exploded_window_end").alias("end"),
                    col("temporal_data_exploded.window.start").alias("start"),
                ).alias("window"),
            )
        ),
    )

    df_final = df_final.drop(
        "temporal_data_exploded", "temporal_data_exploded_window_end"
    )

    return df_final
