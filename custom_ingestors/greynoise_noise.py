from pyspark.sql.functions import col, to_timestamp, explode, struct, collect_list


def apply_custom_rules(df):
    df_exploded = df.withColumn(
        "temporal_data_exploded", explode(col("raw_data.temporal_data"))
    )

    df_transformed = df_exploded.withColumn(
        "temporal_data_exploded_window_end",
        to_timestamp(col("temporal_data_exploded.window.end")),
    )

    df_transformed = df_transformed.withColumn(
        "temporal_data_exploded",
        struct(
            col("temporal_data_exploded.scan").alias("scan"),
            struct(
                col("temporal_data_exploded_window_end").alias("end"),
                col("temporal_data_exploded.window.start").alias("start"),
            ).alias("window"),
        ),
    )

    df_grouped = df_transformed.groupBy(
        *[col for col in df.columns if col != "raw_data.temporal_data"]
    ).agg(collect_list("temporal_data_exploded").alias("temporal_data"))

    df_final = df_grouped.withColumn(
        "raw_data.temporal_data", col("temporal_data")
    ).drop("temporal_data")

    return df_final
