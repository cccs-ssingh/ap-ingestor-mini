from pyspark.sql.functions import col, from_utc_timestamp, date_format


def apply_custom_rules(df):
    # Assuming we want to work with the first element of the 'temporal_data' array:
    df = df.withColumn(
        "raw_data.temporal_data.element.window.end",
        date_format(
            from_utc_timestamp(
                col("raw_data.temporal_data")
                .getItem(0)
                .getField("window")
                .getField("end"),
                "America/Toronto",
            ),
            "yyyy-MM-dd HH:mm:ss",
        ),
    )

    return df
