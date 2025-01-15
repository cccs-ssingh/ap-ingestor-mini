import logging

from pyspark.sql.functions import col, to_json, when

def apply_custom_rules(df):

    old_col = "raw_data.temporal_data"
    new_col = "raw_data.temporal_data_str"

    logging.info(f"casting column: {old_col} -> string with new col name: {new_col}")

    # df = df.withColumn(
    #     new_col,  # New column name
    #     to_json(
    #         col(old_col) # Convert the 'temporal_data' to a JSON string
    #     )
    # )

    df = df.withColumn(
        "raw_data.temporal_data_str",
        when(
            col("raw_data.temporal_data").isNotNull(),
            to_json(
                col("raw_data.temporal_data")
            )
        )
        .otherwise('[]')  # default empty array or a specific string in case of null
    )

    return df