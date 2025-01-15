import logging

from pyspark.sql.functions import col, to_json, when, from_json, ArrayType, StringType

def apply_custom_rules(df):

    old_col = "raw_data.temporal_data"
    new_col = "raw_data.temporal_data_str"

    logging.info(f"casting column: {old_col} -> string with new col name: {new_col}")

    # Convert col to a JSON string and put it in new column
    df = df.withColumn(
        "raw_data.temporal_data_str",  # New column name
        to_json(col("raw_data.temporal_data"))
    )

    logging.info('dropping original column: raw_data.temporal_data')
    df.drop("raw_data.temporal_data")
    df.printSchema()

    # df = df.withColumn(
    #     "raw_data.temporal_data_str",
    #     when(
    #         col("raw_data.temporal_data").isNotNull(),
    #         to_json(
    #             col("raw_data.temporal_data")
    #         )
    #     )
    #     .otherwise('[]')  # default empty array or a specific string in case of null
    # )

    # df = df.withColumn(
    #     "raw_data.temporal_data_str",
    #     from_json(
    #         col("raw_data.temporal_data").cast("string"),
    #         ArrayType(StringType(), True)
    #     )
    # )

    return df