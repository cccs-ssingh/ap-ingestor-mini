import logging

from pyspark.sql.functions import col, to_json, struct

def apply_custom_rules(df):

    old_col = "raw_data.temporal_data"
    new_col = "raw_data.temporal_data_str"

    logging.info(f"casting column: {old_col} -> string with new col name: {new_col}")

    # Add the 'temporal_data_str' field inside 'raw_data' and remove the 'temporal_data' field
    df = df.withColumn(
        "raw_data",  # We're modifying the 'raw_data' struct
        struct(
            col("raw_data.hassh"),
            col("raw_data.ja3"),
            col("raw_data.scan"),
            # Do not include the original 'temporal_data' field
            to_json(col("raw_data.temporal_data")).alias("temporal_data_str"),  # New field
            col("raw_data.web")
        ).alias("raw_data")  # Reassign the modified struct back to 'raw_data'
    )

    # Show the schema to check the result
    df.printSchema()

    # # Convert col to a JSON string and put it in new column
    # df = df.withColumn(
    #     "raw_data.temporal_data_str",  # New column name
    #     to_json(col("raw_data.temporal_data"))
    # )


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

    # logging.info('dropping original column: raw_data.temporal_data')
    # df.drop("raw_data.temporal_data")
    df.printSchema()

    return df