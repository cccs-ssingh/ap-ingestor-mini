import logging

from pyspark.sql import functions as F

def apply_custom_rules(df):

    old_col = "raw_data.temporal_data"
    new_col = "raw_data.temporal_data_str"

    logging.info(f"casting column: {old_col} -> string with new col name: {new_col}")

    # Convert 'temporal_data' to a string (JSON) and add it as a new field 'temporal_data_str' in raw_data
    df = df.withColumn(
        "raw_data",  # Modify the raw_data struct
        F.struct(
            F.col("raw_data.hassh"),  # Keep existing fields
            F.col("raw_data.ja3"),  # Keep existing fields
            F.col("raw_data.scan"),  # Keep existing fields
            # Conditionally apply to_json if temporal_data is a struct/array
            F.when(
                F.col("raw_data.temporal_data").cast("string").isNotNull(),
                F.col("raw_data.temporal_data")  # Keep as string if it's already a string
            ).otherwise(F.to_json(F.col("raw_data.temporal_data"))).alias("temporal_data_str"),
            F.col("raw_data.web")  # Keep existing fields
        )
    )

    # Optionally, drop the original 'raw_data.temporal_data' field if no longer needed
    df = df.withColumn(
        "raw_data",
        F.struct(
            F.col("raw_data.hassh"),
            F.col("raw_data.ja3"),
            F.col("raw_data.scan"),
            F.col("raw_data.web"),
            F.col("raw_data.temporal_data_str")
    ))


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