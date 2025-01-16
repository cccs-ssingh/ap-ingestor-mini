import logging
from pyspark.sql import functions as F

def apply_custom_rules(df):

    logging.info("- replacing field raw_data.temporal_data -> raw_data.temporal_data_str")

    # Get the schema of the 'raw_data' struct
    raw_data_fields = [field.name for field in df.schema["raw_data"].dataType.fields]

    # Dynamically create a list of expressions for all fields in 'raw_data'
    # Replace 'temporal_data' with 'temporal_data_str' in the field list
    updated_fields = [
        F.col(f"raw_data.{field}").alias("temporal_data_str") if field == "temporal_data" else F.col(
            f"raw_data.{field}")
        for field in raw_data_fields
    ]

    # Create the new 'raw_data' struct with the updated fields
    df = df.withColumn(
        "raw_data",                # Modify the raw_data struct
        F.struct(*updated_fields)  # Pass the list of updated fields
    )

    return df