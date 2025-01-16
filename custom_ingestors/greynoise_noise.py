import logging

from pyspark.sql import functions as F

def apply_custom_rules(df):
    from pyspark.sql import functions as F

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
    df_new = df.withColumn(
        "raw_data",  # Modify the raw_data struct
        F.struct(*updated_fields)  # Pass the list of updated fields
    )

    # # Check the updated schema to verify the change
    # df_new.printSchema()

    return df