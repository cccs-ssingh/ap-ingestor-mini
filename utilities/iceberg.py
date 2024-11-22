import logging
import time
import os
import importlib

from spark import read_data
from pyspark.sql.functions import *

def format_size(bytes_size):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.).
    """
    if bytes_size is None or bytes_size == 0:
        return "0 B"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024

def seconds_to_hh_mm_ss(seconds):
    # Calculate the time components
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    # Format the time as HH:MM:SS
    return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

# Retrieve the latest snapshot id for an Iceberg table
def get_latest_snapshot_id(spark, iceberg_table):
    try:
        # Check if the Iceberg table exists by loading the snapshot metadata
        snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")

        # Assuming 'snapshot_id' exists, retrieve the latest snapshot based on 'committed_at'
        latest_snapshot = snapshots_df.orderBy(snapshots_df["committed_at"].desc()).first()

        # Return the 'snapshot_id' column
        return latest_snapshot["snapshot_id"] if latest_snapshot else None

    except Exception as e:
        # If the table doesn't exist, return None or handle it appropriately
        print(f"Table '{iceberg_table}' does not exist or cannot be read: {e}")
        return None


# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(cfg_iceberg, cfg_file, spark, files_to_process):
    logging.info("")

    # Start timing
    start_time = time.time()

    # Read the data based on the file type
    logging.debug(f"- {len(files_to_process)} files to process")
    df = read_data(spark, cfg_file, files_to_process)

    # Manual adjustments
    df = apply_custom_ingestor_rules(df, cfg_iceberg['table']['name'])

    # Populate partition column
    df = populate_timeperiod_partition_column(
        df,
        cfg_iceberg['partition']['field'],
        cfg_iceberg['partition']['value'],
        cfg_iceberg['partition']['format']
    )

    # Write the dataframe
    iceberg_table = f"{cfg_iceberg['catalog']}.{cfg_iceberg['namespace']}.{cfg_iceberg['table']['name']}"
    logging.info(f"")
    logging.info(f"Checking if iceberge table exists: '{iceberg_table}'")

    # New Iceberg table
    if not spark.catalog.tableExists(iceberg_table):
        create_new_iceberg_table(
            df, iceberg_table,
            cfg_iceberg['table']['location'],
            cfg_iceberg['partition']['field']
        )
    # Existing Iceberg Table
    else:
        logging.info(f"- table found!")

        if cfg_iceberg['write_mode'] == 'overwrite':
            overwrite_existing_table(
                df, iceberg_table,
                cfg_iceberg['partition']['field'],
                cfg_iceberg['partition']['value'],
                cfg_iceberg['table']['location'],
            )
        else:
            # log_new_columns(spark, df, iceberg_table)
            merge_into_existing_table(
                df, iceberg_table,
                cfg_iceberg['partition']['field'],
                cfg_iceberg['table']['location']
            )

    # End Spark Session
    log_metrics(df, start_time)
    spark.stop()
    logging.info(f"====================================")

def apply_custom_ingestor_rules(df, module_name):
    # Construct the full file path and check if it exists
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    custom_ingestors_dir = os.path.join(project_root, "custom_ingestors")
    custom_ingestor_path = os.path.join(custom_ingestors_dir, f"{module_name}.py")

    if os.path.exists(custom_ingestor_path):
        logging.info(f"")
        logging.info(f"Custom ingestor exits: 'custom_ingestors/{module_name}.py'")

        # Add the custom_ingestors directory to sys.path, not the full file path
        sys.path.insert(0, custom_ingestors_dir)
        try:
            module = importlib.import_module(f"custom_ingestors.{module_name}")
            # Check if the function apply_custom_rules exists in the module
            if hasattr(module, "apply_custom_rules"):
                logging.info(" - applying custom rules to df")
                df = module.apply_custom_rules(df)  # Pass df to the function if needed
                return df
            else:
                logging.error(f"The function 'apply_custom_rules' does not exist in {module_name}.")
        finally:
            # Clean up sys.path by removing the added directory
            sys.path.pop(0)
    else:
        return df

def populate_timeperiod_partition_column(df, partition_field, partition_value, partition_format):
    logging.info(f"")
    logging.info(f"Populating partition 'column' -> value")
    logging.info(f"- '{partition_field}' -> {partition_value}")
    df = df.withColumn(partition_field, to_date(lit(partition_value), partition_format))
    logging.info(f"- populated")
    return df

def create_new_iceberg_table(df, iceberg_table, table_location, partition_field):
    logging.info(f"- no existing table found")
    logging.info(f"- creating a new iceberg table")

    df.writeTo(iceberg_table) \
        .tableProperty("location", table_location) \
        .tableProperty("write.spark.accept-any-schema", "true") \
        .partitionedBy(partition_field) \
        .create()
    logging.info(f"- created: {iceberg_table}")

def log_new_columns(spark, df, iceberg_table):
    logging.info("")
    logging.info("Checking for new columns in the dataframe")

    # Schemas
    table_schema = spark.table(iceberg_table).schema
    table_fields = {field.name: field.dataType for field in table_schema.fields}
    dataframe_fields = {field.name: field.dataType for field in df.schema.fields}

    new_columns_in_dataframe = {name: datatype for name, datatype in dataframe_fields.items() if name not in table_fields}
    if new_columns_in_dataframe:
        for field, data_type in new_columns_in_dataframe.items():
            logging.info(f" - {field}: {data_type}")
    else:
        logging.info("- no new columns")

def log_changed_columns(table_fields, dataframe_fields):
    logging.info("")
    logging.info("Checking for column data type discrepancies")
    changes_detected = False

    for field, data_type in dataframe_fields.items():
        if field in table_fields and table_fields[field] != data_type:
            logging.info(f"- Column:            {field}")
            logging.info(f"  -     Table type = {table_fields[field]}")
            logging.info(f"  - DataFrame type = {data_type}")
            changes_detected = True

    if not changes_detected:
        logging.info("- all column datatypes match")
    return changes_detected

def overwrite_existing_table(df, iceberg_table, partition_field, partition_value, table_location):
    logging.info("- iceberg.write.mode set to 'overwrite'")
    logging.info('- overwriting existing table')

    df.writeTo(iceberg_table) \
        .tableProperty("location", table_location) \
        .partitionedBy(partition_field) \
        .overwritePartitions()

    logging.info('- table overwritten!')

def merge_into_existing_table(df, iceberg_table, partition_field, table_location):
    logging.info('')
    logging.info(f"- appending to the existing table w/ schema evolution enabled (mergeSchema)")

    df.writeTo(iceberg_table) \
        .tableProperty("location", table_location) \
        .option("mergeSchema", "true") \
        .option("check-ordering", "false") \
        .partitionedBy(partition_field) \
        .append()

    logging.info('- appended!')

def log_metrics(df, start_time):
    elapsed_time = seconds_to_hh_mm_ss(time.time() - start_time)
    logging.info('')
    logging.info('Metrics:')
    logging.info(f"-      records: {df.count():,}")
    logging.info(f"- processed in: {elapsed_time}s")