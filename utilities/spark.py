import time
import os
import importlib

from utilities.iceberg import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Function to create Spark session with Iceberg
def create_spark_session(app_name):
    logging.debug(f"")
    logging.debug("Creating Spark session")

    spark_builder = SparkSession.builder \
        .appName(f"APA4b Ingestor-Mini: {app_name}") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.debug.maxToStringFields", "100")

    spark = spark_builder.getOrCreate()
    log_spark_config(spark)

    return spark

def log_spark_config(spark):
    # Extract and log relevant configuration settings

    # all_configs = spark.sparkContext.getConf().getAll()
    # for key, value in sorted(all_configs):
    #     print(f"{key}: {value}")

    # Access the Spark configuration
    conf = spark.sparkContext.getConf()
    logging.info("=====  Spark Session Configuration ====")
    logging.info(f"          App Name: {conf.get('spark.app.name')}")
    logging.info(f"            Master: {conf.get('spark.master')}")
    logging.info(f"     Driver Memory: {conf.get('spark.driver.memory', 'Not Set')}")
    logging.info(f"   Executor Memory: {conf.get('spark.executor.memory', 'Not Set')}")
    logging.info(f"    Executor Cores: {conf.get('spark.executor.cores', 'Not Set')}")
    logging.info(f"Executor Instances: {conf.get('spark.executor.instances', 'Not Set')}")
    logging.info(f"         Cores MAX: {conf.get('spark.cores.max', 'Not Set')}")
    logging.info(f"   Shuffle Service: {conf.get('spark.shuffle.service.enabled', 'Not Set')}")
    logging.info(f"Dynamic Allocation: {conf.get('spark.dynamicAllocation.enabled', 'Not Set')}")
    logging.info(f"   Memory Fraction: {conf.get('spark.memory.fraction', 'Not Set')}")
    logging.info(f"Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions', 'Not Set')}")
    logging.info("=======================================")

# Read data based on the file type
def read_data(spark, file_cfg, input_files):
    logging.info(f"Reading input data")

    if file_cfg['type'] == "csv":
        df = spark.read.option("header", "true").csv(input_files)

    elif file_cfg['type'] == "parquet":
        df = spark.read.parquet(input_files)

    elif file_cfg['type'] == "avro":
        df = spark.read.format("avro").load(input_files)

    elif file_cfg['type'] == "json":
        if file_cfg['json_multiline']:
            logging.info(f"- json_multiline: {file_cfg['json_multiline']}")
            df = spark.read.option("multiLine", "true").json(input_files)
        else:
            df = spark.read.json(input_files)

    elif file_cfg['type'] == "xml":  # using databricks library
        if not file_cfg["xml_row_tag"]:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")

        logging.info(f"- xml_row_tag: '{file_cfg['xml_row_tag']}'")
        first_file = input_files[0]
        input_dir = first_file.rsplit('/', 1)[0]

        df = (
            spark.read.format("xml")
            .option("rowTag", file_cfg["xml_row_tag"])
            .load(f"{input_dir}/*.xml")
        )

    else:
        raise ValueError(f"Unsupported file type: {file_cfg['type']}")

    logging.info(f"- successfully read data!")
    return df

# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(cfg_iceberg, cfg_file, spark, files_to_process):
    logging.info("")

    # Start timing
    start_time = time.time()

    # Read the data based on the file type
    logging.debug(f"- {len(files_to_process)} files to process")
    df = read_data(spark, cfg_file, files_to_process)

    # Populate partition column
    df = populate_timeperiod_partition_column(
        df,
        cfg_iceberg['partition']['field'],
        cfg_iceberg['partition']['value'],
        cfg_iceberg['partition']['format']
    )

    # Manual adjustments
    df = apply_custom_ingestor_rules(df, cfg_iceberg['table']['name'])

    # Write the dataframe
    iceberg_table = f"{cfg_iceberg['catalog']}.{cfg_iceberg['namespace']}.{cfg_iceberg['table']['name']}"

    if not spark.catalog.tableExists(iceberg_table):
        # New Iceberg table
        create_new_iceberg_table(
            df, iceberg_table,
            cfg_iceberg['table']['location'],
            cfg_iceberg['partition']['field']
        )

    else: # Existing Iceberg Table
        merge_into_existing_table(
            spark, df, iceberg_table,
            cfg_iceberg['partition']['field'],
            cfg_iceberg['table']['location']
        )

    # Logs
    elapsed_time = seconds_to_hh_mm_ss(time.time() - start_time)
    logging.info('')
    logging.info('Metrics:')
    logging.info(f"-      records: {df.count():,}")
    logging.info(f"- processed in: {elapsed_time}s")

    # End Spark Session
    spark.stop()
    logging.info(f"====================================")


def apply_custom_ingestor_rules(df, module_name):
    # Construct the full file path and check if it exists
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    custom_ingestors_dir = os.path.join(project_root, "custom_ingestors")
    custom_ingestor_path = os.path.join(custom_ingestors_dir, f"{module_name}.py")

    if os.path.exists(custom_ingestor_path):
        logging.info(f"")
        logging.info(f"Custom ingestor exits: '{custom_ingestor_path}'")

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
    logging.info(f"")
    logging.info(f"No existing table found!")
    logging.info(f"- creating a new Iceberg Table.")
    df.writeTo(iceberg_table) \
        .tableProperty("location", table_location) \
        .tableProperty("write.spark.accept-any-schema", "true") \
        .partitionedBy(partition_field) \
        .create()
    logging.info(f"- created: {iceberg_table}")

def log_new_columns(table_fields, dataframe_fields):
    logging.info("")
    logging.info("Checking for new columns in the dataframe")
    new_columns_in_dataframe = {name: datatype for name, datatype in dataframe_fields.items() if name not in table_fields}
    if new_columns_in_dataframe:
        for field, data_type in new_columns_in_dataframe.items():
            logging.info(f" - {field}: {data_type}")
    else:
        logging.info("- no new columns")

def log_changed_columns_and_sub_fields(table_fields, dataframe_fields, prefix=""):
    changes_detected = False

    for field, data_type in dataframe_fields.items():
        full_field_name = f"{prefix}.{field}" if prefix else field

        if field in table_fields:
            table_data_type = table_fields[field]

            if isinstance(data_type, StructType) and isinstance(table_data_type, StructType):
                # Recursively check nested fields
                nested_changes = log_changed_columns_and_sub_fields(
                    {f.name: f.dataType for f in table_data_type.fields},
                    {f.name: f.dataType for f in data_type.fields},
                    full_field_name
                )
                if nested_changes:
                    changes_detected = True
            elif isinstance(data_type, ArrayType) and isinstance(table_data_type, ArrayType):
                # Check element types for arrays
                if data_type.elementType != table_data_type.elementType:
                    logging.info(f"- Column:            {full_field_name}")
                    logging.info(f"  -     Table type = {table_data_type}")
                    logging.info(f"  - DataFrame type = {data_type}")
                    changes_detected = True
            else:
                if table_data_type != data_type:
                    logging.info(f"- Column:            {full_field_name}")
                    logging.info(f"  -     Table type = {table_data_type}")
                    logging.info(f"  - DataFrame type = {data_type}")
                    changes_detected = True

    if not changes_detected:
        logging.info("- all column datatypes match")

    return changes_detected

def log_and_apply_schema_changes(df: DataFrame, table_schema: StructType) -> DataFrame:
    def adjust_schema(df: DataFrame, table_fields: dict, dataframe_fields: dict, prefix="") -> DataFrame:
        changes_detected = False

        for field, data_type in dataframe_fields.items():
            full_field_name = f"{prefix}.{field}" if prefix else field

            if field in table_fields:
                table_data_type = table_fields[field]

                if isinstance(data_type, StructType) and isinstance(table_data_type, StructType):
                    # Recursively check nested fields
                    nested_df = df.withColumn(field, col(field))
                    nested_df = adjust_schema(
                        nested_df,
                        {f.name: f.dataType for f in table_data_type.fields},
                        {f.name: f.dataType for f in data_type.fields},
                        full_field_name
                    )
                    df = df.withColumn(field, nested_df[field])
                elif isinstance(data_type, ArrayType) and isinstance(table_data_type, ArrayType):
                    # Check element types for arrays
                    if data_type.elementType != table_data_type.elementType:
                        logging.info(f"- Column:            {full_field_name}")
                        logging.info(f"  -     Table type = {table_data_type}")
                        logging.info(f"  - DataFrame type = {data_type}")
                        df = df.withColumn(full_field_name, to_json(col(full_field_name)))
                        changes_detected = True
                elif isinstance(data_type, MapType) and isinstance(table_data_type, MapType):
                    # Check key and value types for maps
                    if data_type.keyType != table_data_type.keyType or data_type.valueType != table_data_type.valueType:
                        logging.info(f"- Column:            {full_field_name}")
                        logging.info(f"  -     Table type = {table_data_type}")
                        logging.info(f"  - DataFrame type = {data_type}")
                        df = df.withColumn(full_field_name, col(full_field_name).cast(table_data_type))
                        changes_detected = True
                else:
                    if table_data_type != data_type:
                        logging.info(f"- Column:            {full_field_name}")
                        logging.info(f"  -     Table type = {table_data_type}")
                        logging.info(f"  - DataFrame type = {data_type}")
                        df = df.withColumn(full_field_name, col(full_field_name).cast(table_data_type))
                        changes_detected = True

        if not changes_detected:
            logging.info("- all column datatypes match")
        return df

    # Convert schemas to dictionaries for easier processing
    table_fields = {f.name: f.dataType for f in table_schema.fields}
    dataframe_fields = {f.name: f.dataType for f in df.schema.fields}

    # Adjust the DataFrame schema to match the table schema
    df = adjust_schema(df, table_fields, dataframe_fields)
    return df


def merge_into_existing_table(spark, df, iceberg_table, partition_field, table_location):
    # Schemas
    table_schema = spark.table(iceberg_table).schema
    table_fields =     {field.name: field.dataType for field in table_schema.fields}
    dataframe_fields = {field.name: field.dataType for field in df.schema.fields}

    # Log new columns - no action needed as merge-schema option handles this
    log_new_columns(table_fields, dataframe_fields)

    # Identify columns with changed formats
    logging.info("")
    logging.info("Checking for data type discrepancies")
    log_changed_columns_and_sub_fields(table_fields, dataframe_fields)
    df = log_and_apply_schema_changes(df, table_schema)

    # Append to existing table
    logging.info('')
    logging.info(f"Appending to: '{iceberg_table}'")
    logging.info(f"- schema evolution enabled (mergeSchema)")

    df.writeTo(iceberg_table) \
        .tableProperty("location", table_location) \
        .option("mergeSchema", "true") \
        .option("check-ordering", "false") \
        .partitionedBy(partition_field) \
        .append()

    logging.info('- appended!')
