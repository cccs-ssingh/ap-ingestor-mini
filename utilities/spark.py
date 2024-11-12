import time
import os
import json
import sys
import importlib

from pyspark.sql import SparkSession
from utilities.iceberg import *
from pyspark.sql.functions import lit, to_date


# Function to create Spark session with Iceberg
def create_spark_session(spark_cfg, app_name):
    logging.debug(f"")
    logging.debug("Creating Spark session")

    spark_builder = SparkSession.builder \
        .appName(f"APA4b Ingestor-Mini: {app_name}") \
        .master("spark://ver-1-spark-master-0.ver-1-spark-headless.spark.svc.cluster.local:7077") \
        .config("spark.ui.showConsoleProgress", "false") \
        # .config("spark.cores.max", int(spark_cfg['spark.executor.cores']) * int(spark_cfg['spark.executor.instances']))

    # for key, value in spark_cfg.items():
    #     spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    log_spark_config(spark)

    # if spark_cfg['k8s']['name_space']:
    #     logging.info("- configuring spark for Kubernetes mode.")
    #     spark_builder = spark_builder \
    #         .config("spark.master", "k8s://https://kubernetes.default.svc") \
    #         .config("spark.kubernetes.container.image", spark_cfg['k8s']['spark_image']) \
    #         .config("spark.kubernetes.namespace", spark_cfg['k8s']['name_space']) \
    #         .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")

    return spark

def log_spark_config(spark):
    # Extract and log relevant configuration settings

    # all_configs = spark.sparkContext.getConf().getAll()
    # for key, value in sorted(all_configs):
    #     print(f"{key}: {value}")

    # Access the Spark configuration
    conf = spark.sparkContext.getConf()
    logging.info("==== Spark Session Configuration ====")
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
    logging.info("=====================================")

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

    elif file_cfg['type'] == "xml":
        # databricks library
        if not file_cfg["xml_row_tag"]:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")

        logging.info(f"- xml_row_tag: {file_cfg['xml_row_tag']}")
        df = (
            spark.read.format("xml")
            .option("rowTag", file_cfg["xml_row_tag"])
            .load(input_files)
        )

    else:
        raise ValueError(f"Unsupported file type: {file_cfg['type']}")

    logging.info(f" - successfully read data!")
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
    else:
        # Old Iceberg Table
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
    logging.info(f"Populating partition column -> value")
    logging.info(f"- {partition_field} -> {partition_value}")
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

def log_changed_columns(table_fields, dataframe_fields):
    logging.info("")
    logging.info("Checking for changed column data types")
    changes_detected = False

    for field, data_type in dataframe_fields.items():
        if field in table_fields and table_fields[field] != data_type:
            logging.info(f"- Column:{field} data-type discrepancy:")
            logging.info(f"  -     Table type = {table_fields[field]}")
            logging.info(f"  - DataFrame type = {data_type}")
            changes_detected = True

    if not changes_detected:
        logging.info("- all column datatypes match")
    return changes_detected

def merge_into_existing_table(spark, df, iceberg_table, partition_field, table_location):
    # Schemas
    table_schema = spark.table(iceberg_table).schema
    table_fields =     {field.name: field.dataType for field in table_schema.fields}
    dataframe_fields = {field.name: field.dataType for field in df.schema.fields}

    # Log new columns - no action needed as merge-schema option handles this
    log_new_columns(table_fields, dataframe_fields)

    # Identify columns with changed formats
    log_changed_columns(table_fields, dataframe_fields)

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
