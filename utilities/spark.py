import logging
import time
import os
import json

from pyspark.sql import SparkSession
from utilities.iceberg import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, to_date, col, from_json, expr
from pyspark.sql.types import StringType, ArrayType, StructType



def format_size(bytes_size):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.).
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024

# Function to create Spark session with Iceberg
def create_spark_session(spark_cfg, app_name):
    logging.debug(f"")
    logging.debug("Creating Spark session")

    # Spark session configuration
    if spark_cfg.get('config'):
        # Dynamic spark config
        cfg = json.loads(spark_cfg.get('config'))

        spark_builder = SparkSession.builder \
            .appName(f"APA4b Ingestor-Mini: {app_name}") \
            .master("spark://ver-1-spark-master-0.ver-1-spark-headless.spark.svc.cluster.local:7077") \
            .config("spark.cores.max", int(cfg['spark.executor.cores']) * int(cfg['spark.executor.instances']))

        for key, value in cfg.items():
            spark_builder.config(key, value)

    else:
        # cmd-line specified config
        spark_builder = SparkSession.builder \
            .appName(f"APA4b Ingestor-Mini: {app_name}") \
            .master("spark://ver-1-spark-master-0.ver-1-spark-headless.spark.svc.cluster.local:7077") \
            .config(             "spark.executor.cores", spark_cfg['executor']["cores"]) \
            .config(            "spark.executor.memory", spark_cfg['executor']["memory"]) \
            .config(         "spark.executor.instances", spark_cfg['executor']["instances"]) \
            .config(              "spark.driver.memory", spark_cfg['driver']["memory"]) \
            .config("spark.sql.files.maxPartitionBytes", spark_cfg['sql']["maxPartitionBytes"]) \
            .config(              "spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
            .config("spark.cores.max", spark_cfg['executor']["cores"] * spark_cfg['executor']["instances"]) \

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
    logging.info(f"- reading data type: {file_cfg['type']}")

    if file_cfg['type'] == "csv":
        df = spark.read.option("header", "true").csv(input_files)

    elif file_cfg['type'] == "parquet":
        df = spark.read.parquet(input_files)

    elif file_cfg['type'] == "avro":
        df = spark.read.format("avro").load(input_files)

    elif file_cfg['type'] == "json":
        if file_cfg['json_multiline']:
            logging.info(f"   - json_multiline: {file_cfg['json_multiline']}")
            df = spark.read.option("multiLine", "true").json(input_files)
        else:
            df = spark.read.json(input_files)

    elif file_cfg['type'] == "xml":
        # databricks library
        if not file_cfg["xml_row_tag"]:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")

        logging.info(f" - xml_row_tag: {file_cfg['xml_row_tag']}")
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
    iceberg_table = f"{cfg_iceberg['catalog']}.{cfg_iceberg['namespace']}.{cfg_iceberg['table']['name']}"

    # Get the snapshot before the write
    # pre_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # Write the dataframe
    logging.info(f"Reading data from: {cfg_iceberg['table']['location']}")

    # Start timing
    start_time = time.time()

    # Read the data based on the file type
    logging.debug(f"- {len(files_to_process)} files to process")
    df = read_data(spark, cfg_file, files_to_process)

    # Populate timeperiod column for partitioning
    logging.info(f"")
    logging.info(f"Populating column: {cfg_iceberg['partition']['field']} with value: {cfg_iceberg['partition']['value']}")
    df = df.withColumn(
        cfg_iceberg['partition']['field'],
        to_date(lit(cfg_iceberg['partition']['value']), cfg_iceberg['partition']['format'])
    )
    logging.info(f"- populated!")

    # Manual adjustments
    # NVD
    if 'nvd' in iceberg_table:
        logging.info("manual casting of columns")
        # df = df.withColumn("cveTags", col("cveTags").cast("string"))
        df = df.withColumn("cveTags", from_json(col("cveTags").cast("string"), ArrayType(StringType(), True)))

    # New table
    logging.info(f"")
    if not spark.catalog.tableExists(iceberg_table):
        logging.info(f"No table found! Creating a new Iceberg Table.")
        df.writeTo(iceberg_table) \
            .option("merge-schema", "true") \
            .tableProperty("location", cfg_iceberg['table']['location']) \
            .partitionedBy(cfg_iceberg['partition']['field']) \
            .create()

    # Existing Table
    else:
        logging.info("Comparing existing table schema to dataframe")

        # Get Schemas
        table_schema = spark.table(iceberg_table).schema
        table_fields = {field.name: field.dataType for field in table_schema.fields}
        dataframe_schema = df.schema
        dataframe_fields = {field.name: field.dataType for field in dataframe_schema.fields}

        # Log new columns - no action needed as merge-schema option handles this
        log_new_columns(table_fields, dataframe_fields)

        # Add columns that exist in the Table but are missing in the Dataframe
        df = add_missing_columns_to_df(table_fields, dataframe_fields, df)

        # Log any columnds with changed data types

        # Identify columns with changed formats
        if log_changed_columns(table_fields, dataframe_fields):
            align_schema(df, table_schema)

        # Append to existing table
        df.writeTo(iceberg_table) \
            .option("merge-schema", "true") \
            .tableProperty("location", cfg_iceberg['table']['location']) \
            .partitionedBy(cfg_iceberg['partition']['field']) \
            .append()

    # # Calculate time taken
    # time_taken = time.time() - start_time
    #
    # # Get the snapshot after the write
    # post_write_snapshot = get_latest_snapshot(spark, iceberg_table)
    #
    # # Get the new files written during the current operation
    # new_files, total_size = get_new_files(spark, iceberg_table, pre_write_snapshot, post_write_snapshot)
    #
    # # Get the number of records written
    # record_count = df.count()

    # Log metrics
    logging.info('')
    logging.info('Success!')
    # logging.info('Success! Metrics:')
    # logging.info(f'- {len(new_files)} file(s) -> {record_count} records: {format_size(total_size)} in {time_taken:.2f} seconds')

def log_new_columns(table_fields, dataframe_fields):
    logging.debug("Checking for new columns in the dataframe")
    new_columns_in_dataframe = {name: datatype for name, datatype in dataframe_fields.items() if name not in table_fields}
    if new_columns_in_dataframe:
        for field, data_type in new_columns_in_dataframe.items():
            logging.info(f" - {field}: {data_type}")
    logging.debug("- done")

def add_missing_columns_to_df(table_fields, dataframe_fields, df):
    logging.debug("Checking for missing columns in the dataframe")

    missing_columns = set(table_fields) - set(dataframe_fields)
    for column in missing_columns:
        column_type = table_fields[column]
        logging.info(f"- {column} {column_type} -> missing in dataframe, added")
        df = df.withColumn(column, lit(None).cast(column_type))  # Updates to a new DataFrame

    logging.debug("- done")
    return df

def log_changed_columns(table_fields, dataframe_fields):
    logging.debug("Checking for changed column data types")
    changed_fields = {}

    for field, data_type in dataframe_fields.items():
        if field in table_fields and table_fields[field] != data_type:
            changed_fields[field] = (table_fields[field], data_type)

    if changed_fields:
        logging.info("- field change(s) detected:")
        for field, (data_type_table, data_type_dataframe) in changed_fields.items():
            logging.info(f" - {field}:")
            logging.info(f"   -     Table type = {data_type_table}")
            logging.info(f"   - DataFrame type = {data_type_dataframe}")
        return True

    else:
        logging.debug("- done")


def align_schema(df, table_schema):
    """
    Recursively aligns the schema of the DataFrame to match the table schema.
    """
    for field in table_schema.fields:
        if field.name not in df.schema.fieldNames():
            # If the field is missing, add it as null with the correct data type
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
        else:
            # If the field exists but is nested, check recursively
            df_field_types = {f.name: f.dataType for f in df.schema.fields}
            df_field_type = df_field_types.get(field.name)

            # Handle nested StructType recursively
            if isinstance(field.dataType, StructType) and isinstance(df_field_type, StructType):
                nested_df = df.selectExpr(f"`{field.name}`.*")  # Extract nested columns for comparison
                aligned_nested_df = align_schema(nested_df, field.dataType)
                # Reassemble the nested structure with aligned schema
                df = df.drop(field.name).withColumn(field.name, aligned_nested_df)

            # Handle arrays of structs by aligning the struct schema within the array
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                element_type = field.dataType.elementType
                # Create an empty DataFrame to align element schema
                aligned_element_schema = align_schema(spark.createDataFrame([], element_type), element_type).schema
                df = df.withColumn(
                    field.name,
                    col(field.name).cast(ArrayType(aligned_element_schema))
                )

    return df
