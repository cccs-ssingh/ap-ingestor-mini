import logging
import time

from pyspark.sql import SparkSession
from utilities.iceberg import *


def format_size(bytes_size):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.).
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024

# Function to create Spark session with Iceberg and XML support
def create_spark_session(spark_cfg):
    logging.info("Creating Spark session")

    # Spark session configuration
    spark_builder = SparkSession.builder \
        .appName("Iceberg Ingestion with Azure Storage") \
        .config(             "spark.executor.cores", spark_cfg['driver']["spark.executor.cores"]) \
        .config(            "spark.executor.memory", spark_cfg['driver']["spark.executor.memory"]) \
        .config(         "spark.executor.instances", spark_cfg['driver']["spark.executor.instances"]) \
        .config("spark.sql.files.maxPartitionBytes", spark_cfg['driver']["spark.sql.files.maxPartitionBytes"]) \
        .config(              "spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \

    if spark_cfg['k8s']['name_space']:
        logging.info("- configuring spark for Kubernetes mode.")
        spark_builder = spark_builder \
            .config("spark.master", "k8s://https://kubernetes.default.svc") \
            .config("spark.kubernetes.container.image", spark_cfg['k8s']['spark_image']) \
            .config("spark.kubernetes.namespace", spark_cfg['k8s']['name_space']) \
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")

    spark = spark_builder.getOrCreate()
    logging.info('- spark session created')

    # # Print spark config
    # for key, value in spark.sparkContext.getConf().getAll():
    #     logging.warning(f"{key}: {value}")

    return spark

# Function to read data based on the file type
def read_data(spark, file_cfg, input_files):
    logging.info(f"- reading data type: {file_cfg['type']}")

    if file_cfg['type'] == "csv":
        df = spark.read.option("header", "true").csv(input_files)
    elif file_cfg['type'] == "parquet":
        df = spark.read.parquet(input_files)
    elif file_cfg['type'] == "json":
        if file_cfg['json_multiline']:
            logging.info(f"json_multiline: {file_cfg['json_multiline']}")
            df = spark.read.option("multiLine", "true").json(input_files)
        else:
            df = spark.read.json(input_files)
    elif file_cfg['type'] == "avro":
        df = spark.read.format("avro").load(input_files)
    elif file_cfg['type'] == "xml":
        # databricks library
        if not file_cfg["xml_row_tag"]:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")
        df = (
            spark.read.format("xml")
            .option("rowTag", file_cfg["xml_row_tag"])
            .load(input_files)
        )
    else:
        raise ValueError(f"Unsupported file type: {file_cfg['type']}")

    logging.info(f" - done")
    return df

# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(cfg_iceberg, cfg_file, spark, files_to_process):
    iceberg_table  = f"{cfg_iceberg['catalog']}.{cfg_iceberg['namespace']}.{cfg_iceberg['table']['name']}"

    # Get the snapshot before the write
    pre_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # Write the dataframe
    logging.info(f"Ingesting data into:")
    logging.info(f"- Azure url: {cfg_iceberg['table']['location']}")
    logging.info(f"- Iceberg Table: {iceberg_table}")

    # Read the data based on the file type
    df = read_data(spark, cfg_file, files_to_process)

    # Start timing
    start_time = time.time()

    # Write the DataFrame to the Iceberg table
    df.writeTo(iceberg_table ) \
        .option("merge-schema", "true") \
        .tableProperty("location", cfg_iceberg['table']['location']) \
        .createOrReplace()

    # Calculate time taken
    time_taken = time.time() - start_time

    # Get the snapshot after the write
    post_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # Get the new files written during the current operation
    new_files, total_size = get_new_files(spark, iceberg_table, pre_write_snapshot, post_write_snapshot)

    # Get the number of records written
    record_count = df.count()

    # Log metrics
    logging.info('Success!')
    logging.info(f'- {record_count} records')
    logging.info(f'- {len(new_files)} file(s)')
    logging.info(f'- {format_size(total_size)}')
    logging.info(f'- {time_taken:.2f} seconds')