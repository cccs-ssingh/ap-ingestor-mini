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
def read_data(spark, input_files, file_type, xml_row_tag=None):
    if file_type == "csv":
        df = spark.read.option("header", "true").csv(input_files)
    elif file_type == "parquet":
        df = spark.read.parquet(input_files)
    elif file_type == "json":
        # df = spark.read.json(input_files)
        df = spark.read.option("multiLine", "true").json(input_files)
    elif file_type == "avro":
        df = spark.read.format("avro").load(input_files)
    elif file_type == "xml":
        # databricks library
        if not xml_row_tag:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")
        df = (
            spark.read.format("xml")
            .option("rowTag", xml_row_tag)
            .load(input_files)
        )
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    return df

# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(ice_cfg, spark, files_to_process, file_type, xml_row_tag=None):
    iceberg_table  = f"{ice_cfg['catalog']}.{ice_cfg['namespace']}.{ice_cfg['table']['name']}"

    # # Get the snapshot before the write
    # pre_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # Write the dataframe
    logging.info(f"Ingesting data:")
    logging.info(f"- Azure url: {ice_cfg['table']['location']}")
    logging.info(f"- Iceberg Table: {iceberg_table }")

    # Read the data based on the file type
    df = read_data(spark, files_to_process, file_type, xml_row_tag)

    # Start timing
    start_time = time.time()

    # Write the DataFrame to the Iceberg table
    df.writeTo(iceberg_table ) \
        .option("merge-schema", "true") \
        .tableProperty("location", ice_cfg['table']['location']) \
        .createOrReplace()

    # Calculate time taken
    time_taken = time.time() - start_time

    # # Get the snapshot after the write
    # post_write_snapshot = get_latest_snapshot(spark, iceberg_table)
    #
    # # Get the new files written during the current operation
    # new_files, total_size = get_new_files(spark, iceberg_table, pre_write_snapshot, post_write_snapshot)
    #
    # # Get the number of records written
    # record_count = df.count()
    #
    # # Log metrics
    # logging.info('Success!')
    # logging.info(f'- {record_count} records')
    # logging.info(f'- {len(new_files)} file(s)')
    # logging.info(f'- {format_size(total_size)}')
    # logging.info(f'- {time_taken:.2f} seconds')