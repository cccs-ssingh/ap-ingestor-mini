import logging
import time, os

from pyspark.sql import SparkSession
from utilities.iceberg import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, to_date


def format_size(bytes_size):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.).
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024

# Function to create Spark session with Iceberg
def create_spark_session(spark_cfg):
    logging.info("Creating Spark session")
    # log4j_prop_fp = f"{os.getcwd()}/ap-ingestor-mini/utilities/log4j.properties"
    # logging.info(f"log4j prop file: {log4j_prop_fp}")

    # Spark session configuration
    spark_builder = SparkSession.builder \
        .appName("Iceberg Ingestion with Azure Storage") \
        .config(             "spark.executor.cores", spark_cfg['driver']["spark.executor.cores"]) \
        .config(            "spark.executor.memory", spark_cfg['driver']["spark.executor.memory"]) \
        .config(         "spark.executor.instances", spark_cfg['driver']["spark.executor.instances"]) \
        .config(              "spark.driver.memory", spark_cfg['driver']["spark.driver.memory"]) \
        .config("spark.sql.files.maxPartitionBytes", spark_cfg['driver']["spark.sql.files.maxPartitionBytes"]) \
        .config(              "spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
        # .config(    "spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_prop_fp}") \
        # .config(  "spark.executor.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_prop_fp}") \

    # if spark_cfg['k8s']['name_space']:
    #     logging.info("- configuring spark for Kubernetes mode.")
    #     spark_builder = spark_builder \
    #         .config("spark.master", "k8s://https://kubernetes.default.svc") \
    #         .config("spark.kubernetes.container.image", spark_cfg['k8s']['spark_image']) \
    #         .config("spark.kubernetes.namespace", spark_cfg['k8s']['name_space']) \
    #         .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")

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

    elif file_cfg['type'] == "avro":
        spark.conf.set("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY")
        spark.conf.set(               "spark.sql.avro.parseMode", "PERMISSIVE")
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

    logging.info(f" - done")
    return df

# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(cfg_iceberg, cfg_file, spark, files_to_process):
    iceberg_table = f"{cfg_iceberg['catalog']}.{cfg_iceberg['namespace']}.{cfg_iceberg['table']['name']}"

    # # Get the snapshot before the write
    # pre_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # # Write the dataframe
    logging.info(f"Ingesting data into:")
    logging.info(f"- Azure url: {cfg_iceberg['table']['location']}")
    logging.info(f"- Iceberg Table: {iceberg_table}")
    logging.info(f"- {len(files_to_process)} files to process")

    # Read the data based on the file type
    df = read_data(spark, cfg_file, files_to_process)

    # Populate timeperiod column for partitioning
    logging.info(f"- partitioning by: {cfg_iceberg['partition']['field']}")
    df = df.withColumn(cfg_iceberg['partition']['field'], to_date(lit(cfg_iceberg['partition']['value']), "yyyy/MM/dd"))

    # # Start timing
    # start_time = time.time()

    # # Write the DataFrame to the Iceberg table
    # logging.info(f"Partitioning by field: {cfg_iceberg['partition']['field']}")
    # if cfg_iceberg['partition']['field'] in df.columns:
    #     logging.info(f"Partition field '{cfg_iceberg['partition']['field']}' exists in the DataFrame.")
    # else:
    #     raise ValueError(f"Partition field '{cfg_iceberg['partition']['field']}' does not exist in the DataFrame.")

    df.writeTo(iceberg_table ) \
        .option("merge-schema", "true") \
        .tableProperty("location", cfg_iceberg['table']['location']) \
        .partitionedBy(cfg_iceberg['partition']['field']) \
        .createOrReplace()

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
    #
    # # Log metrics
    # logging.info('Success!')
    # logging.info(f'- {record_count} records')
    # logging.info(f'- {len(new_files)} file(s)')
    # logging.info(f'- {format_size(total_size)}')
    # logging.info(f'- {time_taken:.2f} seconds')