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
def create_spark_session(spark_cfg, app_name):
    logging.info(f"")
    logging.info("Creating Spark session")

    # Spark session configuration
    spark_builder = SparkSession.builder \
        .appName(f"APA4b Ingestor-Mini: {app_name}") \
        .master("spark://ver-1-spark-master-0.ver-1-spark-headless.spark.svc.cluster.local:7077") \
        .config(             "spark.executor.cores", spark_cfg['driver']["spark.executor.cores"]) \
        .config(            "spark.executor.memory", spark_cfg['driver']["spark.executor.memory"]) \
        .config(         "spark.executor.instances", spark_cfg['driver']["spark.executor.instances"]) \
        .config(              "spark.driver.memory", spark_cfg['driver']["spark.driver.memory"]) \
        .config("spark.sql.files.maxPartitionBytes", spark_cfg['driver']["spark.sql.files.maxPartitionBytes"]) \
        .config(              "spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") \
        .config("spark.cores.max", spark_cfg['driver']["spark.executor.cores"] * spark_cfg['driver']["spark.executor.instances"]) \
    # .config("spark.sql.adaptive.enabled", "true") \
        # .config("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY") \
        # .config(       "spark.sql.avro.parseMode", "PERMISSIVE") \
        # .config("spark.default.parallelism", 96) \
        # .config("spark.executor.heartbeatInterval", "60s") \
        # .config("spark.dynamicAllocation.enabled", "true") \
        # .config("spark.sql.shuffle.partitions", "512") \
        # .config("spark.sql.adaptive.enabled", "true") \

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
    # # Print spark config
    # for key, value in spark.sparkContext.getConf().getAll():
    #     logging.warning(f"{key}: {value}")

    # Access the Spark configuration
    conf = spark.sparkContext.getConf()

    # Extract and log relevant configuration settings
    logging.info(f"")
    logging.info("==== Spark Session Configuration ====")
    logging.info(f"          App Name: {conf.get('spark.app.name')}")
    logging.info(f"            Master: {conf.get('spark.master')}")
    logging.info(f"     Driver Memory: {conf.get('spark.driver.memory', 'Not Set')}")
    logging.info(f"   Executor Memory: {conf.get('spark.executor.memory', 'Not Set')}")
    logging.info(f"    Executor Cores: {conf.get('spark.executor.cores', 'Not Set')}")
    logging.info(f"Executor Instances: {conf.get('spark.executor.instances', 'Not Set')}")
    logging.info(f"Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions', 'Not Set')}")
    logging.info("=====================================")

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

    # # Get the snapshot before the write
    # pre_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # # Write the dataframe
    logging.info(f"Ingesting data to: {cfg_iceberg['table']['location']}")

    # Start timing
    start_time = time.time()

    # Read the data based on the file type
    logging.info(f"- {len(files_to_process)} files to process")
    df = read_data(spark, cfg_file, files_to_process)

    # Populate timeperiod column for partitioning
    df = df.withColumn(
        cfg_iceberg['partition']['field'],
        to_date(lit(cfg_iceberg['partition']['value']), cfg_iceberg['partition']['format'])
    )
    logging.info(f"- populated column: {cfg_iceberg['partition']['field']} with value: {cfg_iceberg['partition']['value']}")

    logging.info(f"")
    logging.info(f"Schema of new data:")
    df.printSchema()

    # Check if the table exists
    if not spark.catalog.tableExists(iceberg_table):
        # Create the table if it doesn't exist
        logging.info(f"- writing to new Iceberg Table: {iceberg_table}")
        df.writeTo(iceberg_table) \
            .option("merge-schema", "true") \
            .tableProperty("location", cfg_iceberg['table']['location']) \
            .partitionedBy(cfg_iceberg['partition']['field']) \
            .create()
    else:
        # Append to the table if it exists
        logging.info(f"- appending to existing Iceberg Table: {iceberg_table}")
        df.writeTo(iceberg_table) \
            .option("merge-schema", "true") \
            .tableProperty("location", cfg_iceberg['table']['location']) \
            .partitionedBy(cfg_iceberg['partition']['field']) \
            .append()
    #
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