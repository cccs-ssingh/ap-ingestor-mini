import os
import argparse
import logging
import time
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
az_logger = logging.getLogger("azure")
az_logger.setLevel(logging.WARNING)


def parse_connection_string(conn_str):
    """
    Parse the Azure connection string to extract the AccountName and AccountKey.
    """
    # Split the connection string by semicolons to get the individual key-value pairs
    conn_dict = dict(item.split("=", 1) for item in conn_str.split(";"))

    account_name = conn_dict.get("AccountName")
    account_key = conn_dict.get("AccountKey")

    return account_name, account_key

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
        .config(              "spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") # xml support

    if spark_cfg['k8s']['name_space']:
        logging.info("- configuring spark for Kubernetes mode.")
        spark_builder = spark_builder \
            .config("spark.master", "k8s://https://kubernetes.default.svc") \
            .config("spark.kubernetes.container.image", spark_cfg['k8s']['spark_image']) \
            .config("spark.kubernetes.namespace", spark_cfg['k8s']['name_space']) \
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")

    spark = spark_builder.getOrCreate()
    # # Set log level to ERROR to minimize logging
    # spark.sparkContext.setLogLevel("ERROR")

    logging.info('- spark session created')

    # # Print spark config
    # for key, value in spark.sparkContext.getConf().getAll():
    #     logging.warning(f"{key}: {value}")

    return spark


# Function to list blobs in a directory from Azure Blob Storage using connection string
def list_blobs_in_directory(azure_cfg):
    blob_service_client = BlobServiceClient.from_connection_string(azure_cfg['storage_account']['conn_str'])
    container_client = blob_service_client.get_container_client(azure_cfg['container']['input']['name'])
    logging.info(f"Connected to: {container_client.url}")
    logging.info(f"- retrieving blobs from container '{azure_cfg['container']['input']['name']}' in directory '{azure_cfg['container']['input']['dir']}'")
    blobs = container_client.list_blobs(name_starts_with=azure_cfg['container']['input']['dir'])

    blob_urls = []
    for blob in blobs:
        blob_url = f"abfs://{azure_cfg['container']['input']['name']}@{container_client.account_name}.dfs.core.windows.net/{blob.name}"
        blob_urls.append(blob_url)
    logging.info(f"- {len(blob_urls)} blobs total")

    return blob_urls

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

def format_size(bytes_size):
    """
    Convert bytes to a human-readable format (KB, MB, GB, etc.).
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024

# Retrieve the latest snapshot for an Iceberg table
def get_latest_snapshot(spark, iceberg_table):
    snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")
    latest_snapshot = snapshots_df.orderBy(snapshots_df["committed_at"].desc()).first()
    return latest_snapshot["added_snapshot_id"] if latest_snapshot else None

# Retrieve the new files between two snapshots
def get_new_files(spark, iceberg_table, pre_snapshot, post_snapshot):
    if not pre_snapshot or not post_snapshot:
        return [], 0

    # Query the manifests for the new snapshot using 'added_snapshot_id'
    manifests_df = spark.read.format("iceberg").load(f"{iceberg_table}.manifests")
    new_manifest_files = manifests_df.filter(manifests_df["added_snapshot_id"] == post_snapshot).select("path", "length")

    # Collect the new files
    new_files = new_manifest_files.select("path").collect()
    total_size = new_manifest_files.select("length").rdd.map(lambda row: row[0]).sum()

    return new_files, total_size

# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(ice_cfg, spark, files_to_process, file_type, xml_row_tag=None):
    iceberg_table  = f"{ice_cfg['catalog']}.{ice_cfg['namespace']}.{ice_cfg['table']['name']}"

    # Inspect the columns in the snapshots table
    def inspect_snapshots_table(spark, iceberg_table):
        snapshots_df = spark.read.format("iceberg").load(f"{iceberg_table}.snapshots")
        snapshots_df.show(truncate=False)
        print(snapshots_df.columns)  # Print available columns

    # Example usage:
    inspect_snapshots_table(spark, iceberg_table)


    # Get the snapshot before the write
    pre_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # Read the data based on the file type
    df = read_data(spark, files_to_process, file_type, xml_row_tag)

    # Write the dataframe
    logging.info(f"Ingesting data:")
    logging.info(f"- Azure url: {ice_cfg['table']['location']}")
    logging.info(f"- Iceberg Table: {iceberg_table }")

    # Start timing
    start_time = time.time()

    # Write the DataFrame to the Iceberg table
    df.writeTo(iceberg_table ) \
        .option("merge-schema", "true") \
        .tableProperty("location", ice_cfg['table']['location']) \
        .createOrReplace()

    # Calculate time taken
    time_taken = time.time() - start_time

    # Get the snapshot after the write
    post_write_snapshot = get_latest_snapshot(spark, iceberg_table)

    # Get the new files written during the current operation
    new_files, total_size = get_new_files(spark, iceberg_table, pre_write_snapshot, post_write_snapshot)

    # Get the number of records written
    record_count = df.count()

    # Format the total size to a human-readable format
    formatted_size = format_size(total_size)

    # Log metrics
    logging.info('Success!')
    logging.info(f'- {record_count} records')
    logging.info(f'- {len(new_files)} file(s)')
    logging.info(f'- {time_taken:.2f} seconds')

# Azure Connection string from env var
def extract_conn_str_from_env_vars():
    for key, value in os.environ.items():
        if key.endswith('CONN_STR'):
            return value

def parse_cmd_line_args(args, kwargs):
    arg_parser = argparse.ArgumentParser(description="Ingest data from Azure Storage to Iceberg table")

    # Azure
    #   Input
    arg_parser.add_argument('--azure_container_input_name', default="data", help="Input data container name")
    arg_parser.add_argument('--azure_container_input_dir', required=True, help="Raw data directory in Azure Storage")
    #   Output
    arg_parser.add_argument('--azure_container_output_name', default="warehouse", help="Input data container name")
    arg_parser.add_argument('--azure_container_output_dir', default="iceberg", help="Warehouse directory for Iceberg tables")

    # Iceberg
    arg_parser.add_argument('--iceberg_catalog', required=True, help="Target Iceberg catalog name")
    arg_parser.add_argument('--iceberg_namespace', required=True, help="Target Iceberg namespace name")
    arg_parser.add_argument('--iceberg_table', required=True, help="Target Iceberg table name")

    # File Specific details
    arg_parser.add_argument('--file_type', required=True, help="[csv, json, xml, avro]")
    arg_parser.add_argument('--xml_row_tag', help="Row tag to use for XML format ")

    # Spark
    #   Driver
    arg_parser.add_argument('--spark_executor_memory', default="4g", help="Memory allocated to each Spark executor")
    arg_parser.add_argument('--spark_executor_cores', default="4", help="Number of cores allocated to each Spark executor")
    arg_parser.add_argument('--spark_executor_instances', default="1", help="Number of Spark executor instances")
    arg_parser.add_argument('--spark_sql_files_maxPartitionBytes', default="512m", help="Max partition bytes for Spark SQL files")

    #   Kubernetes mode
    arg_parser.add_argument('--k8s_name_space', help="Kubernetes name space")
    arg_parser.add_argument('--k8s_spark_image', help="Kubernetes mode for Spark")

    if kwargs and "run_args" in kwargs["context"]:
        arg_parser = arg_parser.parse_args(kwargs["context"]["run_args"])
    elif args and len(args) > 0:
        arg_parser = arg_parser.parse_args(args)
    else:
        arg_parser = arg_parser.parse_args()

    return arg_parser

def create_cfg_dict(args):
    conn_str = extract_conn_str_from_env_vars()
    storage_account_name, storage_account_key = parse_connection_string(conn_str)

    return {
        "azure": {
            "storage_account": {
                "name": storage_account_name,
                # "key": storage_account_key,
                "conn_str": conn_str,
            },
            "container": {
                "input": {
                    "name": args.azure_container_input_name,
                    "dir": args.azure_container_input_dir
                },
                "output": {
                    "name": args.azure_container_output_name,
                    "dir": args.azure_container_output_dir,
                }
            }
        },
        "iceberg": {
            "catalog": args.iceberg_catalog,
            "namespace": args.iceberg_namespace,
            "table": {
                "name": args.iceberg_table,
                "location": f"abfs://{args.azure_container_output_name}@{storage_account_name}.dfs.core.windows.net/{args.azure_container_output_dir}/{args.iceberg_namespace}/{args.iceberg_table}"
            }
        },
        "spark": {
            "k8s": {
                "spark_image": args.k8s_spark_image,
                "name_space": args.k8s_name_space,
            },
            "driver": {
                "spark.sql.files.maxPartitionBytes": args.spark_sql_files_maxPartitionBytes,
                "spark.executor.memory": args.spark_executor_memory,
                "spark.executor.cores": args.spark_executor_cores,
                "spark.executor.instances": args.spark_executor_instances,
            },
        }
    }

def filter_urls_by_file_type(blob_urls, file_type):
    # Filter expected file type
    blob_urls = [blob_url for blob_url in blob_urls if blob_url.endswith(file_type)]
    logging.info(f'- {len(blob_urls)} blobs of type: {file_type}')
    for blob_url in blob_urls:
        logging.info(f' - {blob_url}')
    return blob_urls

def determine_input_file_list(azure_cfg, file_type):
    azure_blob_urls = list_blobs_in_directory(azure_cfg)
    azure_blob_urls_filtered = filter_urls_by_file_type(azure_blob_urls, file_type)
    return azure_blob_urls_filtered

# Main function
def run(*args, **kwargs):

    # Organized cmd line args dictionary
    args = parse_cmd_line_args(args, kwargs)
    cfg = create_cfg_dict(args)

    # List the files from the Azure directory (data container)
    files_to_process = determine_input_file_list(cfg['azure'], args.file_type)
    if not files_to_process:
        logging.error("No files found in the specified directory.")
        return

    # Create Spark session
    spark = create_spark_session(cfg['spark'])

    # Ingest files into Iceberg table
    ingest_to_iceberg(cfg['iceberg'], spark, files_to_process, args.file_type, args.xml_row_tag)


if __name__ == "__main__":
    run()
