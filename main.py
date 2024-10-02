import os
import argparse
import logging
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
# logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s - %(message)s')


# Function to create Spark session with Iceberg and XML support
def create_spark_session(warehouse_dir, k8s_config, driver_config):
    logging.info("Creating Spark session for Iceberg and XML support.")

    # Basic Spark session configuration
    spark_builder = SparkSession.builder \
        .appName("Iceberg Ingestion") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", warehouse_dir) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.files.maxPartitionBytes", driver_config.get("spark.sql.files.maxPartitionBytes", "512m")) \
        .config("spark.executor.memory", driver_config.get("spark.executor.memory", "4g")) \
        .config("spark.executor.cores", driver_config.get("spark.executor.cores", "4")) \
        .config("spark.executor.instances", driver_config.get("spark.executor.instances", "1")) \
        .config("spark.jars.packages", driver_config.get("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0"))

    if k8s_config['name_space']:
        logging.info("Configuring Spark for Kubernetes mode.")
        spark_builder = spark_builder \
            .config("spark.master", "k8s://https://kubernetes.default.svc") \
            .config("spark.kubernetes.container.image", k8s_config['spark_image']) \
            .config("spark.kubernetes.namespace", k8s_config['name_space']) \
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")

    spark = spark_builder.getOrCreate()
    return spark


# Function to list blobs in a directory from Azure Blob Storage using connection string
def list_blobs_in_directory(conn_str, container_name, raw_data_dir):
    logging.info(f"Listing blobs from container '{container_name}' in directory '{raw_data_dir}'")
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs(name_starts_with=raw_data_dir)
    blob_urls = []

    for blob in blobs:
        blob_url = f"https://{container_client.account_name}.blob.core.windows.net/{container_name}/{blob.name}"
        blob_urls.append(blob_url)

    return blob_urls


# Function to read data based on the file type
def read_data(spark, input_files, file_type, xml_row_tag=None):
    logging.info(f"Reading data from input files with file type: {file_type}")

    if file_type == "csv":
        df = spark.read.option("header", "true").csv(input_files)
    elif file_type == "parquet":
        df = spark.read.parquet(input_files)
    elif file_type == "json":
        df = spark.read.json(input_files)
    elif file_type == "xml":
        if not xml_row_tag:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")
        df = (
            spark.read.format("xml")
            .option("rowTag", xml_row_tag)
            .load(input_files)
        )
    elif file_type == "avro":
        df = spark.read.format("avro").load(input_files)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    return df


# Function to ingest raw data into an Iceberg table dynamically
def ingest_to_iceberg(spark, input_files, table_name, file_type, xml_row_tag=None):
    # Read the data based on the file type
    df = read_data(spark, input_files, file_type, xml_row_tag)

    logging.info(f"Ingesting data into Iceberg table: {table_name}")
    df.writeTo(f"spark_catalog.{table_name}") \
        .option("merge-schema", "true") \
        .createOrReplace()

def parse_cmd_line_args(args, kwargs):

    logging.debug(f"args: {args}")
    logging.debug(f"kwargs: {kwargs}")

    arg_parser = argparse.ArgumentParser(description="Ingest data from Azure Storage to Iceberg table")
    arg_parser.add_argument('--data_container_name', default="data",
                        help="Azure Storage container name for raw data (default: 'data')")
    arg_parser.add_argument('--raw_data_dir', required=True, help="Raw data directory in Azure Storage")
    arg_parser.add_argument('--file_type', required=True,
                        help="File type of the raw data (e.g., 'csv', 'parquet', 'json', 'xml', 'avro')")
    arg_parser.add_argument('--table_name', required=True, help="Target Iceberg table name")
    arg_parser.add_argument('--warehouse_container_name', default="warehouse",
                        help="Azure Storage container name for Iceberg warehouse (default: 'warehouse')")
    arg_parser.add_argument('--warehouse_dir', required=True, help="Warehouse directory for Iceberg tables")
    arg_parser.add_argument('--xml_row_tag', help="Row tag to use for XML format (only required if file_type is 'xml')")

    # Kubernetes mode config arguments
    arg_parser.add_argument('--k8s_name_space', help="Kubernetes name space")
    arg_parser.add_argument('--k8s_spark_image', help="Kubernetes mode for Spark")

    # Driver config arguments for Spark
    arg_parser.add_argument('--spark_sql_files_maxPartitionBytes', default="512m",
                        help="Max partition bytes for Spark SQL files")
    arg_parser.add_argument('--spark_executor_memory', default="4g", help="Memory allocated to each Spark executor")
    arg_parser.add_argument('--spark_executor_cores', default="4", help="Number of cores allocated to each Spark executor")
    arg_parser.add_argument('--spark_executor_instances', default="1", help="Number of Spark executor instances")

    if kwargs and "run_args" in kwargs["context"]:
        # parse airflow args via kwargs
        arg_parser = arg_parser.parse_args(kwargs["context"]["run_args"])
    elif args and len(args) > 0:
        # parse airflow args via args
        arg_parser = arg_parser.parse_args(args)
    else:
        # parse local args
        arg_parser = arg_parser.parse_args()

    return arg_parser


# Main function
def run(*args, **kwargs):
    args = parse_cmd_line_args(args, kwargs)

    # Create driver configuration dictionary
    driver_config = {
        "spark.sql.files.maxPartitionBytes": args.spark_sql_files_maxPartitionBytes,
        "spark.executor.memory": args.spark_executor_memory,
        "spark.executor.cores": args.spark_executor_cores,
        "spark.executor.instances": args.spark_executor_instances,
    }

    # Kubernetes configuration dictionary
    k8s_config = {
        "spark_image": args.k8s_spark_image,
        "name_space": args.k8s_name_space,
    }

    # Azure Connection string from env var
    conn_str = None
    for key, value in os.environ.items():
        if key.endswith('_CONN_STR'):
            conn_str = value
            break

    # Create Spark session with driver configs and Kubernetes mode support
    warehouse_url = f"https://{args.warehouse_container_name}.blob.core.windows.net/{args.warehouse_dir}"
    spark = create_spark_session(warehouse_url, k8s_config, driver_config)

    # List the files from the Azure directory (data container)
    input_files = list_blobs_in_directory(
        conn_str=conn_str,
        container_name=args.data_container_name,
        raw_data_dir=args.raw_data_dir
    )

    if not input_files:
        logging.warning("No files found in the specified directory.")
        return

    # Ingest files into Iceberg table
    ingest_to_iceberg(spark, input_files, args.table_name, args.file_type, args.xml_row_tag)

    logging.info(f"Successfully ingested data into Iceberg table: {args.table_name}")


if __name__ == "__main__":
    run()
