import os
import argparse
import logging
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
def create_spark_session(az_cfg, spark_cfg):
    logging.info("Creating Spark session")
    # logging.info(f"- warehouse url: {az_cfg['container']['warehouse']['url']}")
    # .config(f"spark.sql.catalog.{spark_cfg['catalog']}.dir", az_cfg['container']['warehouse']['dir']) \
    # .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \

    # .config(f"spark.sql.catalog.{spark_cfg['catalog']}.warehouse", az_cfg['container']['warehouse']['url']) \
    # warehouse_url = "abfs://<your-container>@<your-storage-account>.dfs.core.windows.net/<warehouse-dir>"
    warehouse_url = "abfs://warehouse@apdatalakeudatafeeds.dfs.core.windows.net/iceberg"

    # .config(f"spark.hadoop.fs.azure.account.key.{az_cfg['storage_acct']['name']}.blob.core.windows.net", az_cfg['storage_acct']['key']) \
    #         .config(f"spark.sql.catalog.{spark_cfg['catalog']}", "org.apache.iceberg.spark.SparkCatalog") \
    #         .config(f"spark.sql.catalog.{spark_cfg['catalog']}.type", "hadoop") \
    #         .config(f"spark.hadoop.fs.azure.account.key.apdatalakeudatafeeds.blob.core.windows.net", az_cfg['storage_acct']['key']) \

    # Spark session configuration
    spark_builder = SparkSession.builder \
        .appName("Iceberg Ingestion with Azure Storage") \
        .config(             "spark.executor.cores", spark_cfg['driver']["spark.executor.cores"]) \
        .config(            "spark.executor.memory", spark_cfg['driver']["spark.executor.memory"]) \
        .config(         "spark.executor.instances", spark_cfg['driver']["spark.executor.instances"]) \
        .config("spark.sql.files.maxPartitionBytes", spark_cfg['driver']["spark.sql.files.maxPartitionBytes"]) \
        .config(f"spark.sql.catalog.apdatalakeudatafeeds.warehouse", warehouse_url) \
        .config(f"spark.sql.catalog.apdatalakeudatafeeds.dir", warehouse_url) \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0") # xml support

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

    # Print spark config
    for key, value in spark.sparkContext.getConf().getAll():
        logging.warning(f"{key}: {value}")

    return spark


# Function to list blobs in a directory from Azure Blob Storage using connection string
def list_blobs_in_directory(azure_cfg):
    blob_service_client = BlobServiceClient.from_connection_string(azure_cfg['storage_acct']['conn_str'])
    container_client = blob_service_client.get_container_client(azure_cfg['container']['data']['name'])
    logging.info(f"Connected to: {container_client.url}")
    logging.info(f"- retrieving blobs from container '{azure_cfg['container']['data']['name']}' in directory '{azure_cfg['container']['data']['input_dir']}'")
    blobs = container_client.list_blobs(name_starts_with=azure_cfg['container']['data']['input_dir'])

    blob_urls = []
    for blob in blobs:
        blob_url = f"abfs://{azure_cfg['container']['data']['name']}@{container_client.account_name}.dfs.core.windows.net/{blob.name}"
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
def ingest_to_iceberg(spark, azure_cfg, blob_urls, file_type, xml_row_tag=None):

    # # test azure connection
    # test_warehouse_url = azure_cfg['container']['warehouse']['url']
    # # test_warehouse_url = f"abfs://warehouse@{azure_cfg['storage_acct']['name']}.dfs.core.windows.net/iceberg/test/ingestor_mini"
    # logging.info(f'Testing writing to warehouse: {test_warehouse_url}')
    # df = spark.createDataFrame([(1, 'test')], ['id', 'value'])
    # df.write.csv(test_warehouse_url)
    # logging.info('- success')

    # Read the data based on the file type
    df = read_data(spark, blob_urls, file_type, xml_row_tag)

    # Write the dataframe
    logging.info(f"Ingesting data into Iceberg table: {azure_cfg['container']['warehouse']['url']}")
    # df.writeTo(f"{spark_cfg['catalog']}.{spark_cfg['table']}") \
    df.writeTo(f"hogwarts_u.test.kaspersky_json") \
        .option("merge-schema", "true") \
        .createOrReplace()
    logging.info("- data ingested successfully")

def parse_cmd_line_args(args, kwargs):

    logging.debug(f"args: {args}")
    logging.debug(f"kwargs: {kwargs}")
    arg_parser = argparse.ArgumentParser(description="Ingest data from Azure Storage to Iceberg table")

    # Azure config arguments
    #  Input
    arg_parser.add_argument('--data_container', default="data",
                        help="Azure Storage container name for input data (default: 'data')")
    arg_parser.add_argument('--input_data_dir', required=True, help="Raw data directory in Azure Storage")
    arg_parser.add_argument('--file_type', required=True,
                        help="File type of the input data (e.g., 'csv', 'parquet', 'json', 'xml', 'avro')")
    #  Output
    arg_parser.add_argument('--catalog', required=True, help="Spark catalog name")
    arg_parser.add_argument('--database', required=True, help="Spark catalog database name")
    arg_parser.add_argument('--table', required=True, help="Target Iceberg table name")
    arg_parser.add_argument('--warehouse_container', default="warehouse",
                        help="Azure Storage container name for Iceberg warehouse (default: 'warehouse')")
    arg_parser.add_argument('--warehouse_dir', required=True, help="Warehouse directory for Iceberg tables")
    arg_parser.add_argument('--xml_row_tag', help="Row tag to use for XML format (only required if file_type is 'xml')")

    # Kubernetes mode config arguments
    arg_parser.add_argument('--k8s_name_space', help="Kubernetes name space")
    arg_parser.add_argument('--k8s_spark_image', help="Kubernetes mode for Spark")

    # Driver config arguments for Spark
    arg_parser.add_argument('--spark_sql_files_maxPartitionBytes', default="512m", help="Max partition bytes for Spark SQL files")
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

# Azure Connection string from env var
def extract_conn_str_from_env_vars():
    for key, value in os.environ.items():
        if key.endswith('CONN_STR'):
            return value

def create_cfg_dict(args):
    conn_str = extract_conn_str_from_env_vars()
    storage_acct_name, storage_acct_key = parse_connection_string(conn_str)

    return {
        "azure": {
            "storage_acct": {
                "name": storage_acct_name,
                "key": storage_acct_key,
                "conn_str": conn_str,
            },
            "container": {
                "data": {
                    "name": args.data_container,
                    "input_dir": args.input_data_dir
                },
                "warehouse": {
                    "name": args.warehouse_container,
                    "dir": args.warehouse_dir,
                    "url": f"abfs://{args.warehouse_container}@{storage_acct_name}.dfs.core.windows.net/{args.warehouse_dir}/{args.table}"
                }
            }
        },
        "spark": {
            "catalog": args.catalog,
            "database": args.database,
            "table": args.table,
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

# Main function
def run(*args, **kwargs):

    # Organized cmd line args dictionary
    args = parse_cmd_line_args(args, kwargs)
    cfg = create_cfg_dict(args)

    # List the files from the Azure directory (data container)
    azure_blob_urls = list_blobs_in_directory(cfg['azure'])
    azure_blob_urls = filter_urls_by_file_type(azure_blob_urls, args.file_type)
    if not azure_blob_urls:
        logging.error("No files found in the specified directory.")
        return

    # Create Spark session
    spark = create_spark_session(cfg['azure'], cfg['spark'])

    # Ingest files into Iceberg table
    ingest_to_iceberg(spark, cfg['azure'], azure_blob_urls, args.file_type, args.xml_row_tag)


if __name__ == "__main__":
    run()
