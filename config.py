import os
import argparse

from utilities.az import *

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
    arg_parser.add_argument('--iceberg_timeperiod', required=True, help="Partition by timeperiod")

    # File Specific details
    arg_parser.add_argument('--file_type', required=True, help="[csv, json, xml, avro]")
    arg_parser.add_argument('--xml_row_tag', help="Row tag to use for XML format ")
    arg_parser.add_argument('--json_multiline', action='store_true', help="if json is multiline separated")

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

# Azure Connection string from env var
def extract_conn_str_from_env_vars():
    for key, value in os.environ.items():
        if key.endswith('CONN_STR'):
            return value

def create_cfg_dict(args):
    conn_str = extract_conn_str_from_env_vars()
    storage_account_name, storage_account_key = parse_connection_string(conn_str)

    return {
        "file": {
            "type": args.file_type,
            "json_multiline": args.json_multiline,
            "xml_row_tag": args.xml_row_tag,
        },
        "azure": {
            "storage_account": {
                "name": storage_account_name,
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
            "timeperiod_loaded_by": args.iceberg_timeperiod,
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