import os
import argparse
import logging

def parse_cmd_line_args(args, kwargs):

    arg_parser = argparse.ArgumentParser(description="Ingest data from Azure Storage to Iceberg table")

    # Azure
    # - Input
    arg_parser.add_argument('--timeperiod_to_process', required=True, help="ie. yyyy/mm/dd/hh")
    arg_parser.add_argument('--azure_container_input_name', default="data", help="Input data container name")
    arg_parser.add_argument('--azure_container_input_dir', required=True, help="Raw data directory in Azure Storage")
    # - Output
    arg_parser.add_argument('--azure_container_output_name', default="warehouse", help="Input data container name")
    arg_parser.add_argument('--azure_container_output_dir', default="iceberg", help="Warehouse directory for Iceberg tables")

    # Iceberg
    arg_parser.add_argument('--iceberg_catalog', default='hogwarts_u', help="Target Iceberg catalog name")
    arg_parser.add_argument('--iceberg_namespace', required=True, help="Target Iceberg namespace name")
    arg_parser.add_argument('--iceberg_table', required=True, help="Target Iceberg table name")
    arg_parser.add_argument('--iceberg_partition_field', default="timeperiod_loaded_by", help="Column to partition with")
    arg_parser.add_argument('--iceberg_partition_value', help="value to partition by: yyyy/mm/dd/hh etc")
    arg_parser.add_argument('--iceberg_partition_format', required=True, help="partitioning format yyyy/MM/dd")
    arg_parser.add_argument('--iceberg_write_mode', default="append", help="spark write mode: append w/ mergeSchema or overwrite", choices=['append', 'overwrite'])

    # File Specific details
    arg_parser.add_argument('--file_type', required=True, help="[csv, json, xml, avro]")
    arg_parser.add_argument('--xml_row_tag', help="Row tag to use for XML format ")
    arg_parser.add_argument('--json_multiline', action='store_true', help="if json is multiline separated")
    arg_parser.add_argument('--log_files', action='store_true', help="log files to be processed")

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
    # if not conn_str:
    #     conn_str = get_conn_str_from_vault()
    storage_account_name, storage_account_key = parse_connection_string(conn_str)

    config_dict = {
        "file": {
            "type": args.file_type,
            "json_multiline": args.json_multiline,
            "xml_row_tag": args.xml_row_tag,
            "log_files": args.log_files,
        },
        "azure": {
            "storage_account": {
                "name": storage_account_name,
                "conn_str": conn_str,
            },
            "container": {
                "input": {
                    "name": args.azure_container_input_name,
                    "dir": f"{args.azure_container_input_dir}/{args.timeperiod_to_process}",
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
                "location": f"abfss://{args.azure_container_output_name}@{storage_account_name}.dfs.core.windows.net/"
                            f"{args.azure_container_output_dir}/{args.iceberg_namespace}/{args.iceberg_table}"
            },
            "partition": {
                "field": args.iceberg_partition_field,
                "format": args.iceberg_partition_format,
                "value": args.timeperiod_to_process,
            },
            "write_mode": args.iceberg_write_mode,
        },
    }

    return config_dict

def get_conn_str_from_vault():
    from hogwarts.auth.vault.vault_client import VaultClient
    logging.error(f"No azure connection string set as env var")
    logging.error(f"- retrieving connection string via hogwarts.auth.vault.vault_client library")

    vault = VaultClient()
    vault.login()
    group_name = 'APA4B-sg'
    secret_name = 'apdatalakeudatafeeds'
    s = vault.get_group_secret(group_name, secret_name)
    conn_str = s.get("conn_str")

    return conn_str

def parse_connection_string(conn_str):
    """Parse the Azure connection string to extract the AccountName and AccountKey. """

    if not conn_str:
        logging.error("Azure storage account connection string not provided as an Env Var")
        exit(1)

    # Split the connection string by semicolons to get the individual key-value pairs
    conn_dict = dict(item.split("=", 1) for item in conn_str.split(";"))
    account_name = conn_dict.get("AccountName")
    account_key = conn_dict.get("AccountKey")
    return account_name, account_key
