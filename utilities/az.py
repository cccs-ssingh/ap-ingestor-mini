import logging
from azure.storage.blob import BlobServiceClient

# Setup logging
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

# Function to list blobs in a directory from Azure Blob Storage using connection string
def list_blobs_in_directory(conn_str, container_name, container_dir):
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    logging.info(f"")
    logging.info(f"Connected to: {container_client.url}")
    logging.info(f"- retrieving blob urls from  [{container_name}]:{container_dir}")
    blobs = container_client.list_blobs(name_starts_with=container_dir)

    blob_urls = []
    for blob in blobs:
        blob_url = f"abfss://{container_name}@{container_client.account_name}.dfs.core.windows.net/{blob.name}"
        logging.debug(blob_url)
        blob_urls.append(blob_url)
    logging.debug(f"- {len(blob_urls)} blobs total")

    return blob_urls

def filter_urls_by_file_type(blob_urls, file_type):
    # Filter expected file type
    blob_urls = [blob_url for blob_url in blob_urls if blob_url.endswith(file_type)]
    logging.info(f"- {len(blob_urls)} blobs of type: {file_type}")

    # # Print files to process if needed
    # for blob_url in blob_urls:
    #     logging.info(f' - {blob_url}')

    return blob_urls

def determine_files_to_process(azure_cfg, file_type):

    if not azure_cfg['storage_account']['conn_str']:
        azure_cfg['storage_account']['conn_str'] = get_conn_str_from_vault()

    azure_blob_urls = list_blobs_in_directory(
        azure_cfg['storage_account']['conn_str'],
        azure_cfg['container']['input']['name'],
        azure_cfg['container']['input']['dir'],
    )
    azure_blob_urls_filtered = filter_urls_by_file_type(azure_blob_urls, file_type)
    return azure_blob_urls_filtered

def get_conn_str_from_vault():
    from hogwarts.auth.vault.vault_client import VaultClient

    logging.info("getting spellbooksecret from vault")

    vault = VaultClient()
    vault.login()

    # apa4b-sg is the group name, apdatalakeudatafeeds is the secret name
    s = vault.get_group_secret('APA4B-sg', 'apdatalakeudatafeeds')

    # Key inside the secret
    conn_str = s.get("conn_str")
    # conn_str is now the conn_str in the APA4B_SG_APDATALAKEUDATAFEEDS_CONN_STR secret

    return conn_str