import logging
from azure.storage.blob import BlobServiceClient

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