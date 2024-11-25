import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Setup logging
az_logger = logging.getLogger("azure")
az_logger.setLevel(logging.WARNING)


# Function to list blobs in a directory from Azure Blob Storage using connection string
def list_blobs_in_directory(container_name, container_client, container_dir):
    logging.info(f"- retrieving blob urls from [{container_name}]:{container_dir}")
    blobs = container_client.list_blobs(name_starts_with=container_dir)

    blob_urls = []
    for blob in blobs:
        blob_url = f"abfss://{container_name}@{container_client.account_name}.dfs.core.windows.net/{blob.name}"
        logging.debug(blob_url)
        blob_urls.append(blob_url)
    logging.info(f" - {len(blob_urls)} blobs total")

    return blob_urls

def filter_urls_by_file_type(blob_urls, file_type_filter, log_files_flag):
    # Filter expected file type
    logging.info(f'- blobs total: {len(blob_urls)}')
    blob_urls = [blob_url for blob_url in blob_urls if blob_url.endswith(file_type_filter)]
    logging.info(f"- blobs of type '{file_type_filter}': {len(blob_urls)}")

    if log_files_flag:
        for blob_url in blob_urls:
            logging.info(f' - {blob_url}')

    return blob_urls

def determine_files_to_process(azure_cfg, cfg_file):
    """
    Retrieves Azure blob URLs from a specified time period onwards.
    Supports time period formats: yyyy/mm/dd and yyyy/mm/dd/hh.
    :param azure_cfg: Azure configuration dictionary
    :param cfg_file: Configuration file containing the 'catchup' and timestamp details
    :return: List of blob URLs to process
    """

    blob_service_client = BlobServiceClient.from_connection_string(azure_cfg['storage_account']['conn_str'])
    container_client = blob_service_client.get_container_client(azure_cfg['container']['input']['name'])
    logging.info(f"")
    logging.info(f"Connected to: {container_client.url}")

    if not cfg_file['catchup']:
        # single directory to process
        directories_to_search = [f"{azure_cfg['container']['input']['dir']}/{azure_cfg['container']['input']['dir_timeperiod']}"]
    else:
        # Get all directories >= provided timeperiod
        logging.info('- catchup flag: ENABLED')

        directories_to_search = filter_directories_by_timeperiod(
            container_client, azure_cfg['container']['input']['dir'],
            azure_cfg['container']['input']['dir_timeperiod']
        )

    azure_blob_urls = []
    for dir in directories_to_search:
        azure_blob_urls += list_blobs_in_directory(azure_cfg['container']['input']['name'], container_client, dir,)

    # Filter blobs based on file extension
    azure_blob_urls_filtered = filter_urls_by_file_type(azure_blob_urls, cfg_file['type'], cfg_file['log_files'])
    if not azure_blob_urls_filtered:
        logging.warning("")
        logging.warning("No files to be processed. Marking as Skip")
        logging.warning("")
        raise SystemExit(99)

    return azure_blob_urls_filtered

def filter_directories_by_timeperiod(container_client, directory_prefix, timeperiod_str):
    # Determine time period format
    if len(timeperiod_str.split("/")) == 3:
        date_format = "%Y/%m/%d"
    if len(timeperiod_str.split("/")) == 4:
        date_format = "%Y/%m/%d/%H"
    else:
        logging.error(f"invalid timeperiod supplied: {timeperiod_str}")
        exit(1)

    start_time = datetime.strptime(timeperiod_str, date_format)

    # Iterate through blobs to find directories with valid timestamps
    valid_directories = set()
    for blob in container_client.list_blobs(name_starts_with=directory_prefix):
        blob_path = blob.name
        try:
            # Extract the last three or four components (yyyy/mm/dd or yyyy/mm/dd/hh)
            parts = blob_path.split("/")
            timestamp_parts = parts[-4:] if len(parts[-1]) == 2 else parts[-3:]  # Check if last part is "hh"
            timestamp_str = "/".join(timestamp_parts)

            # Parse the timestamp and check against the start_time
            if len(timestamp_parts) == 3:
                blob_timestamp = datetime.strptime(timestamp_str, "%Y/%m/%d")
            elif len(timestamp_parts) == 4:
                blob_timestamp = datetime.strptime(timestamp_str, "%Y/%m/%d/%H")

            # Add the directory to the list if the timestamp is valid
            if start_time <= blob_timestamp:
                directory = "/".join(parts[:-1])  # Get the directory part (exclude filename)
                valid_directories.add(directory)

        except ValueError:
            # Skip paths that don't conform to the expected timestamp format
            continue

    return sorted(list(valid_directories))
