from utilities.config import *
from utilities.azure import determine_files_to_process
from utilities.spark import create_spark_session
from utilities.iceberg import ingest_to_iceberg
import logging
# Main function
def run(*args, **kwargs):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Organized cmd line args dictionary
    args = parse_cmd_line_args(args, kwargs)
    cfg = create_cfg_dict(args)

    # Determine files tp process from Azure
    files_to_process = determine_files_to_process(cfg['azure'], cfg['file'])
    
    spark = create_spark_session(cfg['iceberg']['table']['name'])
    ingest_to_iceberg(cfg['iceberg'], cfg['file'], spark, files_to_process)

    


if __name__ == "__main__":
    run()
