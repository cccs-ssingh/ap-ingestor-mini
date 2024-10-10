from config import *
from utilities.az import *
from utilities.spark import *

# Main function
def run(*args, **kwargs):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Organized cmd line args dictionary
    args = parse_cmd_line_args(args, kwargs)
    cfg = create_cfg_dict(args)

    # Determine files tp process from Azure
    files_to_process = determine_input_file_list(cfg['azure'], args.file_type)
    if not files_to_process:
        logging.error("No files found in the specified directory.")
        return

    # Create Spark session
    spark = create_spark_session(cfg['spark'])

    # Ingest files into Iceberg table
    ingest_to_iceberg(cfg['iceberg'], cfg['file'], spark, files_to_process, args.xml_row_tag)


if __name__ == "__main__":
    run()
