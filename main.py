from config import *
from utilities.az import *
from utilities.spark import *

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
