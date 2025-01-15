from utilities.config import *
from utilities.azure import determine_files_to_process
from utilities.spark import create_spark_session
from utilities.iceberg import ingest_to_iceberg

# Main function
def run(*args, **kwargs):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Organized cmd line args dictionary
    args = parse_cmd_line_args(args, kwargs)
    cfg = create_cfg_dict(args)

    # Determine files tp process from Azure
    files_to_process = determine_files_to_process(cfg['azure'], cfg['file'])

    # Create Spark session
    spark = create_spark_session(cfg['iceberg']['table']['name'])

    for file in files_to_process:
        try:
            logging.info(f'------processing file ------')
            logging.info(f'{file}')
            # Ingest files into Iceberg table
            ingest_to_iceberg(cfg['iceberg'], cfg['file'], spark, [file])
            logging.info('------SUCCESS------')
        except Exception as e:
            logging.error('------FAILED------')
            logging.error(e)

    # End Spark Session
    spark.stop()
    logging.info(f"====================================")

if __name__ == "__main__":
    run()
