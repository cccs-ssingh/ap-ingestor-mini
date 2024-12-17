import logging
from pyspark.sql import SparkSession


# Function to create Spark session with Iceberg
def create_spark_session(app_name):
    logging.debug(f"")
    logging.debug("Creating Spark session")

    spark_builder = SparkSession.builder \
        .appName(f"APA4b Ingestor-Mini: {app_name}") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.debug.maxToStringFields", "100")

    spark = spark_builder.getOrCreate()
    log_spark_config(spark)

    return spark

def log_spark_config(spark):
    # Extract and log relevant configuration settings

    # all_configs = spark.sparkContext.getConf().getAll()
    # for key, value in sorted(all_configs):
    #     print(f"{key}: {value}")

    # Access the Spark configuration
    conf = spark.sparkContext.getConf()
    logging.info("")
    logging.info("=====  Spark Session Configuration ====")
    logging.info(f"          App Name: {conf.get('spark.app.name')}")
    logging.info(f"            Master: {conf.get('spark.master')}")
    logging.info(f"     Driver Memory: {conf.get('spark.driver.memory', 'Not Set')}")
    logging.info(f"   Executor Memory: {conf.get('spark.executor.memory', 'Not Set')}")
    logging.info(f"    Executor Cores: {conf.get('spark.executor.cores', 'Not Set')}")
    logging.info(f"Executor Instances: {conf.get('spark.executor.instances', 'Not Set')}")
    logging.info(f"         Cores MAX: {conf.get('spark.cores.max', 'Not Set')}")
    # logging.info(f"   Shuffle Service: {conf.get('spark.shuffle.service.enabled', 'Not Set')}")
    # logging.info(f"Dynamic Allocation: {conf.get('spark.dynamicAllocation.enabled', 'Not Set')}")
    # logging.info(f"   Memory Fraction: {conf.get('spark.memory.fraction', 'Not Set')}")
    # logging.info(f"Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions', 'Not Set')}")
    logging.info("=======================================")

# Read data based on the file type
def read_data(spark, file_cfg, input_files):
    logging.info(f"Reading input data")

    # remove compression extension and extract native file type
    if file_cfg['type'].endswith('.gz'):
        file_cfg['type'] = file_cfg['type'].split('.')[0]

    # Supported file types
    if file_cfg['type'] == "csv":
        df = spark.read.option("header", "true").csv(input_files)

    elif file_cfg['type'] == "parquet":
        df = spark.read.parquet(input_files)

    elif file_cfg['type'] == "avro":
        df = spark.read.format("avro").load(input_files)

    elif file_cfg['type'] == "json" or file_cfg['type'] == "stix":
        if file_cfg['json_multiline']:
            logging.info(f"- json_multiline: {file_cfg['json_multiline']}")
            df = spark.read.option("multiLine", "true").json(input_files)
        else:
            df = spark.read.json(input_files)

    elif file_cfg['type'] == "xml":  # uses databricks library
        if not file_cfg["xml_row_tag"]:
            raise ValueError("For XML format, 'xml_row_tag' must be provided.")

        logging.info(f"- xml_row_tag: '{file_cfg['xml_row_tag']}'")

        # XML library takes in a XML direcotry, not a file list so we just parse it out
        first_file = input_files[0]
        input_dir = first_file.rsplit('/', 1)[0]

        df = (
            spark.read.format("xml")
            .option("rowTag", file_cfg["xml_row_tag"])
            .load(f"{input_dir}/*.xml")
        )

    else:
        raise ValueError(f"Unsupported file type: {file_cfg['type']}")

    logging.info(f"- successfully read data!")
    return df

