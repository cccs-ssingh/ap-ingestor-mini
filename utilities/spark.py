import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


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
    logging.info("=====  Spark Session Configuration ====")
    logging.info(f"          App Name: {conf.get('spark.app.name')}")
    logging.info(f"            Master: {conf.get('spark.master')}")
    logging.info(f"     Driver Memory: {conf.get('spark.driver.memory', 'Not Set')}")
    logging.info(f"   Executor Memory: {conf.get('spark.executor.memory', 'Not Set')}")
    logging.info(f"    Executor Cores: {conf.get('spark.executor.cores', 'Not Set')}")
    logging.info(f"Executor Instances: {conf.get('spark.executor.instances', 'Not Set')}")
    logging.info(f"         Cores MAX: {conf.get('spark.cores.max', 'Not Set')}")
    logging.info(f"   Shuffle Service: {conf.get('spark.shuffle.service.enabled', 'Not Set')}")
    logging.info(f"Dynamic Allocation: {conf.get('spark.dynamicAllocation.enabled', 'Not Set')}")
    logging.info(f"   Memory Fraction: {conf.get('spark.memory.fraction', 'Not Set')}")
    logging.info(f"Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions', 'Not Set')}")
    logging.info("=======================================")

# Read data based on the file type

def read_data(spark, file_cfg, input_files):
    logging.info(f"Reading input data")

    # Initialize an empty DataFrame
    df = spark.createDataFrame([], schema=None)
    problematic_file = None

    for file in input_files:
        try:
            logging.info(f"Processing file: {file}")

            if file_cfg['type'] == "csv":
                temp_df = spark.read.option("header", "true").csv(file)

            elif file_cfg['type'] == "parquet":
                temp_df = spark.read.parquet(file)

            elif file_cfg['type'] == "avro":
                temp_df = spark.read.format("avro").load(file)

            elif file_cfg['type'] == "json":
                if file_cfg['json_multiline']:
                    logging.info(f"- json_multiline: {file_cfg['json_multiline']}")
                    temp_df = spark.read.option("multiLine", "true").json(file)
                else:
                    temp_df = spark.read.json(file)

            elif file_cfg['type'] == "xml":
                if not file_cfg["xml_row_tag"]:
                    raise ValueError("For XML format, 'xml_row_tag' must be provided.")

                logging.info(f"- xml_row_tag: '{file_cfg['xml_row_tag']}'")

                input_dir = file.rsplit('/', 1)[0]

                temp_df = (
                    spark.read.format("xml")
                    .option("rowTag", file_cfg["xml_row_tag"])
                    .load(f"{input_dir}/*.xml")
                )

            else:
                raise ValueError(f"Unsupported file type: {file_cfg['type']}")

            # Merge the schema by unioning the DataFrames
            df = df.unionByName(temp_df, allowMissingColumns=True)

        except Exception as e:
            logging.error(f"Error processing file: {file}")
            logging.error(e)
            problematic_file = file
            break

    if problematic_file:
        logging.info(f"Problematic file: {problematic_file}")
    else:
        logging.info(f"- successfully read data!")

    return df

