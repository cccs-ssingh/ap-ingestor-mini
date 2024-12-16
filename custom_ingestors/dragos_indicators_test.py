import logging
import pyspark.sql.functions as f


def apply_custom_rules(df):
    #  The objects column is an array of structs.
    #  We need to explode it to then be able to select all of its inner columns.
    #  After that, we can filter the type

    df = df.select(f.explode(f.col("objects")).alias("objects"))
    df = df.select("objects.*")
    df = df.filter(f.col("type") == "indicator")

    for col_name in ['abstract', 'aliases']:
        if col_name not in df.columns:
            logging.info(f"- column '{col_name}' required but missing in dataframe")
            logging.info(f" - adding empty column with Null values to match Schema")
            df = df.withColumn(col_name, f.lit(None))

    return df