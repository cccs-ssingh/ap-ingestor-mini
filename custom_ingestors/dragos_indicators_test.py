import logging
import pyspark.sql.functions as f


def apply_custom_rules(df):
    #  The objects column is an array of structs.
    #  We need to explode it to then be able to select all of its inner columns.
    #  After that, we can filter the type

    df = df.select(f.explode(f.col("objects")).alias("objects"))
    df = df.select("objects.*")
    df = df.filter(f.col("type") == "indicator")

    if 'abstract' not in df.columns:
        logging.info("- adding column 'abstract'")
        df = df.withColumn('abstract', f.lit(None))

    return df