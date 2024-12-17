from pyspark.sql.functions import to_timestamp

def apply_custom_rules(df):

    df = df.withColumn("scan_time", to_timestamp("scan_time"))

    return df