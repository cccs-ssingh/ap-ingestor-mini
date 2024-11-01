def apply_custom_rules_to_df(df):
    df = df.withColumn("cveTags", from_json(col("cveTags").cast("string"), ArrayType(StringType(), True)))