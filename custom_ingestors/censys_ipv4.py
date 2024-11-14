import logging

def apply_custom_rules(df, spark):
    spark_sql_cmds = [
        "DROP TABLE IF EXISTS hogwarts_u.test.censys_ipv4 PURGE",
        "USE hogwarts_u.test",
        "SHOW TABLES LIKE 'censys_ipv4_stg'"
    ]
    
    for cmd in spark_sql_cmds:
        logging.info(f"Executing SQL command: {cmd}")
        spark.sql(cmd)

    table_exists = spark.sql("SHOW TABLES LIKE 'censys_ipv4_stg'").count() > 0
    
    if table_exists:
        alter_cmd = "ALTER TABLE test.censys_ipv4_stg RENAME TO test.censys_ipv4"
        logging.info(f"Executing SQL command: {alter_cmd}")
        spark.sql(alter_cmd)

    final_cmd = "SHOW TABLES IN hogwarts_u.test LIKE 'censys%'"
    logging.info(f"Executing SQL command: {final_cmd}")
    spark.sql(final_cmd)
    
    return df
