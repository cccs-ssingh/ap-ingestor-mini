import logging

def apply_custom_rules(df, spark):

    spark_sql_cmds = [
        "DROP TABLE IF EXISTS hogwarts_u.test.censys_ipv4 PURGE",
        "USE hogwarts_u.test",
        "ALTER TABLE IF EXISTS test.censys_ipv4_stg RENAME TO test.censys_ipv4",
        "SHOW TABLES IN hogwarts_u.test LIKE 'censys%'"
    ]
    for cmd in spark_sql_cmds:
        logging.info(f"Executing SQL command: {cmd}")
        spark.sql(cmd)

    return df