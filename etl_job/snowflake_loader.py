from config import SNOWFLAKE_CONFIG
from pyspark.sql import DataFrame

def load_to_snowflake(df: DataFrame, table_name: str):
    df.write \
      .format("snowflake") \
      .option("dbtable", table_name) \
      .option("sfURL", SNOWFLAKE_CONFIG["sfURL"]) \
      .option("sfDatabase", SNOWFLAKE_CONFIG["sfDatabase"]) \
      .option("sfSchema", SNOWFLAKE_CONFIG["sfSchema"]) \
      .option("sfWarehouse", SNOWFLAKE_CONFIG["sfWarehouse"]) \
      .option("sfRole", SNOWFLAKE_CONFIG["sfRole"]) \
      .option("sfUser", SNOWFLAKE_CONFIG["sfUser"]) \
      .option("sfPassword", SNOWFLAKE_CONFIG["sfPassword"]) \
      .mode("overwrite") \
      .save()
