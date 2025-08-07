from pyspark.sql import SparkSession


def get_spark_session(app_name="ETL Job"):
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    return spark
