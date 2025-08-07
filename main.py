from utils.spark_session import get_spark_session
from etl_job.s3_extractor import extract_from_s3
from etl_job.data_cleaning import clean_data
from etl_job.data_modeling import model_data
from etl_job.snowflake_loader import load_to_snowflake


def main():
    spark = get_spark_session("supply-chain-pipeline")

    # Extract data from AWS S3 Bucket
    print("extracting data from the data source")
    df_pandas = extract_from_s3()

    # Create Spark Dataframe
    print("creating spark dataframe")
    df_raw = spark.createDataFrame(df_pandas)
    
    #Clean the raw data
    print("cleaning the raw data")
    df_cleaned = clean_data(df_raw)
    print("raw data has been successfully cleaned")

    #Perform dimensional modeling
    print("modeling the data")
    modeled_data = model_data(df_cleaned, spark)

    for table_name, df in modeled_data.items():
        print(f"loading {table_name} to Snowflake...")
        load_to_snowflake(df, table_name)
    
    print("ETL pipeline completed.")

if __name__ == "__main__":
    main()
