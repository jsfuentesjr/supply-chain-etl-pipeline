# SUPPLY CHAIN ETL PIPELINE
An end-to-end ETL pipeline that extracts supply chain data from AWS S3, performs data cleaning and dimensional modeling using PySpark, and loads the final data into Snowflake for analytics.

### Project Architecture
<img width="812" height="188" alt="Image" src="https://github.com/user-attachments/assets/ee7007b7-ef30-4a1d-87f4-dbcff11cef9a" />

This ETL project extracts raw supply chain data from AWS S3 using boto3 (AWS SDK for Python), transforms it with PySpark by cleaning the data—ensuring correct data types, handling missing values, standardizing formats, and improving overall data quality—and then applies dimensional modeling to structure the data into a star schema with fact and dimension tables. The final transformed data is loaded into Snowflake for efficient querying and analytics.

