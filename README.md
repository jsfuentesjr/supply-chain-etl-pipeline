# SUPPLY CHAIN ETL PIPELINE
An end-to-end ETL pipeline that extracts supply chain data from AWS S3, performs data cleaning and dimensional modeling using PySpark, and loads the final data into Snowflake for analytics.

### Project Architecture
<img width="812" height="188" alt="Image" src="https://github.com/user-attachments/assets/ee7007b7-ef30-4a1d-87f4-dbcff11cef9a" />

This ETL pipeline is designed to extract raw supply chain data from AWS S3, transform it using PySpark, and load the modeled data into Snowflake for analytics and reporting.

<p>**1. Extraction — AWS S3 with boto3**</p>
  <p>The pipeline connects to AWS S3 using the boto3 SDK and AWS APIs.</p>

  Raw data files (CSV/JSON) are retrieved from a specified S3 bucket.

  The extraction step ensures secure and efficient data retrieval for downstream processing.

**2. Transformation — Data Cleaning & Modeling with PySpark**
Using PySpark, the data undergoes a comprehensive transformation process:
Data Cleaning:
Ensures correct data types for all columns.
Standardizes formats for values such as dates, product names, and locations.
Handles missing values through imputation or filtering based on business logic.
Validates and improves data quality by removing duplicates and enforcing value constraints.
Dimensional Modeling:
The cleaned data is organized into a star schema:
Dimension tables: product_dim, customer_dim, date_dim, etc.
Fact tables: sales_fact, shipment_fact
Keys are generated, and relationships between dimensions and facts are maintained according to best practices in dimensional design.
**3. Loading — Snowflake**
Transformed dimension and fact tables are loaded into Snowflake using optimized batch writes.
Tables are created or replaced using SQL scripts that define primary keys, foreign keys, and data types.
The final schema is stored under the supplychain.public database, ready for BI tools and SQL analytics.

