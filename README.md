# SUPPLY CHAIN ETL PIPELINE
An end-to-end ETL pipeline that extracts supply chain data from AWS S3, performs data cleaning and dimensional modeling using PySpark, and loads the final data into Snowflake for analytics.

### Project Architecture
<img width="812" height="188" alt="Image" src="https://github.com/user-attachments/assets/ee7007b7-ef30-4a1d-87f4-dbcff11cef9a" />

This ETL project extracts raw supply chain data from AWS S3 using boto3 (AWS SDK for Python), transforms it with PySpark by cleaning the data—ensuring correct data types, handling missing values, standardizing formats, and improving overall data quality—and then applies dimensional modeling to structure the data into a star schema with fact and dimension tables. The final transformed data is loaded into Snowflake for efficient querying and analytics.

### Star Schema
<img width="561" height="543" alt="Image" src="https://github.com/user-attachments/assets/19203aba-0775-4cac-8616-5c7f7ad8ae5a" />
<img width="646" height="634" alt="Image" src="https://github.com/user-attachments/assets/1c216d94-e51d-4c8a-a5aa-6110bbd1b308" />

#### Dimension Tables:
**date_dim**: Stores detailed calendar information including day, month, quarter, and weekend flags to support time-based analysis.

**product_dim**: Contains product-level details such as product ID, name, category, and status for product classification and tracking.

**warehouse_dim**: Holds geographic and location-based information about warehouses involved in order fulfillment.

**customer_dim**: Contains customer-related details like names, IDs, and segment classification to support customer analysis.

**department_dim**: Stores department identifiers and names to organize products and sales by department.

**shipping_destination_dim**: Captures the geographical attributes of shipping destinations including city, state, country, and market.

**delivery_dim**: Includes delivery-related information such as shipping mode, delivery status, and lateness flag for logistics tracking.

#### Fact Tables:

**sales_fact**: Records transactional sales data including quantities, prices, discounts, and profit metrics for each order item.

**shipment_fact**: Tracks shipment performance by recording actual and scheduled shipping durations for each order item.

### Setup
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```




