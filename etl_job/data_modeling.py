from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W
from pyspark.sql import DataFrame


def model_data(df_analytics: DataFrame, spark) -> dict:

    #Create date dimension:
    date_range = [("2010-01-01", "2025-12-31")]
    date_columns = ["start_date", "end_date"]

    df_date = spark.createDataFrame(date_range, date_columns) \
                .withColumn("start_date", F.to_date("start_date")) \
                .withColumn("end_date", F.to_date("end_date")) \
                .withColumn("date", F.explode(F.sequence("start_date", "end_date")))

    date_dim = df_date.select("date") \
                .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")) \
                .withColumn("year", F.year("date")) \
                .withColumn("month", F.month("date")) \
                .withColumn("quarter", F.quarter("date")) \
                .withColumn("day", F.dayofmonth("date")) \
                .withColumn("day_of_week", F.date_format("date", "E")) \
                .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1,7), True).otherwise(False)) \
                .select("date_key", "date", "year", "month", "quarter", "day", "day_of_week", "is_weekend")
    
    #Create product dimension:
    product_dim = df_analytics \
        .select("product_id", "product_name", "category_id", "category_name", "product_status") \
        .distinct() \
        .withColumn("product_key", F.row_number().over(W.orderBy(F.col("product_id").asc()))) \
        .select("product_key", "product_id", "product_name", "category_id", "category_name", "product_status")
    
    #Create warehouse dimension:
    warehouse_dim = df_analytics \
        .select("city_wh", "country_wh", "state_wh", "street_wh", "zipcode_wh", "latitude", "longitude") \
        .distinct() \
        .withColumn("warehouse_key", F.row_number().over(W.orderBy(F.col("city_wh").asc()))) \
        .select("warehouse_key", "street_wh", "city_wh", "country_wh", "state_wh", "zipcode_wh", "latitude", "longitude")
    
    #Create customer dimension:
    customer_dim = df_analytics \
        .select("customer_id", "first_name", "last_name", "customer_segment") \
        .distinct() \
        .withColumn("customer_key", F.row_number().over(W.orderBy(F.col("customer_id").asc()))) \
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))) \
        .select("customer_key", "customer_id", "first_name", "last_name", "full_name", "customer_segment")
    
    #Create department dimension:
    department_dim = df_analytics \
        .select("department_id", "department_name") \
        .distinct() \
        .withColumn("department_key", F.row_number().over(W.orderBy(F.col("department_id").asc()))) \
        .select("department_key", "department_id", "department_name")
    
    #Create shipping destination dimension
    shipping_destination_dim = df_analytics \
        .select("shipping_city", "shipping_country", "shipping_region", "shipping_state", "shipping_zipcode", "market") \
        .distinct() \
        .withColumn("shipping_destination_key", F.row_number().over(W.orderBy(F.col("shipping_city").asc()))) \
        .select("shipping_destination_key", "shipping_city", "shipping_state", "shipping_country", "shipping_region", "shipping_zipcode", "market")
    
    #Create delivery dimension:
    delivery_dim = df_analytics \
        .select("delivery_status", "is_late", "shipping_mode") \
        .distinct() \
        .withColumn("delivery_key", F.row_number().over(W.orderBy(F.col("shipping_mode").asc()))) \
        .select("delivery_key", "shipping_mode", "delivery_status", "is_late")
    
    #Convert order date and shipping date to date datatype
    df_analytics = df_analytics.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd")) \
                    .withColumn("shipping_date", F.to_date("shipping_date", "yyyy-MM-dd"))
    

    #Join the dimensions to the main table
    df_enriched = df_analytics \
        .join(date_dim.alias("order_dt"), F.col("order_date") == F.col("order_dt.date"), "left") \
        .withColumnRenamed("date_key", "order_date_key") \
        .join(date_dim.alias("shipping_dt"), F.col("shipping_date") == F.col("shipping_dt.date"), "left") \
        .withColumnRenamed("date_key", "shipping_date_key") \
        .join(product_dim, [
            df_analytics["product_id"] == product_dim["product_id"],
            df_analytics["product_name"] == product_dim["product_name"],
            df_analytics["category_id"] == product_dim["category_id"],
            df_analytics["category_name"] == product_dim["category_name"],
            df_analytics["product_status"] == product_dim["product_status"]
        ], "left") \
        .join(warehouse_dim, [
            df_analytics["city_wh"] == warehouse_dim["city_wh"],
            df_analytics["country_wh"] == warehouse_dim["country_wh"],
            df_analytics["state_wh"] == warehouse_dim["state_wh"],
            df_analytics["street_wh"] == warehouse_dim["street_wh"],
            df_analytics["zipcode_wh"] == warehouse_dim["zipcode_wh"],
            df_analytics["latitude"] == warehouse_dim["latitude"],
            df_analytics["longitude"] == warehouse_dim["longitude"]
        ], "left") \
        .join(customer_dim, [
            df_analytics["customer_id"] == customer_dim["customer_id"],
            df_analytics["first_name"] == customer_dim["first_name"],
            df_analytics["last_name"] == customer_dim["last_name"],
            df_analytics["customer_segment"] == customer_dim["customer_segment"]
        ], "left") \
        .join(department_dim, [
            df_analytics["department_id"] == department_dim["department_id"],
            df_analytics["department_name"] == department_dim["department_name"]
        ], "left") \
        .join(shipping_destination_dim, [
            df_analytics["shipping_city"] == shipping_destination_dim["shipping_city"],
            df_analytics["shipping_country"] == shipping_destination_dim["shipping_country"],
            df_analytics["shipping_region"] == shipping_destination_dim["shipping_region"],
            df_analytics["shipping_state"] == shipping_destination_dim["shipping_state"],
            df_analytics["shipping_zipcode"] == shipping_destination_dim["shipping_zipcode"],
            df_analytics["market"] == shipping_destination_dim["market"]
        ], "left") \
        .join(delivery_dim, [
            df_analytics["delivery_status"] == delivery_dim["delivery_status"],
            df_analytics["is_late"] == delivery_dim["is_late"],
            df_analytics["shipping_mode"] == delivery_dim["shipping_mode"]
        ], "left")
    
    #Create sales fact table
    sales_fact = df_enriched \
        .select("order_id", "order_item_id", "order_date_key", "shipping_date_key",
                "product_key", "warehouse_key", "customer_key", "department_key",
                "shipping_destination_key", "delivery_key", "payment_type", "product_price", 
                "quantity", "discount_rate", "discount_amount", "total_amount", "sales",
                "profit_per_order", "profit_ratio")

    #Create shipment fact table
    shipment_fact = df_enriched \
        .select("order_id", "order_item_id", "order_date_key", "shipping_date_key",
                "product_key", "warehouse_key", "customer_key", "department_key",
                "shipping_destination_key", "delivery_key", 
                "actual_shipping_days", "scheduled_shipping_days")
    
    return {
        "date_dim": date_dim,
        "product_dim": product_dim,
        "warehouse_dim": warehouse_dim,
        "customer_dim": customer_dim,
        "department_dim": department_dim,
        "shipping_destination_dim": shipping_destination_dim,
        "delivery_dim": delivery_dim,
        "sales_fact": sales_fact,
        "shipment_fact": shipment_fact,
    }
