from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame


def clean_data(df_raw: DataFrame) -> DataFrame:

    #Rename the columns in the table
    rename_columns = {
        "Type": "payment_type",
        "Days for shipping (real)": "actual_shipping_days",
        "Days for shipment (scheduled)": "scheduled_shipping_days",
        "Delivery Status": "delivery_status",
        "Late_delivery_risk": "is_late",
        "Category Id": "category_id",
        "Category Name": "category_name",
        "Customer City": "city_wh",
        "Customer Country": "country_wh",
        "Customer Fname": "first_name",
        "Customer Id": "customer_id",
        "Customer Lname": "last_name",
        "Customer Segment": "customer_segment",
        "Customer State": "state_wh",
        "Customer Street": "street_wh",
        "Customer Zipcode": "zipcode_wh",
        "Department Id": "department_id",
        "Department Name": "department_name",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Market": "market",
        "Order City": "shipping_city",
        "Order Country": "shipping_country",
        "order date (DateOrders)": "order_date",
        "Order Id": "order_id",
        "Order Item Discount": "discount_amount",
        "Order Item Discount Rate": "discount_rate",
        "Order Item Id": "order_item_id",
        "Order Item Profit Ratio": "profit_ratio",
        "Order Item Quantity": "quantity",
        "Sales": "sales",
        "Order Item Total": "total_amount",
        "Order Profit Per Order": "profit_per_order",
        "Order Region": "shipping_region",
        "Order State": "shipping_state",
        "Order Zipcode": "shipping_zipcode",
        "Product Card Id": "product_id",
        "Product Name": "product_name",
        "Product Status": "product_status",
        "Product Price": "product_price",
        "shipping date (DateOrders)": "shipping_date",
        "Shipping Mode": "shipping_mode"
    }

    for old_col, new_col in rename_columns.items():
        df_raw = df_raw.withColumnRenamed(old_col, new_col)

    #Drop unnecessary columns
    df_analytics = df_raw.select(
        "order_id", "order_item_id", "order_date", "shipping_date",
        "product_id", "category_id", "product_name", "product_status", "category_name",
        "city_wh", "country_wh", "state_wh", "street_wh", "zipcode_wh", "latitude", "longitude",
        "customer_id", "first_name", "last_name", "customer_segment",
        "department_id", "department_name", 
        "shipping_city", "shipping_country", "shipping_region", "shipping_state", "shipping_zipcode", "market",
        "delivery_status", "is_late", "shipping_mode",
        "actual_shipping_days", "scheduled_shipping_days",
        "product_price", "quantity", "discount_amount", "discount_rate", "total_amount", "sales", "profit_per_order", "profit_ratio", "payment_type"
        
    )

    #remove white spaces
    def trim_string_columns(df):
        for column_name, dtype in df.dtypes:
            if dtype == 'string':
                df = df.withColumn(column_name, F.trim(F.col(column_name)))
        return df

    df_analytics = trim_string_columns(df_analytics)

    #cast columns to their right datatypes
    cast_datatype = {
        "order_date": "timestamp",
        "shipping_date": "timestamp",
        "actual_shipping_days": "int",
        "scheduled_shipping_days": "int",
        "is_late": "int",
        "category_id": "int",
        "product_id": "int",
        "customer_id": "int",
        "department_id": "int",
        "order_id": "int",
        "order_item_id": "int",
        "quantity": "int",
        "product_status": "int",
        "zipcode_wh": "string",
        "shipping_zipcode": "string"
    }

    for column_name, dtype in cast_datatype.items():

        if dtype == "int":
            df_analytics = df_analytics.withColumn(column_name, F.col(column_name).cast(dtype))
        elif dtype == "timestamp":
            df_analytics = df_analytics.withColumn(column_name, F.to_timestamp(F.col(column_name), "MM/dd/yyyy HH:mm")) 
        else:
            df_analytics = df_analytics.withColumn(column_name, F.col(column_name).cast("int").cast(dtype))
    
    
    #Replace "EE. UU." in the country_wh column with "United States"
    df_analytics = df_analytics.replace("EE. UU.", "United States", subset=["country_wh"])

    #fill missing values with the difference between shipping date and order date
    #Check if the actual_shipping_days corresponds to the difference between the two dates
    #Fill missing values in scheduled_shipping_days with 0
    df_analytics = df_analytics.withColumn("actual_shipping_days", F.when(
                            (F.col("actual_shipping_days").isNull()) |
                            (F.col("actual_shipping_days") != F.datediff("shipping_date", "order_date")), 
                                F.datediff("shipping_date", "order_date"))
                                .otherwise(F.col("actual_shipping_days"))) \
                                .fillna({'scheduled_shipping_days': 0})
    
    #Standardize decimal points
    def standardize_decimals(df):
        for column_name, dtype in df.dtypes:
            if dtype == "double":
                df = df.withColumn(column_name, F.round(F.col(column_name), 5))
        return df

    df_analytics = standardize_decimals(df_analytics)
    
    #Drop duplicates
    df_analytics = df_analytics.dropDuplicates()

    #Handle missing values for string and timestamp datatypes columns
    def handle_missing_values(df):
        for column_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(
                    column_name,
                    F.when((F.col(column_name).isNull()) |
                        (F.col(column_name) == "") |
                        (F.col(column_name) == "null") |
                        (F.col(column_name) == "NaN"),
                        "-1"
                        ).otherwise(F.col(column_name))
                )
            elif dtype == "timestamp":
                dummy_timestamp = F.lit("1900-01-01 00:00:00").cast(T.TimestampType())
                df = df.withColumn(
                    column_name,
                    F.when(F.col(column_name).isNull(), dummy_timestamp).otherwise(F.col(column_name))
                )
        return df

    df_analytics = handle_missing_values(df_analytics)

    #impute missing values with derived value from other columns or fill with 0
    #Compute price if quantity and sales are available, else fill with 0 if either is missing. Otherwise, keep the original.
    df_analytics = df_analytics.withColumn(
                    "product_price", 
                    F.when(
                        (F.col("product_price").isNull()) & 
                        (F.col("quantity").isNotNull()) & 
                        (F.col("sales").isNotNull()), 
                        F.round(F.col("sales") / F.col("quantity"), 5)
                    ).when(
                        (F.col("product_price").isNull()) &
                        ((F.col("quantity").isNull()) | (F.col("sales").isNull())),
                        F.lit(0)
                    ).otherwise(F.col("product_price"))
    )

    #Compute quantity if sales and price are available, else fill with 0 if either is missing. Otherwise, keep the original.
    df_analytics = df_analytics.withColumn(
                    "quantity",
                    F.when(
                        (F.col("quantity").isNull()) &
                        (F.col("product_price").isNotNull()) &
                        (F.col("sales").isNotNull()),
                        (F.col("sales") / F.col("product_price")).cast("int")
                    ).when(
                        (F.col("quantity").isNull()) &
                        ((F.col("product_price").isNull()) | (F.col("sales").isNull())),
                        F.lit(0)
                    ).otherwise(F.col("quantity"))
    )
                        
                        
    #Compute sales if quantity and price are available, else fill with 0 if either is missing. Otherwise, keep the original.
    df_analytics = df_analytics.withColumn(
                    "sales",
                    F.when(
                        (F.col("sales").isNull()) &
                        (F.col("quantity").isNotNull()) &
                        (F.col("product_price").isNotNull()),
                        F.round(F.col("quantity") * F.col("product_price"), 5)
                    ).when(
                        (F.col("sales").isNull()) &
                        ((F.col("quantity").isNull()) | (F.col("product_price").isNull())),
                        F.lit(0)
                    ).otherwise(F.col("sales"))
    )

    #Compute discount amount if price and discount rate are available, else fill with 0 if either is missing. Otherwise, keep the original.
    df_analytics = df_analytics.withColumn(
                    "discount_amount",
                    F.when(
                        (F.col("discount_amount").isNull()) &
                        (F.col("discount_rate").isNotNull()) &
                        (F.col("product_price").isNotNull()),
                        F.round(F.col("discount_rate") * F.col("product_price"), 5)
                    ).when(
                        (F.col("discount_amount").isNull()) &
                        ((F.col("discount_rate").isNull()) | (F.col("product_price").isNull())),
                        F.lit(0)
                    ).otherwise(F.col("discount_amount"))
    )

    #Compute discount rate if discount amount and price are available, else fill with 0 if either is missing. Otherwise, keep the original.
    df_analytics = df_analytics.withColumn(
                    "discount_rate",
                    F.when(
                        (F.col("discount_rate").isNull()) &
                        (F.col("discount_amount").isNotNull()) &
                        (F.col("product_price").isNotNull()),
                        F.round(F.col("discount_amount") / F.col("product_price"), 5)
                    ).when(
                        (F.col("discount_rate").isNull()) &
                        ((F.col("discount_amount").isNull()) | (F.col("product_price").isNull())),
                        F.lit(0)
                    ).otherwise(F.col("discount_rate"))
    )

    #Compute total amount if discount amount and price are available, else fill with 0 if either is missing. Otherwise, keep the original.
    df_analytics = df_analytics.withColumn(
                    "total_amount",
                    F.when(
                        (F.col("total_amount").isNull()) &
                        (F.col("discount_amount").isNotNull()) &
                        (F.col("product_price").isNotNull()),
                        F.round(F.col("product_price") - F.col("discount_amount"), 5)
                    ).when(
                        (F.col("total_amount").isNull()) &
                        ((F.col("discount_amount").isNull()) | (F.col("product_price").isNull())),
                        F.lit(0)
                    ).otherwise(F.col("total_amount"))
    )

    #Impute missing values in the profit per order and profit ration with 0
    df_analytics = df_analytics.fillna({'profit_per_order': 0, 'profit_ratio':0})

    return df_analytics
