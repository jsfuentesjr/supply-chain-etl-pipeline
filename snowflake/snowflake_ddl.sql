CREATE DATABASE IF NOT EXISTS supplychain;

USE DATABASE supplychain;
USE SCHEMA public;

CREATE OR REPLACE TABLE supplychain.public.date_dim (
    date_key INT PRIMARY KEY NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    day INT NOT NULL,
    day_of_week STRING NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE OR REPLACE TABLE supplychain.public.product_dim (
    product_key INT PRIMARY KEY NOT NULL,
    product_id INT NOT NULL,
    product_name STRING NOT NULL,
    category_id INT NOT NULL,
    category_name STRING NOT NULL,
    product_status INT NOT NULL
);

CREATE OR REPLACE TABLE supplychain.public.warehouse_dim (
    warehouse_key INT PRIMARY KEY NOT NULL,
    street_wh STRING NOT NULL,
    city_wh STRING NOT NULL,
    country_wh STRING NOT NULL,
    state_wh STRING NOT NULL,
    zipcode_wh STRING NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL
    
);

CREATE OR REPLACE TABLE supplychain.public.customer_dim (
    customer_key INT PRIMARY KEY NOT NULL,
    customer_id INT NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    full_name STRING NOT NULL,
    customer_segment STRING NOT NULL
);

CREATE OR REPLACE TABLE supplychain.public.department_dim (
    department_key INT PRIMARY KEY NOT NULL,
    department_id INT NOT NULL,
    department_name STRING NOT NULL
);

CREATE OR REPLACE TABLE supplychain.public.shipping_destination_dim (
    shipping_destination_key INT PRIMARY KEY NOT NULL,
    shipping_city STRING NOT NULL,
    shipping_state STRING NOT NULL,
    shipping_country STRING NOT NULL,
    shipping_region STRING NOT NULL,
    shipping_zipcode STRING NOT NULL,
    market STRING NOT NULL
);

CREATE OR REPLACE TABLE supplychain.public.delivery_dim (
    delivery_key INT PRIMARY KEY NOT NULL,
    shipping_mode STRING NOT NULL,
    delivery_status STRING NOT NULL,
    is_late INT NOT NULL
);

CREATE OR REPLACE TABLE supplychain.public.sales_fact (
    order_id INT NOT NULL,
    order_item_id INT NOT NULL,
    order_date_key INT NOT NULL,
    shipping_date_key INT NOT NULL,
    product_key INT NOT NULL,
    warehouse_key INT NOT NULL,
    customer_key INT NOT NULL,
    department_key INT NOT NULL,
    shipping_destination_key INT NOT NULL,
    delivery_key INT NOT NULL,
    payment_type STRING NOT NULL,
    product_price DOUBLE NOT NULL,
    quantity INT NOT NULL,
    discount_rate DOUBLE NOT NULL,
    discount_amount DOUBLE NOT NULL,
    total_amount DOUBLE NOT NULL,
    sales DOUBLE NOT NULL,
    profit_per_order DOUBLE NOT NULL,
    profit_ratio DOUBLE NOT NULL,
    PRIMARY KEY(order_id, order_item_id),
    FOREIGN KEY(order_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY(shipping_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY(product_key) REFERENCES product_dim(product_key),
    FOREIGN KEY(warehouse_key) REFERENCES warehouse_dim(warehouse_key),
    FOREIGN KEY(customer_key) REFERENCES customer_dim(customer_key),
    FOREIGN KEY(department_key) REFERENCES department_dim(department_key),
    FOREIGN KEY(shipping_destination_key) REFERENCES    shipping_destination_dim(shipping_destination_key),
    FOREIGN KEY(delivery_key) REFERENCES delivery_dim(delivery_key)
);

CREATE OR REPLACE TABLE supplychain.public.shipment_fact(
    order_id INT NOT NULL,
    order_item_id INT NOT NULL,
    order_date_key INT NOT NULL,
    shipping_date_key INT NOT NULL,
    product_key INT NOT NULL,
    warehouse_key INT NOT NULL,
    customer_key INT NOT NULL,
    department_key INT NOT NULL,
    shipping_destination_key INT NOT NULL,
    delivery_key INT NOT NULL,
    actual_shipping_days INT NOT NULL,
    scheduled_shipping_days INT NOT NULL,
    PRIMARY KEY(order_id, order_item_id),
    FOREIGN KEY(order_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY(shipping_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY(product_key) REFERENCES product_dim(product_key),
    FOREIGN KEY(warehouse_key) REFERENCES warehouse_dim(warehouse_key),
    FOREIGN KEY(customer_key) REFERENCES customer_dim(customer_key),
    FOREIGN KEY(department_key) REFERENCES department_dim(department_key),
    FOREIGN KEY(shipping_destination_key) REFERENCES shipping_destination_dim(shipping_destination_key),
    FOREIGN KEY(delivery_key) REFERENCES delivery_dim(delivery_key)
);
