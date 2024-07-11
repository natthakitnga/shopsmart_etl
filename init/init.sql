CREATE DATABASE shopsmart;

\c shopsmart

CREATE TABLE IF NOT EXISTS customer_transaction(
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity FLOAT,
    price FLOAT,
    timestamp timestamp
);

CREATE TABLE product_catalog(
    product_id VARCHAR(50),
    product_name VARCHAR(50),
    category VARCHAR(50),
    price FLOAT
);