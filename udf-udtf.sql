-- UDF and UDTF Example in Snowflake SQL
-- This script was created for workshops in relation to Snowflake's Season of Build 2025.

-- Display all existing functions
SHOW FUNCTIONS;

-- Create or replace a simple UDF that rounds a FLOAT to the nearest whole number using Python UDF
CREATE OR REPLACE FUNCTION RoundToWhole(value FLOAT)
    RETURNS NUMBER
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.13'
    HANDLER = 'round_value'
AS $$
def round_value(value):
    if value is None:
        return None
    return round(value, 0)
$$;

-- Show the created python UDF
SHOW FUNCTIONS LIKE 'RoundToWhole';

-- Display the Sales table
SELECT * FROM SALES;

-- Display the Sales table with rounded sales amounts using the RoundToWhole UDF
SELECT 
    PRODUCT_ID,
    DATE,
    REGION,
    UNITS_SOLD,
    RoundToWhole(SALES_AMOUNT) AS rounded_sales_amount
FROM SALES;

-- Create or replace a UDTF that calculates average sales per unit using SQL UDTF
-- SQL UDTFs are generally more efficient
CREATE OR REPLACE FUNCTION AvgSalesPerUnit()
    RETURNS TABLE (
        date DATE,
        region VARCHAR,
        product_id NUMBER,
        units_sold NUMBER,
        sales_amount NUMBER(38,2),
        avg_sales_per_unit NUMBER(38,8)
    )
    LANGUAGE SQL
AS $$
    SELECT 
        DATE,
        REGION,
        PRODUCT_ID,
        UNITS_SOLD,
        SALES_AMOUNT,
        SALES_AMOUNT / UNITS_SOLD AS avg_sales_per_unit
    FROM SALES
    WHERE UNITS_SOLD > 0
$$;

-- Show the created SQL UDTF
SHOW FUNCTIONS LIKE 'AvgSalesPerUnit';

-- Call the UDTF to see average sales per unit (aka the estimated cost as its sales amount divided by units sold for each product)
SELECT * FROM TABLE(AvgSalesPerUnit());

-- Using the UDF and the UDTF, create a view with estimated costs per product
CREATE OR REPLACE VIEW avg_sales_by_product AS
SELECT 
    PRODUCT_ID,
    RoundToWhole(avg_sales_per_unit) AS estimated_cost
FROM TABLE(AvgSalesPerUnit());

-- Creates a new table PRODUCTS_WITH_EST_PRICE that joins the PRODUCTS table with the estimated costs from the view
CREATE OR REPLACE TABLE PRODUCTS_WITH_EST_PRICE AS
SELECT 
    p.*,
    COALESCE(a.estimated_cost, 0) AS estimated_cost
FROM PRODUCTS p
LEFT JOIN avg_sales_by_product a ON p.PRODUCT_ID = a.PRODUCT_ID;

-- Completetion Message
SELECT 'UDF and UDTF creation and usage completed successfully!' AS status;