-- DuckSync Test Environment - Test Data Setup
-- Run this after setup_snowflake_permissions.sql
-- This script is idempotent - safe to run multiple times
--
-- Creates minimal test tables with sample data for integration testing.
--
-- BEFORE RUNNING: Replace placeholders or use envsubst:
--   source .env && envsubst < scripts/setup_test_snowflake.sql

USE DATABASE ${SNOWFLAKE_DATABASE};
USE SCHEMA ${SNOWFLAKE_SCHEMA};

-- ============================================================================
-- 1. CUSTOMERS Table
-- ============================================================================
CREATE OR REPLACE TABLE CUSTOMERS (
    id INTEGER,
    name VARCHAR(100),
    email VARCHAR(200),
    region VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO CUSTOMERS (id, name, email, region) VALUES
    (1, 'Acme Corp', 'contact@acme.com', 'US'),
    (2, 'Globex Inc', 'info@globex.com', 'EU'),
    (3, 'Initech', 'hello@initech.com', 'US'),
    (4, 'Umbrella Corp', 'sales@umbrella.com', 'APAC'),
    (5, 'Stark Industries', 'tony@stark.com', 'US'),
    (6, 'Wayne Enterprises', 'bruce@wayne.com', 'US'),
    (7, 'Cyberdyne Systems', 'info@cyberdyne.com', 'EU'),
    (8, 'Weyland-Yutani', 'contact@weyland.com', 'APAC'),
    (9, 'Soylent Corp', 'green@soylent.com', 'US'),
    (10, 'Tyrell Corp', 'replicants@tyrell.com', 'EU');

-- ============================================================================
-- 2. PRODUCTS Table
-- ============================================================================
CREATE OR REPLACE TABLE PRODUCTS (
    id INTEGER,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    in_stock BOOLEAN DEFAULT TRUE
);

INSERT INTO PRODUCTS (id, name, category, price, in_stock) VALUES
    (1, 'Widget A', 'Hardware', 29.99, TRUE),
    (2, 'Widget B', 'Hardware', 49.99, TRUE),
    (3, 'Gadget X', 'Electronics', 199.99, TRUE),
    (4, 'Gadget Y', 'Electronics', 299.99, FALSE),
    (5, 'Service Plan', 'Services', 99.99, TRUE);

-- ============================================================================
-- 3. ORDERS Table
-- ============================================================================
CREATE OR REPLACE TABLE ORDERS (
    id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    total_amount DECIMAL(10, 2),
    order_date DATE,
    status VARCHAR(20)
);

INSERT INTO ORDERS (id, customer_id, product_id, quantity, total_amount, order_date, status) VALUES
    (1, 1, 1, 10, 299.90, '2024-01-15', 'completed'),
    (2, 1, 3, 2, 399.98, '2024-01-20', 'completed'),
    (3, 2, 2, 5, 249.95, '2024-02-01', 'completed'),
    (4, 3, 1, 20, 599.80, '2024-02-10', 'shipped'),
    (5, 4, 4, 1, 299.99, '2024-02-15', 'pending'),
    (6, 5, 5, 3, 299.97, '2024-02-20', 'completed'),
    (7, 2, 1, 15, 449.85, '2024-03-01', 'completed'),
    (8, 6, 3, 1, 199.99, '2024-03-05', 'shipped'),
    (9, 7, 2, 8, 399.92, '2024-03-10', 'pending'),
    (10, 8, 5, 2, 199.98, '2024-03-15', 'completed');

-- ============================================================================
-- 4. Verification
-- ============================================================================
SELECT 'CUSTOMERS' as table_name, COUNT(*) as row_count FROM CUSTOMERS
UNION ALL
SELECT 'PRODUCTS', COUNT(*) FROM PRODUCTS
UNION ALL
SELECT 'ORDERS', COUNT(*) FROM ORDERS;

-- Show table metadata (used by DuckSync for smart refresh)
SELECT 
    table_name,
    last_altered,
    row_count
FROM information_schema.tables 
WHERE table_schema = '${SNOWFLAKE_SCHEMA}'
ORDER BY table_name;
