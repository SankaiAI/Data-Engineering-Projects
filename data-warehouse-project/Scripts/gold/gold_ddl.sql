/*
================================================================================
                           GOLD LAYER ANALYTICAL VIEWS CREATION SCRIPT
================================================================================

PURPOSE:
--------
This script creates the gold layer analytical views for the Data Warehouse 
medallion architecture. The gold layer serves as the business-ready consumption 
layer with dimensional modeling optimized for analytics, reporting, and business 
intelligence applications.

VIEWS CREATED:
--------------
Dimensional Model Components:
- gold.dim_customers  : Customer dimension with integrated CRM/ERP data
- gold.dim_products   : Product dimension with category hierarchies
- gold.fact_sales     : Sales fact table with foreign key relationships

ANALYTICAL ARCHITECTURE:
------------------------
Implements a star schema design pattern:
- Fact table (fact_sales) at the center containing business metrics
- Dimension tables (dim_customers, dim_products) providing descriptive attributes
- Optimized for OLAP operations and business intelligence tools

DATA INTEGRATION FEATURES:
--------------------------
Customer Dimension (dim_customers):
- Surrogate key generation using ROW_NUMBER()
- Data integration from CRM (primary) and ERP (secondary) sources
- Gender data prioritization (CRM over ERP with fallback logic)
- Complete customer profile with demographics and location data
- Master data management with unified customer view

Product Dimension (dim_products):
- Current products only (filters out historical records via prd_end_dt IS NULL)
- Category hierarchy from ERP product categories
- Product lifecycle management with start dates
- Cost and product line information for analysis
- Surrogate key for dimension management

Sales Fact Table (fact_sales):
- Transactional sales data with foreign key relationships
- Date dimensions for temporal analysis (order, shipping, due dates)
- Quantitative measures (sales amount, quantity, price)
- Referential integrity with dimension tables
- Optimized for aggregation and analytical queries

GOLD LAYER CHARACTERISTICS:
---------------------------
- Business-friendly naming conventions
- Denormalized for query performance
- Dimensional modeling for analytics
- Surrogate keys for slowly changing dimensions
- Integrated data from multiple source systems
- Optimized for business intelligence tools
- Star schema design pattern implementation

WARNINGS:
---------
⚠️  VIEW DEPENDENCIES: These views depend on silver layer tables. Ensure 
    silver layer is populated and transformations are complete.

⚠️  CASCADING DROPS: Views are dropped and recreated. Dependent objects
    (reports, dashboards) may be temporarily unavailable during execution.

⚠️  PERFORMANCE: Views perform joins across multiple silver tables. Monitor
    query performance and consider materialized views for large datasets.

⚠️  SCHEMA DEPENDENCY: Requires gold schema to exist (run create_dbwh.sql first).

⚠️  DATA FRESHNESS: Views show real-time data from silver layer. Ensure
    silver layer ETL processes are current for accurate analytics.

USAGE:
------
Prerequisites: 
1. DataWarehouse database must exist
2. Gold schema must be created
3. Silver layer tables must be populated with transformed data
4. User must have CREATE VIEW permissions in gold schema

Run this script using:
- SQL Server Management Studio (SSMS)
- Azure Data Studio
- Command line: sqlcmd -S ServerName -d DataWarehouse -i gold_ddl.sql

BUSINESS INTELLIGENCE INTEGRATION:
---------------------------------
These views are designed for:
- Power BI / Tableau dashboards
- SQL Server Reporting Services (SSRS)
- Excel pivot tables and analysis
- Custom analytical applications
- Data science and machine learning workflows

MEDALLION ARCHITECTURE FLOW:
----------------------------
Silver Layer → [GOLD LAYER] → Business Intelligence & Analytics
                    ↑ THIS SCRIPT

DIMENSIONAL MODEL:
------------------
          dim_customers
               |
               |
         fact_sales ---- dim_products
               |
         (Date Dimensions)

================================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
