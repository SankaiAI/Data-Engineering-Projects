/*
================================================================================
                           SILVER LAYER TABLE CREATION SCRIPT
================================================================================

PURPOSE:
--------
This script creates the silver layer tables for the Data Warehouse medallion 
architecture. The silver layer serves as the cleaned and standardized data zone 
where data from the bronze layer is transformed, validated, and prepared for 
business consumption in the gold layer.

TABLES CREATED:
---------------
CRM System Tables (Cleaned & Standardized):
- silver.crm_cust_info     : Cleaned customer information with standardized formats
- silver.crm_prd_info      : Validated product information with category mappings
- silver.crm_sales_details : Normalized sales transactions with proper date formats

ERP System Tables (Cleaned & Standardized):
- silver.erp_loc_a101      : Standardized location and country data
- silver.erp_cust_az12     : Cleaned customer demographics with date validation
- silver.erp_px_cat_g1v2   : Standardized product category hierarchy

DATA TRANSFORMATIONS:
--------------------
Key improvements over bronze layer:
- Data type standardization (proper DATE formats vs INT)
- Data quality validation and cleansing
- Consistent naming conventions
- Added audit trail columns (dwh_create_date)
- Referential integrity preparation
- Business rule applications

SILVER LAYER CHARACTERISTICS:
-----------------------------
- Cleaned and validated data
- Standardized data types and formats
- Business rules applied
- Data quality checks implemented
- Audit columns for tracking
- Prepared for analytical consumption
- Foundation for gold layer aggregations

WARNINGS:
---------
⚠️  DESTRUCTIVE OPERATION: This script will DROP existing silver tables 
    if they exist, permanently deleting ALL data and transformations.

⚠️  DATA DEPENDENCIES: Silver tables depend on bronze layer data. Ensure 
    bronze layer is populated before running silver ETL processes.

⚠️  BUSINESS RULES: Data transformations follow specific business logic. 
    Changes to this schema may impact downstream gold layer processes.

⚠️  SCHEMA DEPENDENCY: Requires silver schema to exist (run create_dbwh.sql first).

USAGE:
------
Prerequisites: 
1. DataWarehouse database must exist
2. Silver schema must be created
3. Bronze layer tables should be populated
4. User must have CREATE TABLE permissions in silver schema

Run this script using:
- SQL Server Management Studio (SSMS)
- Azure Data Studio
- Command line: sqlcmd -S ServerName -d DataWarehouse -i silver_ddl.sql

MEDALLION ARCHITECTURE FLOW:
----------------------------
Bronze Layer → [SILVER LAYER] → Gold Layer
                    ↑ THIS SCRIPT

ETL FLOW:
---------
1. Bronze tables (raw data)
2. Silver DDL (this script - table structures)
3. Silver ETL processes (data transformation)
4. Gold layer consumption

================================================================================
*/

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),
    cntry           NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

