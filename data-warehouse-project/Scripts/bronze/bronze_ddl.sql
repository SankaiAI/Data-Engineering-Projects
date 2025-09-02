/*
================================================================================
                           BRONZE LAYER TABLE CREATION SCRIPT
================================================================================

PURPOSE:
--------
This script creates the bronze layer tables for the Data Warehouse medallion 
architecture. The bronze layer serves as the raw data landing zone where data 
from source systems is stored in its original format with minimal transformation.

TABLES CREATED:
---------------
CRM System Tables:
- bronze.crm_cust_info     : Customer information from CRM
- bronze.crm_prd_info      : Product information from CRM  
- bronze.crm_sales_details : Sales transaction details from CRM

ERP System Tables:
- bronze.erp_loc_a101      : Location data from ERP system
- bronze.erp_cust_az12     : Customer demographics from ERP
- bronze.erp_px_cat_g1v2   : Product category information from ERP

DATA CHARACTERISTICS:
--------------------
- Raw data format (minimal data type constraints)
- Source system field names preserved
- No business logic applied
- Staging area for further processing into silver layer

WARNINGS:
---------
⚠️  DESTRUCTIVE OPERATION: This script will DROP existing bronze tables 
    if they exist, permanently deleting ALL data.

⚠️  DATA LOSS: Ensure proper backup procedures are in place before execution.

⚠️  DEPENDENCIES: Requires bronze schema to exist (run create_dbwh.sql first).

⚠️  SOURCE DATA: This script only creates table structure - data loading 
    processes are handled separately.

USAGE:
------
Prerequisites: 
1. DataWarehouse database must exist
2. Bronze schema must be created
3. User must have CREATE TABLE permissions

Run this script using:
- SQL Server Management Studio (SSMS)
- Azure Data Studio
- Command line: sqlcmd -S ServerName -d DataWarehouse -i bronze.sql

MEDALLION ARCHITECTURE FLOW:
----------------------------
Source Systems → [BRONZE LAYER] → Silver Layer → Gold Layer
                     ↑ THIS SCRIPT

================================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO
