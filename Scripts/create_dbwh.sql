/*
================================================================================
                        DATA WAREHOUSE INITIALIZATION SCRIPT
================================================================================

PURPOSE:
--------
This script initializes the DataWarehouse database for a data engineering project.
It creates:
- A new DataWarehouse database
- Three-tier data architecture schemas:
  * bronze: Raw/landing data layer (source system replica)
  * silver: Cleaned and transformed data layer 
  * gold: Business-ready aggregated data layer (data mart/analytical layer)

WARNINGS:
---------
⚠️  DESTRUCTIVE OPERATION: This script will DROP the existing DataWarehouse 
    database if it exists, permanently deleting ALL data and objects.

⚠️  BACKUP REQUIRED: Ensure you have backed up any existing DataWarehouse 
    database before running this script.

⚠️  PRODUCTION SAFETY: DO NOT run this script in production environments 
    without proper change management approval.

⚠️  PERMISSIONS: Requires sysadmin or dbcreator privileges to create databases.

USAGE:
------
Run this script using:
- SQL Server Management Studio (SSMS)
- Azure Data Studio  
- Command line: sqlcmd -S ServerName -i create_db.sql

ENVIRONMENT:
------------
Target: SQL Server (2016 or later recommended)
Database: Creates new 'DataWarehouse' database
Schemas: bronze, silver, gold (medalion architecture)

================================================================================
*/


USE master;

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
Create DATABASE DataWarehouse;
GO

Use DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
