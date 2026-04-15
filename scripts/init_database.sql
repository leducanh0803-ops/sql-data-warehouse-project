/* 
===========================================================
CREATE DATABASE AND SCHEMA
===========================================================

This script:
1. Checks if the DATAWAREHOUSE database already exists
2. If it exists → forces disconnect and deletes it
3. Creates a fresh database
4. Creates 3 schemas (bronze, silver, gold) following
   the Medallion Architecture (data layering approach)
===========================================================
*/

-- Switch context to the system database
-- Required because we cannot drop a database while using it
USE master;
GO

-- Check if the database 'DATAWAREHOUSE' already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DATAWAREHOUSE')
BEGIN 
    -- Force all active connections to close
    -- SINGLE_USER mode ensures only one connection is allowed
    -- ROLLBACK IMMEDIATE cancels any running transactions
    ALTER DATABASE DATAWAREHOUSE 
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Delete the existing database
    DROP DATABASE DATAWAREHOUSE;
END;
GO

-- Create a new clean database
CREATE DATABASE DATAWAREHOUSE;
GO

-- Switch to the newly created database
USE DATAWAREHOUSE;
GO

/* 
===========================================================
SCHEMA CREATION (Medallion Architecture)
===========================================================

We create 3 schemas to organize data into layers:

1. BRONZE  → Raw data (unchanged, ingested from source)
2. SILVER  → Cleaned and transformed data
3. GOLD    → Business-ready data for analytics/reporting
===========================================================
*/

-- Bronze layer: stores raw, unprocessed data
CREATE SCHEMA bronze;
GO

-- Silver layer: stores cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Gold layer: stores aggregated, business-level data
CREATE SCHEMA gold;
GO
