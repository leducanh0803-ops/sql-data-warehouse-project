/*
=====================================================================
Procedure: bronze.load_bronze
=====================================================================

Purpose:
This stored procedure loads raw data into the Bronze layer of the 
data warehouse from external CSV files.

Overview:
The Bronze layer represents the raw ingestion stage where data is
loaded exactly as received from source systems (CRM & ERP) without
any transformation.

Key Features:

1. Full Reload Strategy
   - Uses TRUNCATE TABLE before each load
   → Ensures old data is removed and replaced with fresh data

2. Bulk Data Ingestion
   - Uses BULK INSERT to efficiently load large CSV files
   → Faster than row-by-row inserts

3. Multi-Source Integration
   Loads data from:
   - CRM system:
     + Customer info
     + Product info
     + Sales details
   - ERP system:
     + Customer attributes
     + Location data
     + Product categories

4. Performance Tracking
   - Captures start and end time for each table load
   - Prints duration (in seconds)
   → Helps monitor ETL performance

5. Batch Monitoring
   - Tracks total execution time of the entire procedure

6. Error Handling (TRY...CATCH)
   - Captures and prints:
     + Error message
     + Error number
   → Helps debugging when load fails

7. File Configuration
   - Skips header row (FIRSTROW = 2)
   - Uses comma-separated format (FIELDTERMINATOR = ',')
   - Uses newline as row delimiter (ROWTERMINATOR = '\n')
   - TABLOCK improves bulk insert performance

Notes:
- This procedure should be run before any transformation step 
  (Silver/Gold layers).
- File paths must be accessible by SQL Server.
- Ensure proper permissions for BULK INSERT.

=====================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
       SET @batch_start_time =GETDATE()
       PRINT '===================================='
       PRINT 'Loading Bronze Layer'
        -- CRM Customer Info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\leduc\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

        -- CRM Product Info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\leduc\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

        -- CRM Sales Details
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\leduc\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

        -- ERP Customer
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\leduc\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
        -- ERP Location
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\leduc\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

        -- ERP Product Category
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\leduc\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
         );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration:' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'

        SET @batch_end_time = GETDATE();
        PRINT '==============================================='
        PRINT 'Loading Bronze Layer Is Completed'
        PRINT '>> Total Batch Load Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
        PRINT '==============================================='
    END TRY

    BEGIN CATCH
        PRINT '==============================='
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT  'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
        PRINT '==============================='
    END CATCH

END;
