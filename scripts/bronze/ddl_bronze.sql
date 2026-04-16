/*
=============================================================
Purpose: Initialize Bronze Layer Tables (Raw Data Ingestion)
=============================================================

This script prepares the "bronze" layer of the data warehouse
by creating raw staging tables for CRM and ERP data sources.

Key Actions:
1. Drops existing tables if they already exist 
   → Ensures a clean reload (idempotent execution).

2. Creates new tables with predefined schemas
   → Designed to store raw, untransformed data.

Tables Created:
- bronze.crm_cust_info       : Customer information from CRM
- bronze.crm_prd_info        : Product information from CRM
- bronze.crm_sales_details   : Sales transaction data from CRM

- bronze.erp_cust_az12       : Additional customer attributes (ERP)
- bronze.erp_loc_a101        : Customer location data (ERP)
- bronze.erp_px_cat_g1v2     : Product category hierarchy (ERP)

Notes:
- This layer acts as the foundation of the data pipeline.
- No transformations or cleaning are applied here.
- Data will be processed and refined in higher layers 
  (Silver → Gold).

Usage:
Run this script before loading data (e.g., via BULK INSERT)
to ensure the schema is consistent and ready for ingestion.

=============================================================
*/
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT ,
	cst_key VARCHAR(20),
	cst_firstname NVARCHAR(20),
	cst_lastname NVARCHAR(20),
	cst_marital_status NVARCHAR(20),
	cst_gndr NVARCHAR(20),
	cst_create_date DATETIME
)

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(20),
	prd_nm VARCHAR(20),
	prd_cost FLOAT,
	prd_line VARCHAR(20),
	prd_start_dt DATE,
	prd_end_dt DATE

)
IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num VARCHAR(20),
	sls_prd_key VARCHAR(20),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
)

IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	CID VARCHAR(20),
	BDATE DATE,
	GEN VARCHAR (20)
)
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	CID VARCHAR(20),
	CNTRY VARCHAR(20)
)
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	ID VARCHAR(20),
	CAT VARCHAR (20),
	SUBCAT VARCHAR(20),
	MAINTENANCE VARCHAR(20)
)

