/*
=============================================================
Gold Layer - Data Warehouse (Star Schema)

This script creates dimension and fact views for analytical use.

- dim_customers: Customer master data (enriched from CRM + ERP)
- dim_products : Product master data with category mapping
- fact_sales   : Transactional sales fact table

Design:
- Star schema (Fact + Dimensions)
- Surrogate keys generated using ROW_NUMBER()
- Data sourced from Silver layer (cleaned & integrated)

=============================================================
*/
/*
=============================================================
Dimension: Customers

- Combines customer data from CRM and ERP systems
- Uses CRM as the primary source for customer attributes
- Enriches with:
    + Country (location table)
    + Gender fallback (ERP if missing in CRM)
    + Birthdate from ERP

- Generates surrogate key (customer_key) for DW usage
=============================================================
*/
CREATE VIEW gold.dim_customers AS 
SELECT	
	-- Surrogate key for dimension table
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

	-- Business keys
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,

	-- Customer attributes
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_marital_status AS marital_status,

	-- Gender logic: CRM prioritized, fallback to ERP
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.GEN,'n/a')
	END AS gender,

	-- Additional attributes
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci

-- Join ERP customer table for additional attributes
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.CID

-- Join location table for country info
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;
/*
=============================================================
Dimension: Products

- Contains product master data
- Enriched with category & subcategory from ERP mapping
- Filters only active products (prd_end_dt IS NULL)

- Surrogate key generated for DW joins
=============================================================
*/
CREATE VIEW gold.dim_products AS
SELECT 
	-- Surrogate key
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

	-- Business keys
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,

	-- Product attributes
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn

-- Join category mapping table
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id

-- Keep only current/active products
WHERE pn.prd_end_dt IS NULL;
/*
=============================================================
Fact Table: Sales

- Stores transactional sales data
- Links to:
    + dim_customers
    + dim_products

- Contains key business metrics:
    + Sales amount
    + Quantity
    + Price

- Grain: One row per sales transaction line
=============================================================
*/
CREATE VIEW gold.fact_sales AS
SELECT 
	-- Transaction identifiers
	sd.sls_ord_num AS order_num,

	-- Foreign keys (link to dimensions)
	pr.product_key,
	cu.customer_key,

	-- Dates
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,

	-- Measures
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS sales_quantity,
	sd.sls_price AS sales_price

FROM silver.crm_sales_details sd

-- Join product dimension
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number

-- Join customer dimension
LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id;
/*
=============================================================
Final Check Query

- Combines fact table with dimensions
- Used for validation and analytical preview
=============================================================
*/
SELECT * 
FROM gold.fact_sales f

-- Join customer details
LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key 

-- Join product details
LEFT JOIN gold.dim_products p 
	ON p.product_key = f.product_key;
