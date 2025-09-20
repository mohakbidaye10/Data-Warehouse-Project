/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- Use only silver layer

-- ============================================================
-- Creating Dimension Customers: gold.dim_customers
-- ============================================================

-- Checking duplicates
-- SELECT cst_id, COUNT(*) FROM 
-- ()t
-- GROUP BY cst_id 
-- HAVING COUNT(*) > 1

-- Renaming columns
-- Creating customer object , i.e. VIEW

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is master for gender info
		ELSE COALESCE(ca.gen, 'n/a')			
		END AS gender,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci	
LEFT JOIN silver.erp_cust_az12 ca -- joining table erp_cust_az12
ON		ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la -- joining erp_loc_a101
ON		ci.cst_key = la.cid 

GO;

-- ============================================================
-- Creating Dimension Product: gold.dim_products
-- ============================================================
-- Dimension because each row is describing an object or product

-- Current data is data that has no end date
-- Since the main/master source system is CRM, we use LEFT JOIN so that we dont miss out on any data

-- Checking duplicates
/*
SELECT prd_key, COUNT(*) FROM (
)t 
GROUP BY prd_key
HAVING COUNT(*) > 1
*/

-- Renamed columns with friendlier names
-- Creating surrogate key
-- Created product object using VIEW

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- surrogate key
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pn.prd_cost AS product_cost, 
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out historical data, keeping only current data

GO;


-- ============================================================
-- Creating Fact Sales: gold.fact_sales
-- ============================================================

-- This is a fact table
-- It connects multiple dimensions hences its a fact

-- To connect the facts with dimensions, use the dimensions 
-- surrogate keys instead of original IDs
-- This is done by joining the dimension tables which contain the surrogate keys
-- Created sales fact object using VIEW


CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number



