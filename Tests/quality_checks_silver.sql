/*
Quality Check of silver table after inserting clean data
Includes checks for:
- Null or duplicate primary keys
- Unwanted spaces in string fields
- Data standardization and consistency
- Invalid date ranges and orders
- Data consistency between related fields
*/

-- =======================================================
-- CHECKING 'crm_cust_info table'
-- =======================================================

-- Check for nulls or duplicates
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
GO

SELECT * 
FROM silver.crm_cust_info
WHERE cst_id = 29483


-- Check for unwanted spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


-- Data standardization and consistency
-- Checking cardinality
SELECT DISTINCT(cst_gndr) 
FROM silver.crm_cust_info

SELECT DISTINCT(cst_marital_status) 
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

-- =============================================
-- CHECKING crm_prod_info
-- =============================================

-- Checking for duplicates and nulls 
SELECT prd_id, COUNT(*)
FROM silver.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
GO
--no nulls or duplicates

-- Checking for unwanted spaces
SELECT prd_nm
FROM silver.crm_prod_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLS or negative numbers
SELECT prd_cost
FROM silver.crm_prod_info
WHERE prd_cost < 0 or prd_cost IS NULL	

-- Data standardization and consistency
-- checking cardinality
SELECT DISTINCT(prd_line) 
FROM silver.crm_prod_info

-- Check for invalid date orders
-- end date mmust not be earlier than start date
SELECT * 
FROM silver.crm_prod_info
WHERE prd_end_dt < prd_start_dt

SELECT * FROM silver.crm_prod_info


-- =============================================
-- CHECKING crm_sales_details
-- =============================================

-- Check for invalid dates

SELECT 
NULLIF(sls_order_dt, 0)	sls_order_dt
FROM silver.crm_sales_details
-- length of the date must be 8 
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101 

-- Check for invalid date orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
	
-- Check Data Consistency: Between Sales, Quantity and Price
-- Sales = Quantity * Price
-- Values must not be Null, zero or negative

SELECT DISTINCT
sls_sales, 
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

/*

----CALCULATION-----

-- fixing the columns based on rules
/* Rules : 1. If sales is negative,zero or null, derive using Quantity and Price
		   2. If Price is zero or null, calculate it using Sales and Quantity
		   3. If Price is negative, convert it to positive
*/
SELECT DISTINCT
sls_sales as old_sales,
sls_quantity,
sls_price as old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY 1,2,3
*/

SELECT * FROM silver.crm_sales_details

-- =============================================
-- CHECKING erp_cust_az12
-- =============================================

-- Identifying invalid dates
-- out of range dates
SELECT *
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data standardization and consistency
SELECT DISTINCT	gen
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12

-- =============================================
-- CHECKING erp_loc_a101
-- =============================================

-- Data standardization and consistency
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101

SELECT * FROM silver.erp_loc_a101

SELECT * FROM silver.erp_px_cat_g1v2
