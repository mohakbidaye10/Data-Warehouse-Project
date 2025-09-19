/*
exploring quality issues in the table data 
using bronze layer

go through all the tables of the bronze layer,
clean up data n then insert to silver layer
*/
-- =============================================
-- CHECKING crm_cust_info
-- =============================================
-- Check for nulls or duplicates in primary key

SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
GO

/*
for duplicate rows, to select one row, 
give preference to the latest create date as it 
holds the most fresh information
*/
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_id = 29483


-- check for unwanted spaces
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
-- if orignal value is not equal to the same value
-- after trimming, it means there are spaces
-- result is a list of all first names with spaces	
-- this check is done for cst_lastname and cst_gndr too
-- columns cst_firstname and cst_lastname have values with spaces

-- Data standardization and consistency
-- checking cardinality
SELECT DISTINCT(cst_gndr) 
FROM bronze.crm_cust_info

SELECT DISTINCT(cst_marital_status) 
FROM bronze.crm_cust_info

-- =============================================
-- CHECKING crm_prod_info
-- =============================================

-- Checking for duplicates and nulls 
SELECT prd_id, COUNT(*)
FROM bronze.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
GO
--no nulls or duplicates

-- Checking for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prod_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLS or negative numbers
SELECT prd_cost
FROM bronze.crm_prod_info
WHERE prd_cost < 0 or prd_cost IS NULL	

-- Data standardization and consistency
-- checking cardinality
SELECT DISTINCT(prd_line) 
FROM bronze.crm_prod_info

-- Check for invalid date orders
-- end date mmust not be earlier than start date
SELECT * 
FROM bronze.crm_prod_info
WHERE prd_end_dt < prd_start_dt


-- =============================================
-- CHECKING crm_sales_details
-- =============================================

-- Check for invalid dates

SELECT 
NULLIF(sls_order_dt, 0)	sls_order_dt
FROM bronze.crm_sales_details
-- length of the date must be 8 
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101 

-- Check for invalid date orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
	
-- Check Data Consistency: Between Sales, Quantity and Price
-- Sales = Quantity * Price
-- Values must not be Null, zero or negative

SELECT DISTINCT
sls_sales, 
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

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


-- =============================================
-- CHECKING erp_cust_az12
-- =============================================

-- Identifying invalid dates
-- out of range dates
SELECT *
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data standardization and consistency
SELECT DISTINCT	gen
FROM bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12


-- Data standardization and consistency
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101

SELECT 
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101


-- =============================================
-- CHECKING erp_px_cat_g1v2
-- =============================================

-- Checking unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data standardization and consistency
SELECT DISTINCT 
cat
FROM bronze.erp_px_cat_g1v2















