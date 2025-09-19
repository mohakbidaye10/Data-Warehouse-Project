/*
Creating DDL for the silver layer
*/


-- Creating tables for 'source_crm'

--  Table for cust_info csv file

-- If the table silver.crm_cust_info exists, drop it before recreating
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info; 

CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Adding a column because i left it out accidently
ALTER TABLE silver.crm_cust_info
ADD cst_create_date DATE;


-- Table for prd_info csv file

-- If the table silver.crm_prod_info exists, drop it before recreating
IF OBJECT_ID('silver.crm_prod_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prod_info; 

-- modified this statement bcos new columns and changed datatype during cleaning process
CREATE TABLE silver.crm_prod_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Table for sales_details csv file

-- If the table silver.crm_sales_details exists, drop it before recreating

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details; 

CREATE TABLE silver.crm_sales_details(
	sls_ord_num	NVARCHAR(50),
	sls_prd_key	NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt DATE, 
	sls_ship_dt	DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Creating tables for 'source_erp'

-- Table for cust_az12 csv file

-- If the table silver.erp_cust_az12 exists, drop it before recreating

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Table for loc_a101 csv file

-- If the table silver.erp_loc_a101 exists, drop it before recreating

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);

-- Table for px_cat_g1v2 csv file

-- If the table silver.erp_px_cat_g1v2 exists, drop it before recreating

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME DEFAULT GETDATE()
);
