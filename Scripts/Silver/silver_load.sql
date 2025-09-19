/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


-- query for data transformation and cleansing
-- fixing the issues with the data

/*
for duplicate rows, to select one row, 
give preference to the latest create date as it 
holds the most fresh information
*/

/*
To get the value with the latest create date, rank
all the duplicate values based on the create date
and only pick the highest one
*/

-- Writing transformations to clean up columnns cst_firstname and cst_lastname 

/*
Since low cardinality, replacing values of cst_gndr with M-Male, F-Female, NULL - n/a
Same done for cst_marital_status, M-Married, S-Single, NULL - n/a
*/

-- Using window function to rank and pick the value

-- Inserting clean data into silver table

-- Created stored procedure for resuability


CREATE OR ALTER PROCEDURE silver.load_silver_layer AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data Into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM
		(
		SELECT *, 
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY  cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t 
		WHERE flag_last = 1 -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



	-- as it holds alot of info, we split the prd_key column into 2
	-- cat_id column is created keeping in mind the category column from our other source system i.e. erp_px_cat_g1v2
	-- replacing '-' of the cat_id column with '_' to mathc with the 'id' column from 'erp_px_cat_g1v2' so that join is possible
	-- replacing NULL in 'prd_cost' with '0'
	-- replacing the abbrevations in the 'prd_line' with words
	-- changed the datatype of 'prd_start_dt and 'prd_end_dt' to DATE cos no time data
	-- set proper end dates for every product using lead
	-- next, modify the ddl script for this table as we added new columns and changed column datatypes
	-- lastly, insert cleaned table data into silver table

	-- Loading silver.crm_prd_info
    SET @start_time = GETDATE();
	PRINT 'Truncating Table: silver.crm_prod_info'
	TRUNCATE TABLE silver.crm_prod_info
	PRINT '>> Inserting Data Into: silver.crm_prod_info'
	INSERT INTO silver.crm_prod_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')	as cat_id, -- Extract category ID (derived columns)
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,		   -- Extract product key
	prd_nm,
	ISNULL(prd_cost, 0) as prd_cost,
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line, -- Map product line codes to descriptive values 
	CAST(prd_start_dt AS DATE) as prd_start_dt,
	CAST(
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
		AS DATE 
	) AS prd_end_dt -- Calculated end date as one day before the next start date (Data enrichments)
	FROM bronze.crm_prod_info;
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';



	-- changing datatype of sls_order_dt from INT to DATE
	-- checked for invalid dates in the 'sls_order_dt', 'sls_ship_dt' and 'sls_due_dt' and changed datatype
	-- made data consistent between the columns 'sls_sales', 'sls_quantity' and 'sls_price'
	-- updating the ddl statements for this table
	-- inserting data into silver

	SET @start_time = GETDATE();
	PRINT 'Truncating Table: silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>> Inserting Data Into: silver.crm_sales_details'
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_date,
		CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END sls_sales, -- Recalculate sale if original value is missing or incorrect
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price -- Derive price if original value is missing or incorrect
		END AS sls_price
	FROM bronze.crm_sales_details
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

	-- WHERE sls_ord_num != TRIM(sls_ord_num)
	-- WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prod_info)
	-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


	PRINT '------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '------------------------------------------------';

	-- removing unwanted letters from the cid, so that its easy to connect to crm_cust_info
	-- Identifying out of range dates
	-- Inserting
	-- Loading silver.erp_cust_az12
	SET @start_time = GETDATE();
	PRINT 'Truncating Table: silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT '>> Inserting Data Into: silver.erp_cust_az12'
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	SELECT 
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix	
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate -- Set future birth dates to NULL
	END as bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'n/a'
	END AS gen -- Normalized gender values and handle unknown cases
	FROM bronze.erp_cust_az12
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

	-- Loading silver.erp_loc_a101
	SET @start_time = GETDATE();
	PRINT 'Truncating Table: silver.erp_loc_a101'
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>> Inserting Data Into: silver.erp_loc_a101'
	INSERT INTO silver.erp_loc_a101 (cid, cntry)
	SELECT 
	REPLACE (cid, '-', '') cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry) -- Normalized and handled missing or blank cntry values 
	END AS cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';


	-- Loading erp_px_cat_g1v2
	SET @start_time = GETDATE();
	PRINT 'Truncating Table: silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
	INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';
	
	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

EXEC silver.load_silver_layer







