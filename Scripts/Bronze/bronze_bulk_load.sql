/*
Inserted data from the 2 source systems CRM and ERP
Carried out bulk insert
Full load was done i.e. emptying the table and then loading from scratch
Created stored procedure for resuability
Used try and catch to deal with errors
Declared variables to store date/time values to measure load duration
Found load duration per table and of the whole batch too
*/

-- Stored procedure for resuability
CREATE or ALTER PROCEDURE bronze.load_bronze_layer AS
BEGIN
	-- declared variables to get the load duration
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- try block
	BEGIN TRY
		-- finding duration of the whole batch
		SET @batch_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';
		
		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE()	;
		PRINT 'Truncating table bronze.crm_cust_info'

		-- Emptying the table and then loading from scratch (full load)
		TRUNCATE TABLE bronze.crm_cust_info;

		-- Inserting data from the source system 'SOURCE_CRM'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Mohak\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.crm_prod_info'
		
		-- Full load
		TRUNCATE TABLE bronze.crm_prod_info;

		-- inserting data from prod_info
		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Users\Mohak\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE()	;

		PRINT 'Truncating table bronze.crm_sales_details'
		-- Full load
		TRUNCATE TABLE bronze.crm_sales_details;

		-- Inserting data from sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Mohak\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		-- Inserting data from the source system 'SOURCE_ERP'
		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables'
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.erp_cust_az12';
		-- Full load
		TRUNCATE TABLE bronze.erp_cust_az12;

		-- Inserting data from cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Mohak\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.erp_loc_a101'
		-- Full load
		TRUNCATE TABLE bronze.erp_loc_a101;

		-- Inserting data from loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Mohak\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating table bronze.erp_px_cat_g1v2'
		-- Full load
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		-- Inserting data from px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Mohak\OneDrive\Desktop\Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		
		SET @batch_end_time = GETDATE();
		PRINT '===============================================' 
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===============================================';

	END TRY
	-- catch block
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '================================================='
	END CATCH
END;


-- executing the stored procedure
EXEC bronze.load_bronze_layer;
