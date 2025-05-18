/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	BEGIN TRY
		
		DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;

		SET @start_batch_time = GETDATE();

		PRINT '===================================================================================================================';
		PRINT 'LOADING BRONZE LAYER ............';
		PRINT '===================================================================================================================';
		
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT 'Inserting Data Into Table : bronze.crm_cust_info';	
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\EHSAN\SQL\Sql - DataWarehouse - Analytics - Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table bronze.crm_cust_info : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';


		SET @start_time = GETDATE();
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		PRINT 'Truncating Table : bronze.crm_prd_info';	
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Inserting Data Into Table : bronze.crm_prd_info';	
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\EHSAN\SQL\Sql - DataWarehouse - Analytics - Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table bronze.crm_prd_info : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';


		SET @start_time = GETDATE();
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		PRINT 'Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Inserting Data Into Table : bronze.crm_sales_details';	
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\EHSAN\SQL\Sql - DataWarehouse - Analytics - Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table bronze.crm_sales_details : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';


		PRINT '-------------------------------------------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		PRINT 'Inserting Data Into Table : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\EHSAN\SQL\Sql - DataWarehouse - Analytics - Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table bronze.erp_cust_az12 : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';


		SET @start_time = GETDATE();
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		PRINT 'Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		
		PRINT 'Inserting Data Into Table : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\EHSAN\SQL\Sql - DataWarehouse - Analytics - Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table bronze.erp_loc_a101 : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';


		SET @start_time = GETDATE();
		PRINT '-------------------------------------------------------------------------------------------------------------------';
		PRINT 'Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		PRINT 'Inserting Data Into Table : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\EHSAN\SQL\Sql - DataWarehouse - Analytics - Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table bronze.erp_px_cat_g1v2 : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';
		PRINT '-------------------------------------------------------------------------------------------------------------------';

	END TRY

	BEGIN CATCH
		
		SET @start_time = GETDATE();		
		PRINT '===================================================================================================================';
		PRINT 'ERROR OCCURED ............';
		PRINT '===================================================================================================================';

		PRINT 'Error Message : ' + Error_Message();
		PRINT 'Error Number : ' + CAST(Error_Number() AS VARCHAR);
		PRINT 'Error Line : ' + CAST(Error_LINE() AS VARCHAR);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Error : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';
		
	END CATCH

	SET @end_batch_time = GETDATE();
	PRINT '===================================================================================================================';
	PRINT 'Loading Of Bronze Layer Is Completed.'
	PRINT '-Loading Duration Of Full Bronze Layer: ' + CAST(DATEDIFF(Second,@start_batch_time,@end_batch_time) AS VARCHAR) + ' Seconds';
	PRINT '===================================================================================================================';

END
