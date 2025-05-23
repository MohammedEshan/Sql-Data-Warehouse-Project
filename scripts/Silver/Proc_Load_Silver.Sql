/*
====================================================================================
ETL PROCEDURE: Silver.load_silver
------------------------------------------------------------------------------------
Purpose:
This procedure performs the ETL (Extract, Transform, Load) operations to populate 
the Silver layer from the Bronze layer in a structured and standardized format.

Overview of Steps:
1. Logging & Timing:
   - Captures timestamps to measure and log processing durations for each table.

2. CRM Data Load:
   - Silver.crm_cust_info:
     - Removes duplicates by selecting the latest record per customer.
     - Standardizes gender and marital status fields.
   - Silver.crm_prd_info:
     - Extracts category ID and product key from composite keys.
     - Normalizes product line values.
     - Computes end dates using LEAD window function for SCD-like behavior.
   - Silver.crm_sales_details:
     - Validates and formats date fields.
     - Ensures calculated sales match quantity * price logic.
     - Recalculates price if necessary to ensure consistency.

3. ERP Data Load:
   - Silver.erp_cust_az12:
     - Cleans up customer ID (removes 'NAS' prefix if present).
     - Handles invalid birthdates and normalizes gender.
   - Silver.erp_loc_a101:
     - Removes hyphens from customer IDs.
     - Converts country codes into user-friendly names.
   - Silver.erp_px_cat_g1v2:
     - Simple direct load without transformation.

4. Error Handling:
   - A TRY-CATCH block ensures any failure is logged with details (message, number, line).
   - Execution time for failed sections is also logged.

5. Completion Logging:
   - Total batch duration is printed for monitoring and performance insights.

Note:
This is a truncate-and-load (full refresh) approach, not incremental.
Ensure no recursive calls or triggers are executing this procedure to avoid nesting errors.

Usage Example:
    EXEC Silver.load_Silver;
====================================================================================
*/


GO
CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
		
	BEGIN TRY 

		DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_time DATETIME, @end_batch_time DATETIME;

		SET @start_batch_time = GETDATE();

		PRINT '==================================================================================';
		PRINT 'Loading Silver Layer............';
		PRINT '==================================================================================';
		PRINT '----------------------------------------------------------------------------------';
		PRINT 'Loading CRM TABLES';
		PRINT '----------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table : Silver.crm_cust_info';
		TRUNCATE TABLE Silver.crm_cust_info;

		PRINT 'Inserting Data Into : Silver.crm_cust_info';
		INSERT INTO Silver.crm_cust_info
			(
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
				ELSE 'N/A'
			END AS cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
				ELSE 'N/A'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table Silver.crm_cust_info : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';



		SET @start_time = GETDATE();
		PRINT '----------------------------------------------------------------------------------';
		PRINT 'Truncating Table : Silver.crm_prd_info';
		TRUNCATE TABLE Silver.crm_prd_info;

		PRINT 'Inserting Data Into : Silver.crm_prd_info';
		INSERT INTO Silver.crm_prd_info
			(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt ,
				prd_end_dt
			)
		SELECT		 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			TRIM(prd_nm) prd_nm,
			COALESCE(prd_cost,0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'MOUNTAIN'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt  
		FROM Bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table Silver.crm_prd_info : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';



		SET @start_time = GETDATE();
		PRINT '----------------------------------------------------------------------------------';
		PRINT 'Truncating Table : Silver.crm_sales_details ';
		TRUNCATE TABLE Silver.crm_sales_details ;

		PRINT 'Inserting Data Into : Silver.crm_sales_details ';
		INSERT INTO Silver.crm_sales_details 
			(
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
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity, 0) 
				ELSE sls_price
			END AS sls_price
		FROM Bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table Silver.crm_sales_details : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';



		PRINT '----------------------------------------------------------------------------------';
		PRINT 'Loading ERP TABLES';
		PRINT '----------------------------------------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table : Silver.erp_cust_az12 ';
		TRUNCATE TABLE Silver.erp_cust_az12 ;

		PRINT 'Inserting Data Into : Silver.erp_cust_az12';
		INSERT INTO Silver.erp_cust_az12
			(
				cid,
				bdate,
				gen
			)
		SELECT
			CASE	
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid 
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				ELSE 'N/A'
			END AS gen
		FROM Bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table Silver.erp_cust_az12 : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';



		SET @start_time = GETDATE();
		PRINT '----------------------------------------------------------------------------------';
		PRINT 'Truncating Table : Silver.erp_loc_a101 ';
		TRUNCATE TABLE Silver.erp_loc_a101 ;

		PRINT 'Inserting Data Into : Silver.erp_loc_a101';
		INSERT INTO Silver.erp_loc_a101
			(
				cid,
				cntry
			)
		SELECT 
			REPLACE(cid,'-','') cid,
			CASE
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(CNTRY)) IN ('US','USA') THEN 'United States'
				WHEN cntry = '' OR CNTRY IS NULL THEN 'N/A'
				ELSE cntry
			END AS cntry
		FROM Bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table Silver.erp_loc_a101 : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';



		SET @start_time = GETDATE();
		PRINT '----------------------------------------------------------------------------------';
		PRINT 'Truncating Table : Silver.erp_px_cat_g1v2';
		TRUNCATE TABLE Silver.erp_px_cat_g1v2 ;

		PRINT 'Inserting Data Into : Silver.erp_px_cat_g1v2';
		INSERT INTO Silver.erp_px_cat_g1v2
			(
				id,
				cat,
				subbcat,
				maintenance
			)
		SELECT 
			id,
			cat,
			subbcat,
			maintenance
		FROM Bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Table Silver.erp_px_cat_g1v2 : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';
		PRINT '-------------------------------------------------------------------------------------------------------------------';

	END TRY


	BEGIN CATCH
		
		SET @start_time = GETDATE();		
		PRINT '===================================================================================================================';
		PRINT 'ERROR OCCURED ............';
		PRINT '===================================================================================================================';

		PRINT 'Error Message : ' + Error_Message();
		PRINT 'Error Number : ' + CAST(Error_Number() AS VARCHAR);
		PRINT 'Error Line : ' + CAST(Error_Line() AS VARCHAR);
		PRINT 'Error Line : ' + CAST(Error_State() AS VARCHAR);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration Of Error : ' + '0.' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' ms';
		
	END CATCH


	SET @end_batch_time = GETDATE();
	PRINT '===================================================================================================================';
	PRINT 'Loading Of Silver Layer Is Completed.'
	PRINT '-Loading Duration Of Full Silver Layer: ' + CAST(DATEDIFF(Second,@start_batch_time,@end_batch_time) AS VARCHAR) + ' Seconds';
	PRINT '===================================================================================================================';

END

