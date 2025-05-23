/*
====================================================================================================
DATA QUALITY & VALIDATION CHECKS FOR SILVER LAYER TABLES (BY TABLE CATEGORY)
----------------------------------------------------------------------------------------------------
Purpose:
To ensure that all Silver layer tables contain clean, standardized, and reliable data following 
the ETL process from the Bronze layer.

Each table is validated for key integrity, data cleanliness, standardization, and referential 
consistency where applicable. Expectations are that each query returns ZERO records. Any results 
indicate potential quality issues.

----------------------------------------------------------------------------------------------------
1. TABLE: silver.crm_cust_info
----------------------------------------------------------------------------------------------------
- Check for NULL or duplicate primary keys (`cst_id`)
- Detect leading/trailing whitespaces in name and gender fields
- Verify gender and marital status values are standardized (e.g., 'Male', 'Married')
- Expectation: Unique, trimmed, and normalized data

----------------------------------------------------------------------------------------------------
2. TABLE: silver.crm_prd_info
----------------------------------------------------------------------------------------------------
- Check for NULL or duplicate product IDs (`prd_id`)
- Identify whitespace issues in product name and line
- Validate cost is non-negative and not NULL
- Ensure `prd_line` values are standardized (e.g., 'Mountain', 'Road')
- Check logical date ordering: `prd_start_dt <= prd_end_dt`
- Expectation: Clean, accurate, and logically valid product data

----------------------------------------------------------------------------------------------------
3. TABLE: silver.crm_sales_details
----------------------------------------------------------------------------------------------------
- Trim whitespace from order number and product key
- Validate date fields (`sls_order_dt`, `sls_ship_dt`, `sls_due_dt`) are within valid range
- Ensure order dates precede ship/due dates
- Financial validation: `sales = quantity * price`, no NULL or zero/negative values
- Expectation: Accurate and complete sales data with no inconsistencies

----------------------------------------------------------------------------------------------------
4. TABLE: silver.erp_cust_az12
----------------------------------------------------------------------------------------------------
- Ensure no future birthdates and no dates too far in the past
- Confirm gender values are standardized (e.g., 'Male', 'Female')
- Expectation: Valid demographic information

----------------------------------------------------------------------------------------------------
5. TABLE: silver.erp_loc_a101
----------------------------------------------------------------------------------------------------
- Verify customer IDs (`cid`) match those in `crm_cust_info.cst_key`
- Standardize country names (e.g., 'Germany', 'United States')
- Expectation: Referential integrity and clean geographic data

----------------------------------------------------------------------------------------------------
6. TABLE: silver.erp_px_cat_g1v2
----------------------------------------------------------------------------------------------------
- Trim whitespace from category, subcategory, and maintenance fields
- Check for NULL or blank values in key fields
- Expectation: Well-formed, complete product category data

====================================================================================================
Note:
These checks act as post-ETL data gates and can be incorporated into automated data quality 
dashboards or validation suites to ensure trusted analytics and reporting.
====================================================================================================
*/




--====================================================================================================
--Checking 'silver.crm_cust_info'
--====================================================================================================


--Check For Nulls or Duplicates in Primary Key
-- Expectation : No Results --> Quality Data

SELECT 
	cst_id,
	COUNT(*) AS Count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for Unwanted Spaces in string 
-- Expectation : No Results --> Quality Data


--cst_firstname

SELECT 
	cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--cst_lastname

SELECT 
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--cst_marital_status

SELECT 
	cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)

--cst_gndr

SELECT 
	cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)


-- Data Standardization and consistancy
-- Replace Abbriviated Value to User-Friendly Name 
-- Expectation : No Results --> Quality Data


SELECT 
	DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT 
	DISTINCT cst_marital_status
FROM silver.crm_cust_info


--====================================================================================================
--Checking 'silver.crm_prd_info'
--====================================================================================================


--Check For Nulls or Duplicates in Primary Key
-- Expectation : No Results --> Quality Data


SELECT 
	prd_id,
	COUNT(*) AS Count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for Unwanted Spaces in string 
-- Expectation : No Results --> Quality Data

SELECT 
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT 
	prd_line
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Checking for Nulls or Negative number 
-- Expectation : No Results --> Quality Data

SELECT 
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization and consistancy
-- Expectation : replace abbriviated value to user-friendly name 

SELECT 
	DISTINCT prd_line
FROM silver.crm_prd_info


-- Check for Invalid Date Orders	
-- Expectation : Correct Order of Date

SELECT 
	*
FROM silver.crm_prd_info
WHERE  prd_start_dt> prd_end_dt 



--====================================================================================================
--Checking 'silver.crm_sales_details'
--====================================================================================================


-- Checking for leading or trailing whitpaces
-- Expectations : No Result

SELECT
	sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)


SELECT
	sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key)

-- Checking for Invalid Dates
-- Expectations : Operand type clash: date is incompatible with tinyint  

SELECT 
	NULLIF(sls_order_dt,0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0     -- throws error while comparing date value with int 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20260101  
OR sls_order_dt < 20000101



SELECT 
	NULLIF(sls_ship_dt,0) AS sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <= 0   -- throws error while comparing date value with int 
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20260101  
OR sls_ship_dt < 20000101


SELECT 
	NULLIF(sls_due_dt,0) AS sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0    -- throws error while comparing date value with int 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20260101  
OR sls_due_dt < 20000101


-- Checking for Invalid Date Orders
-- Expectations : No Result

SELECT 
	sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Checking Data Consistency : Between Sales , Quantity , Price 
--> Sales = Quantity * Price
--> Value Must not be NULL or Zero or Negative 
-- Expectations : No Result



SELECT 
	sls_quantity,
	sls_price,
	sls_sales
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR	sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR	sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL



--====================================================================================================
--Checking 'silver.erp_cust_az12'
--====================================================================================================



-- Identify out of range dates 
-- Expectation : No Future Dates , set to null
SELECT 
	DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


-- Checking for Data Standardization & Consistency
-- Expectations : Normalized Data

SELECT 
	DISTINCT gen
FROM silver.erp_cust_az12



--====================================================================================================
--Checking 'silver.erp_loc_a101 '
--====================================================================================================



-- Checking the matching column with other table
-- Expectations : No Result 
SELECT * 
FROM (
	SELECT 
		cid
FROM silver.erp_loc_a101 
)T
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Checking for Data Standardization and Consistency
-- Expectations : Normalized Data

SELECT
	DISTINCT cntry
FROM silver.erp_loc_a101

	

--====================================================================================================
--Checking 'silver.erp_px_cat_g1v2'
--====================================================================================================



-- Checking for Unmwanted Spaces 
-- Expectation : No Result
SELECT 
	cat,
	subbcat,
	maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
OR subbcat != TRIM(subbcat)
OR maintenance != TRIM(maintenance)

-- Checking for Data Standardization and Consistency
-- Expectation : No Result

SELECT 
	DISTINCT cat
FROM silver.erp_px_cat_g1v2
WHERE cat IS NULL OR cat = ''

SELECT 
	DISTINCT subbcat
FROM silver.erp_px_cat_g1v2
WHERE subbcat IS NULL OR subbcat = ''


SELECT 
	DISTINCT maintenance
FROM silver.erp_px_cat_g1v2
WHERE maintenance IS NULL OR maintenance = ''
