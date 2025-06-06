/*
-- ==================================================================================================
-- DATA QUALITY CHECKS FOR GOLD LAYER TABLES 
-- ==================================================================================================
-- Data Quality Checks for Dimensional Model Integrity:
-- 1. Checking for duplicate customer records after joining CRM and ERP customer/location tables using customer ID.
--    Ensures that joins do not introduce unintended record multiplication.
-- 2. Checking for duplicate product records after joining CRM product info with ERP category data using product key.
--    Confirms uniqueness of product dimension entries after filtering out inactive products.
-- 3. Validating referential integrity in the fact_sales view by identifying records where customer or product 
--    foreign keys do not match any entry in the corresponding dimension tables.
--    Highlights potential join issues or missing dimension data affecting reporting accuracy.
*/


-- ==================================================================================================
-- Checking for gold.dim_customers
-- ==================================================================================================

--Checking if Duplicates Present After Joining Tables 
-- Expectation : No Result

SELECT
	cst_id,
	COUNT(*)
FROM (
	SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_gndr,
		ci.cst_marital_status,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM Silver.crm_cust_info ci
	LEFT JOIN
			silver.erp_cust_az12 ca
	ON		
			ci.cst_key = ca.cid
	LEFT JOIN 
			Silver.erp_loc_a101 la
	ON
			ci.cst_key = la.cid
)t 
GROUP BY cst_id
HAVING COUNT(*) > 1


-- ==================================================================================================
-- Checking for gold.dim_products
-- ==================================================================================================

-- checking data quality after creating product object
-- checking for duplicates after joining 
-- Expectation : No Result


SELECT 
	prd_key,
	COUNT(*)
FROM (
	SELECT
		pn.prd_id,
		pn.prd_key,
		pn.prd_nm,
		pn.cat_id,
		pc.cat AS category,
		pc.subbcat,
		pc.maintenance,
		pn.prd_line,
		pn.prd_cost,
		pn.prd_start_dt 
	FROM Silver.crm_prd_info AS pn
	LEFT JOIN
			Silver.erp_px_cat_g1v2 AS pc
	ON		
			pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL    -- filtering out historical data
)t
GROUP BY prd_key
HAVING COUNT(*) > 1 


-- ==================================================================================================
-- Checking for gold.fact_sales
-- ==================================================================================================

-- Foreign Key Integrity (Dimensions)
-- checking if all Dimension tables join successfully to Fact Table
-- Expectation : No Result

SELECT *
FROM Gold.fact_sales AS fs
LEFT JOIN 
    Gold.dim_customers AS dc
ON 
    fs.customer_key = dc.customer_key
LEFT JOIN 
    Gold.dim_products AS dp
ON 
    fs.product_key = dp.product_key
WHERE dc.customer_key IS NULL OR dp.product_key IS NULL


