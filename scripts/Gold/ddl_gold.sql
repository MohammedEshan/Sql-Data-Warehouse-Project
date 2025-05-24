/*
-- Creating an integrated data model for analytics and reporting:
-- - gold.dim_customers: Combines CRM and ERP customer data to enrich records with name, gender (with fallback logic), 
--   marital status, birth date, and country, assigning a surrogate key for consistency.
-- - gold.dim_products: Merges CRM product details with ERP category attributes, including product line, cost, and maintenance info. 
--   Only active products (no end date) are included.
-- - gold.fact_sales: Links sales transactions to the customer and product dimensions, capturing order, shipment, and due dates,
--   along with sales metrics like quantity, price, and total sales amount.
*/


===================================================================================================================
-- Creating customer dimension view by integrating CRM and ERP data. 
===================================================================================================================

CREATE VIEW gold.dim_customers AS 
(
	SELECT 
		ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		CASE 
			WHEN ci.cst_gndr != 'N/A' THEN  ci.cst_gndr
			ELSE COALESCE(ca.gen,'N/A')
		END AS gender,
		ci.cst_marital_status AS marital_status,
		ca.bdate AS birth_date,
		ci.cst_create_date AS create_date
	FROM Silver.crm_cust_info ci
	LEFT JOIN
			silver.erp_cust_az12 ca
	ON		
			ci.cst_key = ca.cid
	LEFT JOIN 
			Silver.erp_loc_a101 la
	ON
			ci.cst_key = la.cid
)

===================================================================================================================
-- Creating product dimension view that combines CRM product information with ERP category data. 
===================================================================================================================

CREATE VIEW Gold.dim_products AS 
(
	SELECT
		ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id  AS category_id,
		pc.cat AS category,
		pc.subbcat AS sub_category,
		pc.maintenance AS maintenance,
		pn.prd_line AS product_line,
		pn.prd_cost AS product_cost,
		pn.prd_start_dt AS product_start_date
	FROM Silver.crm_prd_info AS pn
	LEFT JOIN
			Silver.erp_px_cat_g1v2 AS pc
	ON		
			pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL 
)


===================================================================================================================
-- Creating a sales fact view that links detailed sales transactions with enriched product and customer dimensions. 
===================================================================================================================

CREATE VIEW Gold.fact_sales AS 
(
	SELECT
		sd.sls_ord_num AS order_number,
		pr.product_key,
		cu.customer_key,
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS ship_date,
		sd.sls_due_dt AS due_date,
		sd.sls_price AS price,
		sd.sls_quantity AS quantity,
		sd.sls_sales AS sales_amount
	FROM Silver.crm_sales_details sd
	LEFT JOIN 
			Gold.dim_products pr
	on		
			sd.sls_prd_key = pr.product_number 
	LEFT JOIN
			Gold.dim_customers cu
	ON
			sd.sls_cust_id = cu.customer_id
)
