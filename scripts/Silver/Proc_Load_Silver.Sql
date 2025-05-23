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

====================================================================================
*/
