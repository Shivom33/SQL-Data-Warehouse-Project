/*===============================================================================
Quality Checks for Silver Layer
===============================================================================*/

PRINT '=======================================';
PRINT 'Running Quality Checks for Silver Layer';
PRINT '=======================================';
GO

-- =====================================================
-- Checking silver.crm_cust_info
-- =====================================================
PRINT 'Checking: silver.crm_cust_info - NULL or Duplicate Primary Keys';
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
GO

PRINT 'Checking: silver.crm_cust_info - Unwanted Spaces in cst_key';
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);
GO

PRINT 'Checking: silver.crm_cust_info - Distinct Marital Status Values';
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;
GO

-- =====================================================
-- Checking silver.crm_prd_info
-- =====================================================
PRINT 'Checking: silver.crm_prd_info - NULL or Duplicate Primary Keys';
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
GO

PRINT 'Checking: silver.crm_prd_info - Unwanted Spaces in prd_nm';
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
GO

PRINT 'Checking: silver.crm_prd_info - NULL or Negative Values in Cost';
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
GO

PRINT 'Checking: silver.crm_prd_info - Distinct Product Line Values';
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;
GO

PRINT 'Checking: silver.crm_prd_info - Invalid Date Orders (Start Date > End Date)';
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
GO

-- =====================================================
-- Checking silver.crm_sales_details
-- =====================================================
PRINT 'Checking: bronze.crm_sales_details - Invalid Due Dates';
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;
GO

PRINT 'Checking: silver.crm_sales_details - Invalid Date Orders';
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;
GO

PRINT 'Checking: silver.crm_sales_details - Sales Value Consistency';
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;
GO

-- =====================================================
-- Checking silver.erp_cust_az12
-- =====================================================
PRINT 'Checking: silver.erp_cust_az12 - Out-of-Range Birthdates';
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();
GO

PRINT 'Checking: silver.erp_cust_az12 - Distinct Gender Values';
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;
GO

-- =====================================================
-- Checking silver.erp_loc_a101
-- =====================================================
PRINT 'Checking: silver.erp_loc_a101 - Distinct Country Values';
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;
GO

-- =====================================================
-- Checking silver.erp_px_cat_g1v2
-- =====================================================
PRINT 'Checking: silver.erp_px_cat_g1v2 - Unwanted Spaces in Text Fields';
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);
GO

PRINT 'Checking: silver.erp_px_cat_g1v2 - Distinct Maintenance Values';
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
GO
