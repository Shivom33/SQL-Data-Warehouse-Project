 create or alter  procedure bronze.load_bronze as 
begin
declare @start_time datetime, @end_time datetime ,@batch_start_time datetime, @batch_end_time datetime;
begin try
set @batch_start_time= getdate();
print '===========================================================';
print'Loading Bronze Layer';
print'============================================================';
print '-----------------------------------------------------------';
print 'Loading CRM table';
print '-----------------------------------------------------------';



set @start_time= getdate();
print '>>Truncating Table: bronze.crm_cust_info';
truncate table bronze.crm_cust_info


print '>> Inserting Data Into Table:bronze.crm_cust_info';
bulk insert bronze.crm_cust_info
from 'E:\first project\datasets\source_crm\cust_info.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time= getdate();
print '>> Load duration :' +cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'



set @start_time= getdate();
print '>>Truncating Table: bronze.crm_prd_info'
 truncate table bronze.crm_prd_info
 

 print '>> Inserting Data Into Table:bronze.crm_prd_info';
 BULK insert bronze.crm_prd_info
from 'E:\first project\datasets\source_crm\prd_info.csv'
with( 
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time= getdate();
print '>> Load duration :' +cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'



set @start_time= getdate();
print '>>Truncating Table: bronze.crm_sales_details'
truncate table bronze.crm_sales_details 


print '>> Inserting Data Into Table:bronze.crm_sales_details';
BULK insert bronze.crm_sales_details
from 'E:\first project\datasets\source_crm\sales_details.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time= getdate();
print '>> Load duration :' +cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'



print '-----------------------------------------------------------';
print 'Loading ERP table';
print '-----------------------------------------------------------';



set @start_time= getdate();
print '>>Truncating Table:bronze.erp_cust_az12'
truncate table bronze.erp_cust_az12


print '>> Inserting Data Into Table:bronze.erp_cust_az12';
BULK insert bronze.erp_cust_az12
from 'E:\first project\datasets\source_erp\cust_az12.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time= getdate();
print '>> Load duration :' +cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'




set @start_time= getdate();
print '>>Truncating Table: bronze.erp_loc_a101'
truncate table bronze.erp_loc_a101


print '>> Inserting Data Into Table:bronze.erp_loc_a101';
BULK insert bronze.erp_loc_a101
from 'E:\first project\datasets\source_erp\loc_a101.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time= getdate();
print '>> Load duration :' +cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'



set @start_time= getdate();
print '>>Truncating Table: bronze.erp_px_cat_g1v2'
truncate table bronze.erp_px_cat_g1v2


print '>> Inserting Data Into Table:bronze.erp_px_cat_g1v2';
BULK insert bronze.erp_px_cat_g1v2
from 'E:\first project\datasets\source_erp\px_cat_g1v2.csv'
with(
firstrow = 2,
fieldterminator = ',',
tablock
);
set @end_time= getdate();
print '>> Load duration :' +cast(datediff(second,@start_time,@end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'


set @batch_end_time= getdate();
print '------------------------------------------------------------------'
print'Loading Bronze Layer is Completed';
print '>> Total Load duration :' +cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+ 'seconds';
print '------------------------------------------------------------------'
end try
begin catch
print'-------------------------------------------------------';
print'ERROR OCCURED DURING LOADING BRONZE LAYER'
print 'Error Message' + ERROR_MESSAGE();
print 'Error Message' + cast(ERROR_NUMBER() AS Nvarchar);
print'Error Message' + cast(error_state() as nvarchar );
print'-------------------------------------------------------';
end catch;
end;