use master;

create DATABASE DataWarehouse;

use DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


if OBJECT_ID ('bronze.crm_cust_info','U') is not null
	DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

if OBJECT_ID ('bronze.crm_prd_info','U') is not null
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);


if OBJECT_ID ('bronze.crm_sales_details','U') is not null
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

if OBJECT_ID ('bronze.erp_cust_az12','U') is not null
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

if OBJECT_ID ('bronze.erp_loc_a101','U') is not null
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry  NVARCHAR(50)
);

if OBJECT_ID ('bronze.erp_px_cat_g1v2','U') is not null
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @StartTime DATETIME, @EndTime DATETIME, @Duration INT;

    BEGIN TRY
        PRINT 'Starting data load process...';

        -- CRM Customer Info
        PRINT 'Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        SET @StartTime = GETDATE();
        PRINT 'Loading data into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @EndTime = GETDATE();
        SET @Duration = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Data loaded successfully into bronze.crm_cust_info in ' + CAST(@Duration AS VARCHAR) + ' seconds.';

        -- CRM Product Info
        PRINT 'Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        SET @StartTime = GETDATE();
        PRINT 'Loading data into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @EndTime = GETDATE();
        SET @Duration = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Data loaded successfully into bronze.crm_prd_info in ' + CAST(@Duration AS VARCHAR) + ' seconds.';

        -- CRM Sales Details
        PRINT 'Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @StartTime = GETDATE();
        PRINT 'Loading data into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @EndTime = GETDATE();
        SET @Duration = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Data loaded successfully into bronze.crm_sales_details in ' + CAST(@Duration AS VARCHAR) + ' seconds.';

        -- ERP Location A101
        PRINT 'Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        SET @StartTime = GETDATE();
        PRINT 'Loading data into bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\datawarehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @EndTime = GETDATE();
        SET @Duration = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Data loaded successfully into bronze.erp_loc_a101 in ' + CAST(@Duration AS VARCHAR) + ' seconds.';

        -- ERP Customer AZ12
        PRINT 'Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        SET @StartTime = GETDATE();
        PRINT 'Loading data into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\datawarehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @EndTime = GETDATE();
        SET @Duration = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Data loaded successfully into bronze.erp_cust_az12 in ' + CAST(@Duration AS VARCHAR) + ' seconds.';

        -- ERP PX Category G1V2
        PRINT 'Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        SET @StartTime = GETDATE();
        PRINT 'Loading data into bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\datawarehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SET @EndTime = GETDATE();
        SET @Duration = DATEDIFF(SECOND, @StartTime, @EndTime);
        PRINT 'Data loaded successfully into bronze.erp_px_cat_g1v2 in ' + CAST(@Duration AS VARCHAR) + ' seconds.';

        PRINT 'Data load process completed successfully.';
    
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred during data load!';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
    END CATCH
END;




SELECT * FROM bronze.crm_cust_info;
select * from bronze.crm_prd_info;
select * from bronze.crm_sales_details;
