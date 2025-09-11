/*
This script creates a new database 'DataWarehouse' 
and sets up three schemas within the database
*/

USE master;

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- create database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- creating schema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO



