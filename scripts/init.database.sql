/*
Create Database and Schemas
==============================================================================================================================
            -- Script Purpose:
-- This script checks whether the 'DataWarehouse' database already exists.
-- If the 'DataWarehouse' database exists, it is dropped to ensure a clean setup.
-- After dropping, the 'DataWarehouse' database is recreated from scratch.
-- Once the database is created, the script sets up three schemas: bronze, silver, and gold.
-- These schemas are used to organize data by processing stages:
--   - Bronze: Raw or minimally processed data
--   - Silver: Cleaned and enriched data
--   - Gold: Final, business-ready data
-- This setup is typically used in data lakehouse or modern data warehouse architectures.
================================================================================================================================
            -- WARNING:
-- If the 'DataWarehouse' database already exists, this script will DROP the entire database,
-- including all data, schemas, and objects within it.
-- This operation is irreversible and will result in total data loss for that database.
-- Please run this script with caution.
-- Ensure that proper backups are in place before execution.
*/


USE master;
Go

IF EXISITS (Select 1 From Sys.Databases Where name = 'DataWarehouse')   
  BEGIN
      -- Set database to single-user mode to force disconnect any existing connections
      Alter Database DataWarehouse SET SINGLE_USER WITH ROLLBACK_IMMEDIATE

      -- Drop the existing database
      Drop Database DataWarehouse
  END;

CREATE Database DataWarehouse;
GO

USE DataWarehouse;
GO

-- Creating Schemas

CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO





