-- Use sysadmin role to create the database objects and warehouses

USE ROLE SYSADMIN;

-- Create a database

CREATE OR REPLACE DATABASE Tasty_Bytes;

-- Create a the medallion architecture


USE ROLE DATA_ENGINEER;

CREATE OR REPLACE SCHEMA RAW;

CREATE OR REPLACE SCHEMA SILVER;

CREATE OR REPLACE SCHEMA GOLD;

-- Use the database

USE DATABASE Tasty_Bytes;

-- Use sysadmin role to crea

USE ROLE TASTY_ADMIN;
-- Create warehouses

-- ***** Data engineering WH *****
CREATE OR REPLACE WAREHOUSE DE_WH
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for tasty bytes';

-- ***** Data analysis WH *****
CREATE OR REPLACE WAREHOUSE DA_WH
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for tasty bytes';

-- Use only if we create a service principal to connect to the storage account (AWS, Azure, GCP)

CREATE STORAGE INTEGRATION tasty_files
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://sfquickstarts/frostbyte_tastybytes/');

-- Create the file format to read files in the storages

CREATE OR REPLACE FILE FORMAT s3_ff_tasty
TYPE = 'CSV'
PARSE_HEADER = TRUE;

/* Create the external stage : for public ones we just provide the link with no credentials */


CREATE OR REPLACE STAGE s3_tasty_files
URL = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = Tasty_Bytes.public.s3_ff_tasty;


list @s3_tasty_files;


CREATE OR REPLACE FILE FORMAT raw_movies
TYPE = 'CSV'
PARSE_HEADER = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ',';

CREATE OR REPLACE STAGE movies_database
    URL = 'azure://adlsecodemos.blob.core.windows.net/moviesraw/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'sp=rwl&st=2025-08-03T20:36:47Z&se=2025-11-30T05:51:47Z&spr=https&sv=2024-11-04&sr=c&sig=heMq3pOrBpvB27cX37S%2BGFX8nivu3mK6obMNn3mqhQo%3D')
    file_format = raw_movies;

SELECT NAME FROM (list @movies_database) WHERE NAME LIKE '%json%';

SELECT $4 FROM @movies_database/tmdb_5000_credits.csv;


SELECT * FROM 
TABLE ( 
    INFER_SCHEMA(
    LOCATION => '@movies_database/tmdb_5000_credits.csv',
    FILE_FORMAT=>'raw_movies'
    )
    );