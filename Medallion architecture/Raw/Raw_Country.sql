/**************************************************************************
 In this section we will create the raw tables to load data from the stage !
 **************************************************************************
*/


-- Let's create the raw tables based on the content of the headers we have ! 

--1 Explore the schema of files :
USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA PUBLIC;


SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@S3_TASTY_FILES/raw_pos/country',
        FILE_FORMAT => 'S3_FF_TASTY'
    )
);


-- Creating COUNTRY table 
-- Note that for VARCHAR we can specify the lenght but it will not be enforced it is just for documentation
-- The NUMBER however is enforced ! it gives exact number as specified NUMBER(precision,scale)

USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA RAW;
CREATE OR REPLACE TABLE RAW_COUNTRY
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216),
    FILE_NAME VARCHAR(16777216),
    LOAD_TIME TIMESTAMP,
    file_last_modified TIMESTAMP
);

--Load data in the COUNTRY table

COPY INTO TASTY_BYTES.RAW.RAW_COUNTRY
FROM 
(
    SELECT 
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        METADATA$FILE_LAST_MODIFIED
    FROM @TASTY_BYTES.PUBLIC.S3_TASTY_FILES/raw_pos/country/country.csv.gz
)
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
ON_ERROR = ABORT_STATEMENT;

-- check what we loaded
SELECT * FROM TASTY_BYTES.RAW.RAW_COUNTRY;




