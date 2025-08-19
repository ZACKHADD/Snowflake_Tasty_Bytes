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
        LOCATION => '@S3_TASTY_FILES/raw_pos/location',
        FILE_FORMAT => 'S3_FF_TASTY'
    )
);


-- Creating LOCATION table 
-- Note that for VARCHAR we can specify the lenght but it will not be enforced it is just for documentation
-- The NUMBER however is enforced ! it gives exact number as specified NUMBER(precision,scale)

USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA RAW;
CREATE OR REPLACE TABLE RAW_LOCATION
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216),
    FILE_NAME VARCHAR(16777216),
    LOAD_TIME TIMESTAMP,
    file_last_modified TIMESTAMP
);

--Load data in the LOCATION table

COPY INTO TASTY_BYTES.RAW.RAW_LOCATION
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
    FROM @TASTY_BYTES.PUBLIC.S3_TASTY_FILES/raw_pos/location/location.csv.gz
)
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
ON_ERROR = ABORT_STATEMENT;

-- check what we loaded
SELECT * FROM TASTY_BYTES.RAW.RAW_LOCATION;


