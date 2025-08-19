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
        LOCATION => '@S3_TASTY_FILES/raw_pos/truck',
        FILE_FORMAT => 'S3_FF_TASTY'
    )
);


-- Creating TRUCK table 
-- Note that for VARCHAR we can specify the lenght but it will not be enforced it is just for documentation
-- The NUMBER however is enforced ! it gives exact number as specified NUMBER(precision,scale)

USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA RAW;
CREATE OR REPLACE TABLE RAW_TRUCK
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE,
    FILE_NAME VARCHAR(16777216),
    LOAD_TIME TIMESTAMP,
    file_last_modified TIMESTAMP
);

--Load data in the TRUCK table

COPY INTO TASTY_BYTES.RAW.RAW_TRUCK
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
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        METADATA$FILE_LAST_MODIFIED
    FROM @TASTY_BYTES.PUBLIC.S3_TASTY_FILES/raw_pos/truck/truck.csv.gz
)
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
ON_ERROR = ABORT_STATEMENT;

-- check what we loaded
SELECT * FROM TASTY_BYTES.RAW.RAW_TRUCK;


