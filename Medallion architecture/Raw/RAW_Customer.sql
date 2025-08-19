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
        LOCATION => '@"TASTY_BYTES"."PUBLIC"."S3_TASTY_FILES"/raw_customer/customer_loyalty/customer_loyalty_0_0_0.csv.gz',
        FILE_FORMAT => 'S3_FF_TASTY'
    )
);


-- Creating CUSTOMER table 
-- Note that for VARCHAR we can specify the lenght but it will not be enforced it is just for documentation
-- The NUMBER however is enforced ! it gives exact number as specified NUMBER(precision,scale)

USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA RAW;
CREATE OR REPLACE TABLE RAW_CUSTOMER
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216),
    FILE_NAME VARCHAR(16777216),
    LOAD_TIME TIMESTAMP,
    file_last_modified TIMESTAMP
);

--Load data in the CUSTOMER table

COPY INTO TASTY_BYTES.RAW.RAW_CUSTOMER
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
        $15,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        METADATA$FILE_LAST_MODIFIED
    FROM '@"TASTY_BYTES"."PUBLIC"."S3_TASTY_FILES"/raw_customer/customer_loyalty/customer_loyalty_0_0_0.csv.gz'
)
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
ON_ERROR = ABORT_STATEMENT;

-- check what we loaded
SELECT * FROM TASTY_BYTES.RAW.RAW_CUSTOMER;


