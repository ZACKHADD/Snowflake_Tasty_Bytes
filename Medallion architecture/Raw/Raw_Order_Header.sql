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
        LOCATION => '@S3_TASTY_FILES/raw_pos/order_header',
        FILE_FORMAT => 'S3_FF_TASTY'
    )
);


-- Creating ORDER_HEADER table 
-- Note that for VARCHAR we can specify the lenght but it will not be enforced it is just for documentation
-- The NUMBER however is enforced ! it gives exact number as specified NUMBER(precision,scale)

USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA RAW;
CREATE OR REPLACE TABLE RAW_ORDER_HEADER
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4),
    FILE_NAME VARCHAR(16777216),
    LOAD_TIME TIMESTAMP,
    file_last_modified TIMESTAMP
);

--Load data in the ORDER_HEADER table

COPY INTO TASTY_BYTES.RAW.RAW_ORDER_HEADER
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
        $16,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        METADATA$FILE_LAST_MODIFIED
    FROM @TASTY_BYTES.PUBLIC.S3_TASTY_FILES/raw_pos/order_header/
)
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
ON_ERROR = ABORT_STATEMENT;

-- check what we loaded
SELECT * FROM TASTY_BYTES.RAW.RAW_ORDER_HEADER;


