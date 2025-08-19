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
        LOCATION => '@S3_TASTY_FILES/raw_pos/order_detail',
        FILE_FORMAT => 'S3_FF_TASTY'
    )
);


-- Creating ORDER_DETAIL table 
-- Note that for VARCHAR we can specify the lenght but it will not be enforced it is just for documentation
-- The NUMBER however is enforced ! it gives exact number as specified NUMBER(precision,scale)

USE ROLE DATA_ENGINEER;
USE DATABASE TASTY_BYTES;
USE SCHEMA RAW;
CREATE OR REPLACE TABLE RAW_ORDER_DETAIL
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216),
    FILE_NAME VARCHAR(16777216),
    LOAD_TIME TIMESTAMP,
    file_last_modified TIMESTAMP
);


-- Optional ! Dry run to validate the data loading 
COPY INTO TASTY_BYTES.RAW.RAW_ORDER_DETAIL
FROM @TASTY_BYTES.PUBLIC.S3_TASTY_FILES/raw_pos/order_detail/
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
VALIDATION_MODE = RETURN_ERRORS;

--Load data in the ORDER_DETAIL table

COPY INTO TASTY_BYTES.RAW.RAW_ORDER_DETAIL
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
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        METADATA$FILE_LAST_MODIFIED
    FROM @TASTY_BYTES.PUBLIC.S3_TASTY_FILES/raw_pos/order_detail/
)
FILE_FORMAT = 'TASTY_BYTES.PUBLIC.S3_FF_TASTY'
ON_ERROR = 'ABORT_STATEMENT';

-- Verify errors

SELECT * 
FROM TABLE(VALIDATE(TASTY_BYTES.RAW.RAW_ORDER_DETAIL, JOB_ID => '_last'));

-- check what we loaded
SELECT * FROM TASTY_BYTES.RAW.RAW_ORDER_DETAIL;

