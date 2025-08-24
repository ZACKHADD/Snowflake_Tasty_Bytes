/**************************************************************************
    This file is reserved for queries to explore and test transformations
 **************************************************************************
*/

/**************************************************************************
                             Menu table
**************************************************************************/                            
USE ROLE DATA_ENGINEER;
SELECT * FROM TASTY_BYTES.RAW.RAW_MENU;

-- We can use lateral flatten to transform semi-structured data and retrieve additional columns
WITH V_MENU AS
(
SELECT 
    menu_id,
    menu_type_id,
    menu_type,
    truck_brand_name,
    menu_item_id,
    menu_item_name,
    item_category,
    item_subcategory,
    cost_of_goods_usd,
    sale_price_usd,
    lf.VALUE:ingredients::VARIANT ingredients,
    lf.VALUE:is_dairy_free_flag::VARCHAR is_dairy_free_flag,
    lf.VALUE:is_gluten_free_flag::VARCHAR is_gluten_free_flag,
    lf.VALUE:is_healthy_flag::VARCHAR is_healthy_flag,
    lf.VALUE:is_nut_free_flag::VARCHAR is_nut_free_flag
FROM TASTY_BYTES.RAW.RAW_MENU,
LATERAL FLATTEN (input => menu_item_health_metrics_obj:menu_item_health_metrics) lf
)


-- Now we can query that transformed data to search if the ingredients column contains some ingredient
-- We use ARRAY_CONTAINS because the column ingredients is of type ARRAY (more generaly VARIANT)

SELECT * FROM V_MENU
WHERE ARRAY_CONTAINS('Lemons'::VARIANT,ingredients);

-- Note that in the case of semi-structured arrays we need to explicitly cast literals to variants or it throws an error
-- if the element we search for is a numeric, then no need ! Snowflake auto-cast that to variant

SELECT ARRAY_CONTAINS(3, ARRAY_CONSTRUCT(1,2,3)); -- Works because it is a numeric type ! auto-casted

-- Throws an error because ARRAY_CONSTRUCT generates a VARIANT type not structured array !
SELECT ARRAY_CONTAINS('Lemons', ARRAY_CONSTRUCT('Lemons','T','L'));

-- Works now because we casted the array_construct to real structured array of type varchar
SELECT ARRAY_CONTAINS('Lemons', ARRAY_CONSTRUCT('Lemons','T','L')::ARRAY(VARCHAR));

-- Another way of dealing with semi-structured data would be to use EXPANSION Operator **
-- This turns elements in a semi-structured CONSTANT ARRAYS data into separate columns 

SELECT ** [3, 4] as col;

SELECT COALESCE(** [NULL, NULL, 'my_string_1', 'my_string_2']) AS first_non_null;


/**************************************************************************
                             Country table
**************************************************************************/  

SELECT * FROM TASTY_BYTES.RAW.RAW_COUNTRY;



/**************************************************************************
                             Customer table
**************************************************************************/  

SELECT * FROM TASTY_BYTES.RAW.raw_customer;

SELECT COUNT(*) FROM TASTY_BYTES.RAW.raw_customer;

SELECT COUNT(FAVOURITE_BRAND) FROM TASTY_BYTES.RAW.raw_customer;

SELECT COUNT(*) FROM TASTY_BYTES.RAW.raw_customer WHERE FAVOURITE_BRAND IS NULL;



/**************************************************************************
                             FRANCHISE table
**************************************************************************/  

SELECT * FROM TASTY_BYTES.RAW.RAW_FRANCHISE;

-- Let's check for absolute dupplicates if there are any ! Not only based on ID but on all columns
-- Normaly we should exclude metadata columns !
SELECT 
(SELECT COUNT(DISTINCT(*)) FROM TASTY_BYTES.RAW.RAW_FRANCHISE) = (SELECT COUNT(*) FROM TASTY_BYTES.RAW.RAW_FRANCHISE); --Flase

--Let's identify them
SELECT *, COUNT(*)
FROM TASTY_BYTES.RAW.RAW_FRANCHISE
GROUP BY ALL
HAVING COUNT(*) > 1;

-- Let's drop dupplicates
-- Using row number to retrive only the first appearence
SELECT *
FROM TASTY_BYTES.RAW.RAW_FRANCHISE
QUALIFY 
ROW_NUMBER() OVER 
    (
    PARTITION BY 
    FRANCHISE_ID, FIRST_NAME, LAST_NAME, CITY, COUNTRY, E_MAIL, PHONE_NUMBER 
    ORDER BY FRANCHISE_ID ASC ) = 1;

-- Let's check the data quality for emails and phone numbers
-- We can check also if data is NULL or empty

SELECT FRANCHISE_ID
FROM TASTY_BYTES.RAW.RAW_FRANCHISE
WHERE E_MAIL IS NULL OR TRIM(E_MAIL) = ''
      AND PHONE_NUMBER IS NULL OR TRIM(PHONE_NUMBER) = '';

-- Using regex to check for invalid data
SELECT FRANCHISE_ID
FROM TASTY_BYTES.RAW.RAW_FRANCHISE
WHERE regexp_like(E_MAIL,'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
AND regexp_like(PHONE_NUMBER,'^[0-9]{3,3}+-[0-9]{3,3}+-[0-9]{4,4}$');

-- Snowflake expects that the regex pattern matchs all the value of the column
-- For example : starts with a digit would be :
SELECT lf.VALUE Phones
FROM LATERAL FLATTEN(INPUT => ['865-015-6733','670-95-0505','607-052-4445']) lf
WHERE regexp_like(Phones,'^[0-9]+.*');

-- However this will not work :

SELECT lf.VALUE Phones
FROM LATERAL FLATTEN(INPUT => ['865-015-6733','670-95-0505','607-052-4445']) lf
WHERE regexp_like(Phones,'^[0-9]+'); -- this specifies only digits while we have also '-'


/**************************************************************************
                             Location table
**************************************************************************/  

SELECT * FROM TASTY_BYTES.RAW.RAW_LOCATION;

-- Check for duplicates

SELECT 
(SELECT COUNT(DISTINCT(*)) FROM TASTY_BYTES.RAW.RAW_LOCATION) = (SELECT COUNT(*) FROM TASTY_BYTES.RAW.RAW_LOCATION);


SELECT *, COUNT(*)
FROM TASTY_BYTES.RAW.RAW_LOCATION
GROUP BY ALL
HAVING COUNT(*) > 1;

-- Pay attention tu nulls in a column ! it can hide dupplicates
-- This does not exclude nulls
SELECT COUNT(*) FROM (SELECT DISTINCT *  FROM TASTY_BYTES.RAW.RAW_LOCATION); -- 13,093
-- This does exclude nulls
SELECT COUNT(DISTINCT(*)) FROM TASTY_BYTES.RAW.RAW_LOCATION; -- 13,055

-- We identified the region column as the one containing nulls ! we exclude it and recheck the count
SELECT DISTINCT * EXCLUDE REGION FROM TASTY_BYTES.RAW.RAW_LOCATION; -- 13,093

-- Now we are sure there is are no absolute dupplicates

-- Automatic null detection in columns using blocks and snowflake scripting
-- Set variables
SET table_name = 'RAW_LOCATION';
SET schema_name = 'RAW';
DECLARE 
col_name STRING;
dyn_sql STRING;
c1 CURSOR FOR 
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = $schema_name 
        AND table_name = $table_name;
rs RESULTSET;

BEGIN
  -- create temp result holder
  CREATE OR REPLACE TEMP TABLE tmp_nulls (column_name STRING, null_count NUMBER);

  FOR rec IN c1
  DO
    dyn_sql := 'INSERT INTO tmp_nulls ' ||
               'SELECT ''' || rec.column_name || ''', COUNT(*) - COUNT("' || rec.column_name || '") ' ||
               'FROM RAW.RAW_LOCATION';
    EXECUTE IMMEDIATE dyn_sql;
  END FOR;

  rs := (SELECT column_name, null_count FROM tmp_nulls ORDER BY column_name);
  RETURN TABLE(rs);
END;


/********************************Snowflake Scripting******************************************/  

/* 
    1- Blocks : We generaly use blocks to write a script in snowflake : 

        DECLARE
            -- (variable declarations, cursor declarations, etc.) ...
        BEGIN
            -- (Snowflake Scripting and SQL statements) ...
        EXCEPTION
            -- (statements for handling exceptions) ...
        END;

        -- Outside snowsight we use dollars : 
            EXECUTE IMMEDIATE 
                $$
                DECLARE
                profit number(38, 2) DEFAULT 0.0;
                BEGIN
                LET cost number(38, 2) := 100.0;
                LET revenue number(38, 2) DEFAULT 110.0;

                profit := revenue - cost;
                RETURN profit;
                END;
                $$
                ;
        -- Blocks can be nested and they don't share variables even if they have the same name
            CREATE OR REPLACE PROCEDURE duplicate_name(pv_name VARCHAR)
            RETURNS VARCHAR
            LANGUAGE SQL
            AS
            $$
            BEGIN
            DECLARE
                PV_NAME VARCHAR;
            BEGIN
                PV_NAME := 'middle block variable';
                DECLARE
                PV_NAME VARCHAR;
                BEGIN
                PV_NAME := 'innermost block variable';
                INSERT INTO names (v) VALUES (:PV_NAME);
                END;
                -- Because the innermost and middle blocks have separate variables
                -- named "pv_name", the INSERT below inserts the value
                -- 'middle block variable'.
                INSERT INTO names (v) VALUES (:PV_NAME);
            END;
            -- This inserts the value of the input parameter.
            INSERT INTO names (v) VALUES (:PV_NAME);
            RETURN 'Completed.';
            END;
            $$
            ;
    2- Variables : 
        -- We use LET, SET or := to assign a value varibales
        -- We call the variables using RETURN or inside an execute immediate or a sql query like insert
    
    3- Returning values :
        -- We can return a table or a single value SQL data type ! If inside SP we specify the type of the value we will return
            CREATE PROCEDURE ...
            RETURNS TABLE(...)
            ...
            RETURN TABLE(my_result_set);
            ...
    4- Conditional logic :
            EXECUTE IMMEDIATE $$
            BEGIN
            LET count := 1;
            IF (count < 0) THEN
                RETURN 'negative value';
            ELSEIF (count = 0) THEN
                RETURN 'zero';
            ELSE
                RETURN 'positive value';
            END IF;
            END;
            $$
            ;
    5- Loops :
        -- For loop: 
            -- With counter (We can use [ REVERSE ] to count in other way):
                EXECUTE IMMEDIATE $$
                DECLARE
                counter INTEGER DEFAULT 0;
                maximum_count INTEGER default 5;
                BEGIN
                FOR i IN 1 TO maximum_count DO
                    counter := counter + 1;
                END FOR;
                RETURN counter;
                END;
                $$
                ;
                -- We use it with insert also :
                    EXECUTE IMMEDIATE $$
                    DECLARE
                    counter INTEGER DEFAULT 0;
                    maximum_count INTEGER default 5;
                    BEGIN
                    CREATE OR REPLACE TABLE test_for_loop_insert(i INTEGER);
                    FOR i IN 1 TO maximum_count DO
                        INSERT INTO test_for_loop_insert VALUES (:i);
                        counter := counter + 1;
                    END FOR;
                    RETURN counter || ' rows inserted';
                    END;
                    $$
                    ;
            -- With Cursor :
                EXECUTE IMMEDIATE $$
                DECLARE
                total_price FLOAT;
                c1 CURSOR FOR SELECT price FROM invoices;
                BEGIN
                total_price := 0.0;
                FOR record IN c1 DO
                    total_price := total_price + record.price;
                END FOR;
                RETURN total_price;
                END;
                $$
                ;

            -- With resultsets :
                EXECUTE IMMEDIATE $$
                DECLARE
                total_price FLOAT;
                rs RESULTSET;
                BEGIN
                total_price := 0.0;
                rs := (SELECT price FROM invoices);
                FOR record IN rs DO
                    total_price := total_price + record.price;
                END FOR;
                RETURN total_price;
                END;
                $$
                ;
    
        -- While loop :
            EXECUTE IMMEDIATE $$
            BEGIN
            LET counter := 0;
            WHILE (counter < 5) DO
                counter := counter + 1;
            END WHILE;
            RETURN counter;
            END;
            $$
            ;
        -- Repeat:  
            EXECUTE IMMEDIATE $$
            BEGIN
            LET counter := 5;
            LET number_of_iterations := 0;
            REPEAT
                counter := counter - 1;
                number_of_iterations := number_of_iterations + 1;
            UNTIL (counter = 0)
            END REPEAT;
            RETURN number_of_iterations;
            END;
            $$
            ;
        -- Loop loop until break:
            EXECUTE IMMEDIATE $$
            BEGIN
            LET counter := 5;
            LOOP
                IF (counter = 0) THEN
                BREAK;
                END IF;
                counter := counter - 1;
            END LOOP;
            RETURN counter;
            END;
            $$
            ;
            -- Nested loops with labels :
                EXECUTE IMMEDIATE $$
                BEGIN
                LET inner_counter := 0;
                LET outer_counter := 0;
                LOOP
                    LOOP
                    IF (inner_counter < 5) THEN
                        inner_counter := inner_counter + 1;
                        CONTINUE OUTER;
                    ELSE
                        BREAK OUTER;
                    END IF;
                    END LOOP INNER; -- name of the inner loop is inner
                    outer_counter := outer_counter + 1;
                    BREAK;
                END LOOP OUTER; -- name of the outer loop is outer
                RETURN ARRAY_CONSTRUCT(outer_counter, inner_counter);
                END;
                $$;
    
    6- Cursors : 
        -- In FOR loops we don't need to open cursor
        -- In other cases we need to open it and close it :  
            EXECUTE IMMEDIATE $$
            DECLARE
            c1 CURSOR FOR SELECT * FROM LATERAL FLATTEN(INPUT=>[4,5,6]);
            ft NUMBER;
            BEGIN
            OPEN c1;
            FETCH c1 INTO ft; -- We use fetch to capture the current item of the cursor !
            IF (ft > 5) THEN
            RETURN TABLE(RESULTSET_FROM_CURSOR(c1));
            ELSE 
            RETURN TABLE(c1);
            END IF;
            CLOSE c1;
            END;
            $$
            ;
        -- We can use also bind parameters:
            EXECUTE IMMEDIATE $$
            DECLARE
            id INTEGER DEFAULT 0;
            minimum_price NUMBER(13,2) DEFAULT 22.00;
            maximum_price NUMBER(13,2) DEFAULT 33.00;
            c1 CURSOR FOR SELECT id FROM invoices WHERE price > ? AND price < ?;
            BEGIN
            OPEN c1 USING (minimum_price, maximum_price);
            FETCH c1 INTO id;
            RETURN id;
            END;
            $$
            ;

    7- Resutlsets:  
        -- A SQL data type that points to the result set of a query.
        -- we can : 
            Use the TABLE(...) syntax to retrieve the results as a table.
            Iterate over the RESULTSET with a cursor.
        DECLARE
        res RESULTSET;
        col_name VARCHAR;
        select_statement VARCHAR;
        BEGIN
        col_name := 'col1';
        select_statement := 'SELECT ' || col_name || ' FROM mytable';
        res := (EXECUTE IMMEDIATE :select_statement);
        RETURN TABLE(res);
        END;
        -- We can run asyncronous child jobs using resultsets also :
        CREATE OR REPLACE PROCEDURE test_sp_async_child_jobs_insert(
        arg1 INT,
        arg2 NUMBER(12,2),
        arg3 INT,
        arg4 NUMBER(12,2))
        RETURNS TABLE()
        LANGUAGE SQL
        AS
        $$
        BEGIN
        CREATE TABLE IF NOT EXISTS orders_q3_2024 (
            order_id INT,
            order_amount NUMBER(12,2));
            LET insert_1 RESULTSET := ASYNC (INSERT INTO orders_q3_2024 SELECT :arg1, :arg2);
            LET insert_2 RESULTSET := ASYNC (INSERT INTO orders_q3_2024 SELECT :arg3, :arg4);
            AWAIT insert_1;
            AWAIT insert_2;
            LET res RESULTSET := (SELECT * FROM orders_q3_2024 ORDER BY order_id);
            RETURN TABLE(res);
        END;
        $$;
    8- Exeptions
        -- We can either declare our own exeptions and raise them   
            EXECUTE IMMEDIATE $$
            DECLARE
            my_exception EXCEPTION (-20002, 'Raised MY_EXCEPTION.');
            BEGIN
            LET counter := 0;
            LET should_raise_exception := true;
            IF (should_raise_exception) THEN
                RAISE my_exception;
            END IF;
            counter := counter + 1;
            RETURN counter;
            END;
            $$
            ; 
        -- Use also EXCEPTION in a custom way : 
            EXECUTE IMMEDIATE $$
            DECLARE
            counter_val INTEGER DEFAULT 0;
            my_exception EXCEPTION (-20002, 'My exception text');
            BEGIN
            WHILE (counter_val < 12) DO
                counter_val := counter_val + 1;
                IF (counter_val > 10) THEN
                RAISE my_exception;
                END IF;
            END WHILE;
            RETURN counter_val;
            EXCEPTION
            WHEN my_exception THEN
                RETURN 'Error ' || sqlcode || ': Counter value ' || counter_val || ' exceeds the limit of 10.';
            END;
            $$
            ;     
        -- We can also handel exeptions using the EXCEPTION block and populate an error log table !
            DECLARE
                my_exception EXCEPTION (-20002, 'Raised MY_EXCEPTION.');
            BEGIN
            -- SELECT 1/0;
            -- LET var := 1/0;
            LET counter := 0;
            LET should_raise_exception := true;
            IF (should_raise_exception) THEN
                RAISE my_exception;
            END IF;
            counter := counter + 1;
            RETURN counter;
            EXCEPTION
            WHEN STATEMENT_ERROR THEN
                INSERT INTO test_error_log VALUES(
                'STATEMENT_ERROR', :sqlcode, :sqlerrm, :sqlstate, CURRENT_TIMESTAMP());
                RETURN OBJECT_CONSTRUCT('Error type', 'STATEMENT_ERROR',
                                        'SQLCODE', sqlcode,
                                        'SQLERRM', sqlerrm,
                                        'SQLSTATE', sqlstate);
            WHEN my_exception THEN
                INSERT INTO test_error_log VALUES(
                'MY_EXCEPTION', :sqlcode, :sqlerrm, :sqlstate, CURRENT_TIMESTAMP());
                RETURN OBJECT_CONSTRUCT('Error type', 'MY_EXCEPTION',
                                        'SQLCODE', sqlcode,
                                        'SQLERRM', sqlerrm,
                                        'SQLSTATE', sqlstate);
            WHEN OTHER THEN
                INSERT INTO test_error_log VALUES(
                'OTHER', :sqlcode, :sqlerrm, :sqlstate, CURRENT_TIMESTAMP());
                RETURN OBJECT_CONSTRUCT('Error type', 'Other error',
                                        'SQLCODE', sqlcode,
                                        'SQLERRM', sqlerrm,
                                        'SQLSTATE', sqlstate);
            END;

*/  

