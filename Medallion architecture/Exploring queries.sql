/**************************************************************************
    This file is reserved for queries to explore and test transformations
 **************************************************************************
*/

/**************************************************************************
                             Menu table
**************************************************************************/                            
USE ROLE DATA_ENGINEER;
SELECT * FROM TASTY_BYTES.RAW.RAW_COUNTRY;

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
