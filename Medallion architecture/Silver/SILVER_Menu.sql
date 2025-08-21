
/**************************************************************************
            This file is reserved for transformations queries 
 **************************************************************************
*/

-- We will simply flatten the semi structured data and specify the types

USE ROLE DATA_ENGINEER;
CREATE TABLE TASTY_BYTES.SILVER.SILVER_MENU AS 
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
LATERAL FLATTEN (input => menu_item_health_metrics_obj:menu_item_health_metrics) lf;


SELECT * FROM TASTY_BYTES.SILVER.SILVER_MENU;