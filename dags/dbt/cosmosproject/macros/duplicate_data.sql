
{% macro duplicate_data(table_name) %}
WITH duplicate_data AS (
    SELECT 
        OPPORTUNITY_ID, 
         TOTAL_VALUE_TND,
        ROW_NUMBER() OVER (PARTITION BY OPPORTUNITY_ID ORDER BY  Total_Value_TND ASC NULLS LAST) AS row_num
    FROM {{ table_name }}
)
DELETE FROM {{ table_name }}
WHERE (OPPORTUNITY_ID,  TOTAL_VALUE_TND) IN (
    SELECt duplicate_data
    WHERE row_num > 1
);
{% endmacro %}
