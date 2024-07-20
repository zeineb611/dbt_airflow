-- models/staging/deduplicated_data.sql

{{ config(
    materialized='table',
    snowflake_warehouse='load_data1',
     schema=generate_schema_name('curated_zone', this)
) }}

WITH deduplicated_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY Opportunity_ID ORDER BY Total_Value_TND ASC NULLS LAST) AS row_num
    FROM {{ ref('data_curated') }}
)

SELECT *
FROM deduplicated_data
WHERE row_num = 1
