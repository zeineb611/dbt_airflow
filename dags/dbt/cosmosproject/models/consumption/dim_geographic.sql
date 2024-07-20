
{{ config(
    materialized='incremental',
    unique_key='geographicID',
    schema=generate_schema_name('consumption_zone', this)
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['Country']) }} AS geographicID,
        Country,
        region
    FROM 
        {{ source('curated_zone', 'opportunity') }}
    GROUP BY
        Country,
        region
      
)

SELECT *
FROM source_data
