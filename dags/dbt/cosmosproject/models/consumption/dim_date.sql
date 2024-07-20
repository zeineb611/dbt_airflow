{{ config(
    materialized='incremental',
    unique_key='dateID',
    schema=generate_schema_name('consumption_zone', this)
) }}

WITH source_data AS (
    SELECT
        COALESCE(CAST(Effective_Open_Date AS VARCHAR), '') || '_' ||
        COALESCE(CAST(Anticipated_Win_Date AS VARCHAR), '')AS dateID_combination,
        Effective_Open_Date,
        Anticipated_Win_Date,
        Effective_Close_Date
     
    FROM {{ source('curated_zone', 'opportunity') }}
)

SELECT
    md5(dateID_combination) as dateID,
    Effective_Open_Date,
    Anticipated_Win_Date,
    Effective_Close_Date
FROM source_data
