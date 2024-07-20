{{ config(
    materialized='incremental',
    unique_key='clientID',
    schema=generate_schema_name('consumption_zone', this)
) }}

WITH source_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['Account_Name', 'Client_Sector']) }} as clientID,
        Account_Name AS AccountName,
        Client_Sector AS ClientSector
    FROM {{ source('curated_zone', 'opportunity') }}
)

SELECT *
FROM source_data
