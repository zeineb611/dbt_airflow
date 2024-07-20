
{{ config(
    materialized='incremental',
    unique_key='employeeID',
    schema=generate_schema_name('consumption_zone', this)
) }}

WITH source_data AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['Pursuit_Leader_Name','Managed_By_Name']) }} as employeeID,
       Pursuit_Leader_Name,Managed_By_Name ,
       Pursuit_Team_Name ,
       Identified_By_Name
      
   from {{ source('curated_zone', 'opportunity') }}
)

SELECT *
FROM source_data