
{{ config(
     materialized='incremental',
     unique_key='opportunityID',
     schema=generate_schema_name('consumption_zone', this)
) }}

WITH source_data AS (
    SELECT
      {{ dbt_utils.generate_surrogate_key(['OPPORTUNITY_ID']) }} as opportunityID ,
      OPPORTUNITY_ID,
      Opportunity_Name,
      Opportunity_Partner_Name,
      Stage,
      Opportunity_Service_Line,
      Currency,
      MS,
      SSL1
   from {{ source('curated_zone', 'opportunity') }}
)

SELECT *
FROM source_data