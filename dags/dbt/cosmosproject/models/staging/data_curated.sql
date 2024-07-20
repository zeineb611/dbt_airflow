{{ config(
    materialized='table',
    schema=generate_schema_name('curated_zone', this)
     
) }}

with source_data as (
    select
        Opportunity_Service_Line,
        Client_Sector,
        Account_Name,
        Prospect_Client_Name,
        Opportunity_Name,
        Stage,
        Proba_Of_Win,
        TO_DATE(Effective_Open_Date) as Effective_Open_Date,
        TO_DATE(Anticipated_Win_Date) as Anticipated_Win_Date,
        TO_DATE(Effective_Close_Date) as Effective_Close_Date,
        Opportunity_Partner_Name,
        Pursuit_Leader_Name,
        Managed_By_Name,
        Pursuit_Team_Name,
        Identified_By_Name,
        Currency,
        CAST(REPLACE(Total_Value_TND, ',', '.') AS DECIMAL(10,3)) as Total_Value_TND,
        Opportunity_Sub_SL,
        Original_Opportunity_ID,
        Opportunity_ID,
        MS,
        SSL1,
        Country,
        Region
    from {{ source('landing_zone', 'data') }}
)

select *
from source_data

