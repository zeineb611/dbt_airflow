-- models/fact_Opp.sql

{{ config(
    materialized='table',
    unique_key='fact_id',
    schema=generate_schema_name('consumption_zone', this)
) }}


WITH source_data AS (
    SELECT 
        o.opportunityID ,
        g.geographicID AS GeographicID,
        c.clientID AS ClientID,
        d.dateID AS dateID ,
        e.employeeID AS EmployeeID,
        oo.Proba_Of_Win AS ProbaOfWin,
        oo.Total_Value_TND AS TotalvalueTND,
        oo.Total_Value_TND / oo.ValueOFcurrency AS Totalvalue,
        ROW_NUMBER() OVER (ORDER BY oo.Opportunity_ID) AS fact_id
    FROM 
          {{ source('curated_zone', 'opportunity') }} oo
    JOIN 
        {{ ref('dim_employee') }} e 
        ON oo.Identified_By_Name = e.Identified_By_Name
           AND oo.Pursuit_Leader_Name = e.Pursuit_Leader_Name
           AND oo.Managed_By_Name = e.Managed_By_Name
    JOIN 
        {{ ref('dim_date') }} d 
        ON oo.Effective_Open_Date = d.Effective_Open_Date
        AND oo.Anticipated_Win_Date = d.Anticipated_Win_Date
        
         
          
    JOIN 
        {{ ref('dim_opportunity') }} o
        ON oo.Opportunity_ID = o.OPPORTUNITY_ID
      
             
    JOIN 
        {{ ref('dim_geographic') }} g 
        ON oo.Country = g.Country 
           AND oo.Region = g.Region
    JOIN 
        {{ ref('dim_client') }} c 
        ON oo.Account_Name = c.AccountName

     
)

SELECT *
FROM source_data
