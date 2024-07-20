{% macro update(table_name) %}
{% set sql %}
-- Perform the update operation on the specified model
UPDATE {{ table_name }}  -- Use model_name directly instead of ref()
SET MS = CASE
    WHEN CLIENT_SECTOR = 'startup' THEN 'private'
    WHEN CLIENT_SECTOR IN ('Technology', 'Media & Entertainment', 'Telecommunications') THEN 'TMT'
    WHEN CLIENT_SECTOR IN ('Wealth & Asset Management', 'Banking & Capital Markets', 'Insurance') THEN 'FS'
    WHEN CLIENT_SECTOR = 'Defense' THEN 'GPS'
    ELSE MS
  END
WHERE CLIENT_SECTOR IN ('startup', 'Technology', 'Media & Entertainment', 
'Telecommunications', 'Wealth & Asset Management', 'Banking & Capital Markets', 'Insurance', 'Defense');
{% endset %}

-- Log the SQL for debugging purposes
{{ log(sql, info=True) }}

-- Execute the SQL
{{ return(sql) }}
{% endmacro %}
