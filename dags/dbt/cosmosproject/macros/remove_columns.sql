-- macros/remove_columns.sql

{% macro remove_columns(table_name) %}
    {# List of SQL statements to drop columns #}
    {%- set drop_columns_sql = [
     
        "ALTER TABLE " ~ table_name ~ " DROP COLUMN Total_Value_Of_Potential_Sale;",
        "ALTER TABLE " ~ table_name ~ " DROP COLUMN RELATED_ENGAGEMENTS;",
        "ALTER TABLE " ~ table_name ~ " DROP COLUMN PACE_STATUS;"
    ] -%}

    {# Joining the SQL statements with newline characters #}
    {{- drop_columns_sql | join("\n") -}}
{% endmacro %}