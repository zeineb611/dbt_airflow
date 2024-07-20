-- macros/add_and_update_column.sql

{% macro add_and_update_column(table_name) %}
  {% set statements = [] %}

  {% set add_column_statement = "ALTER TABLE " ~ table_name ~ " ADD COLUMN IF NOT EXISTS ValueOFcurrency DECIMAL(10, 3);" %}
  {% set update_column_statement = (
    "UPDATE " ~ table_name ~ " SET ValueOFcurrency = CASE " ~
    "WHEN Currency = 'EUR' THEN 3.40 " ~
    "WHEN Currency = 'USD' THEN 3.12 " ~
    "WHEN Currency = 'CAD' THEN 2.29 " ~
    "ELSE 1 END;"
  ) %}

  {% do statements.append(add_column_statement) %}
  {% do statements.append(update_column_statement) %}

  {% for statement in statements %}
    {{ statement }}
  {% endfor %}
{% endmacro %}
