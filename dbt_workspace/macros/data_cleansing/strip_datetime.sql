{%- macro strip_datetime(column_name) -%}
  REGEXP_REPLACE(
    {{ column_name }},
    r'\d{4}-\d{2}-\d{2}|' || -- YYYY-MM-DD dates
    r'\d{2}-\d{2}-\d{4}|' || -- DD-MM-YYYY dates
    , ''
  )
{%- endmacro %}
