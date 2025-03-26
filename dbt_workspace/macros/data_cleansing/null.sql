{% macro to_null(column_name) %}
  CAST(NULLIF({{column_name}}, '-') AS FLOAT64)
{% endmacro %}