{% macro calculate_difference(column1, column2) %}
    {{ column1 }} - {{ column2 }}
{% endmacro %}