{% macro bool_to_string(field) %}
CASE WHEN {{ field }} IS TRUE THEN 'TRUE' ELSE 'FALSE' END
{% endmacro %}
