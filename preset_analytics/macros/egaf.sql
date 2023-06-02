
{% macro case_for_ga_status(field) %}
    CASE
        WHEN A.{{ field }} = 'churned' AND NOT is_activated THEN 'churn_no_activation'
        ELSE A.{{ field }}
    END AS {{ field }}
{% endmacro %}
