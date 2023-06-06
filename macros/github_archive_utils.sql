{% macro year_range(from_year=2016, to_year=2023) %}
  {% for i in range(from_year, to_year) %}
    SELECT * FROM githubarchive.year.{{ i }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro month_range(from_month=202301, to_month=202305) %}
  {% for i in range(from_month, to_month) %}
    SELECT * FROM githubarchive.month.{{ i }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro day_range(from_day, to_day) %}
   {% set cur = from_day %}
   {% for _ in range((to_day - from_day).days + 1) %}
    SELECT * FROM githubarchive.day.{{ cur.strftime("%Y%m%d") }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% set cur = cur + modules.datetime.timedelta(1) %}
  {% endfor %}
{% endmacro %}
