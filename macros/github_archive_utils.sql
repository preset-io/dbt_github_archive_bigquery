{% macro year_range(from_year=2016, to_year=2023) %}
  {% for i in range(from_year, to_year) %}
    SELECT * FROM githubarchive.year.{{ i }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro month_range(from_month=202301, to_month=202305) %}
  {%- set start_year = (from_month|string)[:4]|int -%}
  {%- set end_year = (to_month|string)[:4]|int -%}
  {%- set start_month = (from_month|string)[4:6]|int -%}
  {%- set end_month = (to_month|string)[4:6]|int -%}
  {%- set last_query = end_year * 100 + end_month -%}

  {% for year in range(start_year, end_year + 1) %}
    {%- set loop_start_month = start_month if loop.first else 1 -%}
    {%- set loop_end_month = end_month if loop.last else 13 -%}
    
    {% for month in range(loop_start_month, loop_end_month) %}
      SELECT * FROM githubarchive.month.{{ '%04d%02d' % (year, month) }}
      {% if not year * 100 + month == last_query %}
      UNION ALL
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endmacro %}

{% macro day_range(from_day, to_day) %}
   {% set cur = from_day %}
   {% for _ in range((to_day - from_day).days + 1) %}
    SELECT * FROM githubarchive.day.{{ (cur + modules.datetime.timedelta(_)).strftime("%Y%m%d") }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% endmacro %}
