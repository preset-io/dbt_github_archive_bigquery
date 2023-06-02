{% macro preset_scrape_model_prep(
    source_table,
    dttm_column='ds',
    pk_fields=['id'],
    dedup_override=false,
    do_model='incr',
    do_primary_key='id',
    do_order_by_key='ds',
    do_except=true
) %}
  {#
    This macro prepares a source from the daily manager / superset scrape for downstream use.
    NOTE: The latest partition of hibernated workspaces are propogated until the next scrape is received.

    Inputs:
      - source_table = name of the source table in dbt source syntax
      - dttm_column = name of the date column in the source table
      - pk_fields = list of the primary keys of the source table

    Step by step of logic:
      1. create a datespine
      2. format source table and remove partition restrictions with date filter.
      3. apply datespine date filter on source table through inner join
      4. deduplicate the results
      5. find the greatest partition for each primary key
      6. fill gaps in datespine with greatest partition for each pk
  #}

  WITH date_spine AS (
    SELECT
      dt AS {{ dttm_column }},
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this, this_date_col=dttm_column) }}
    {% endif %}
  ),

  tbl AS (
    SELECT *
    FROM {{ source_table }}
    WHERE {{ dttm_column }} > '1970-01-01'
  ),

  incr AS (
    SELECT tbl.*
    FROM tbl
    INNER JOIN date_spine
      ON date_spine.ds = tbl.{{ dttm_column }}

  ),

  dedup AS (
    {% if dedup_override %}
      {{ generate_deduped_src(model=do_model, primary_key=do_primary_key, order_by_key=do_order_by_key) }}
    {% else %}
      {{ generate_deduped_src(model='incr', primary_key=dttm_column ~ ', ' ~ pk_fields|join(', '), order_by_key=dttm_column) }}
    {% endif %}
  ),

  partition_for_day AS (
    SELECT
      date_spine.{{ dttm_column }},
      {% for field in pk_fields %}
      dedup.{{ field }},
      {% endfor %}
      MAX(dedup.{{ dttm_column }}) AS max_partition,
    FROM date_spine
    LEFT JOIN dedup
      ON date_spine.{{ dttm_column }} >= dedup.{{ dttm_column }}
    GROUP BY
      date_spine.{{ dttm_column }}
      {% for field in pk_fields %}
        , dedup.{{ field }}
      {% endfor %}
  ),

  propogate_hibernated_workspaces AS (
    SELECT
      partition_for_day.{{ dttm_column }},
      {% for field in pk_fields %}
      partition_for_day.{{ field }},
      {% endfor %}

      {% if do_except %}
      dedup.* EXCEPT ({{ dttm_column ~ ', ' ~ pk_fields|join(', ') }}),
      {% endif %}
      dedup.{{ dttm_column }} != partition_for_day.max_partition AS is_hibernated
    FROM partition_for_day
    INNER JOIN dedup
      ON partition_for_day.max_partition = dedup.{{ dttm_column }}
      {% for field in pk_fields %}
        AND partition_for_day.{{ field }} = dedup.{{ field }}
      {% endfor %}
  )

  SELECT * FROM propogate_hibernated_workspaces

{% endmacro %}
