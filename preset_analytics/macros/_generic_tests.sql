{% test latest_partition_is_biggest(model, column_name="ds", severity='error') %}
    {# Using in `tests/` to make sure the latest partition is the biggest #}

    {{ config(severity=severity)}}

    {% set latest = latest_dt(model, dttm_column=column_name) %}
    SELECT * FROM (
        SELECT
        (
            -- Getting the HIGHEST row count
            SELECT COUNT(1)
            FROM {{ model }}
            WHERE {{ column_name }} >= '1970-01-01'
                {# FILTERING OUT BAD DOUBLE LOAD ON 2022-05-04 AND 2022-06-06 #}
                AND {{ column_name }} <> DATE('2022-05-04')
                AND {{ column_name }} <> DATE('2022-06-06')
                AND {{ column_name }} <= DATE( '{{ latest }}' ) -- only compare full partitions
            GROUP BY {{ column_name }}
            ORDER BY 1 DESC
            LIMIT 1
        ) AS max_ds_count,
        (
            -- Getting the LATEST row count
            SELECT COUNT(1)
            FROM {{ model }}
            WHERE {{ column_name }} >= '1970-01-01'
                AND {{ column_name }} <= DATE( '{{ latest }}' ) -- only compare full partitions
            GROUP BY {{ column_name }}
            ORDER BY {{ column_name }} DESC
            LIMIT 1
        ) AS latest_ds_count,
    )
    WHERE
        latest_ds_count < max_ds_count
{% endtest %}

{% test count_within_threshold(model, column_name="ds", threshold_value=1, latest_only=True, allow_gaps=True, calc_off_max=False) %}
    {# Using in `tests/` to make daily loads are within an acceptable count range #}

    WITH row_counts_int AS (
        SELECT
            {{ column_name }} AS dt_col,
            ROW_NUMBER() OVER (ORDER BY {{ column_name }}) AS rn,
            COUNT(*) AS cnt
        FROM {{ model }}
        -- a date filter is need for partitioned tables in BQ
        WHERE {{ column_name }} > '2020-01-01'
        GROUP BY 1
    ),

    row_counts AS (
        SELECT
            ds.dt AS dt_col,
            rc.rn,
            COALESCE(rc.cnt, 0) AS cnt
        FROM {{ ref('date_spine') }} AS ds
        {{ 'INNER' if allow_gaps else 'LEFT' }} JOIN row_counts_int AS rc
            ON ds.dt = rc.dt_col
    ),

    max_row_count AS (
        SELECT * FROM row_counts WHERE cnt = (SELECT MAX(cnt) FROM row_counts)
    ),

    count_ratios AS (
        SELECT
            rc.dt_col,
            rc.cnt,
            rc_sub1.cnt AS cnt_sub1,
            {% set calc_denom = '(SELECT cnt FROM max_row_count)' if calc_off_max else 'rc_sub1.cnt'  %}
            rc.cnt / {{ calc_denom }} AS ratio
        FROM row_counts AS rc
        {% set join_col = 'rn' if allow_gaps else 'dt_col'  %}
        LEFT JOIN row_counts AS rc_sub1
            ON rc.{{ join_col }} = rc_sub1.{{ join_col }} + 1
        {% if latest_only %}
        WHERE rc.rn = (SELECT MAX(rn) FROM row_counts)
        {% endif %}
    )

    SELECT COUNT(*)
    FROM count_ratios
    WHERE ratio < {{ threshold_value }} OR ratio IS NULL
    HAVING COUNT(*) > 0
{% endtest %}

{% test ptc_null(model, column_name, allowable_ptc_null) %}
    -- tests column for percentage of rows with nulls
    -- allowable_ptc_null is in percentage

    WITH null_count AS (
        SELECT count(*) AS null_cnt
        FROM {{ model }}
        WHERE {{ column_name }} IS NULL
    ),

    tot_count AS (
        SELECT count(*) AS tot_cnt
        FROM {{ model }}
    )

    SELECT *
    FROM tot_count
    CROSS JOIN null_count
    WHERE (SAFE_DIVIDE(null_cnt, tot_cnt) * 100) > {{ allowable_ptc_null }}

{% endtest %}
