{% macro team_attributes(alias="A", include_team_id=True, include_core=True, include_mau_rank=True) %}
    {% if include_team_id %}
    {{ alias }}.team_id,
    {% endif %}
    {{ alias }}.team_name,
    {{ alias }}.team_hash,
    {{ alias }}.team_is_deleted,
    {{ alias }}.team_billing_status,
    {{ alias }}.created_dttm AS team_created_dttm,
    {{ alias }}.team_billing_status_derived,
    {{ alias }}.is_preset,
    {{ alias }}.hs_company_id,
    {{ alias }}.hs_company_name,
    {{ alias }}.hs_company_state,
    {% if include_mau_rank %}
    {{ alias }}.mau_rank,
    {{ alias }}.mau_rank_group,
    {% endif %}
    {{ alias }}.hs_deal_stage,
    {{ alias }}.hs_company_customer_type,
    {{ alias }}.hs_company_db_created,
    {{ alias }}.hs_company_deal_pipeline_name,
    {{ alias }}.hs_company_ce_status,
    {{ alias }}.hs_company_owner_id,
    {{ alias }}.hs_company_last_activity_date,
    {{ alias }}.team_creation_era,
    {{ alias }}.is_duplicate_team,
    {% if include_core %}
    {{ alias }}.is_activated AS team_is_activated,
    {{ alias }}.activation_score AS team_activation_score,
    {% endif %}
{% endmacro %}

{% macro workspace_attributes(alias="A") %}
    {{ alias }}.workspace_id,
    {{ alias }}.workspace_hash,
    {{ alias }}.workspace_title,
    {{ alias }}.workspace_hostname,
    {{ alias }}.workspace_region,
    {{ alias }}.last_accessed_at,
{% endmacro %}

{% macro coalesce_2(field, alias1="Z1", alias2="Z2", generate_alias=True) %}
    COALESCE({{ alias1 }}.{{ field }}, {{ alias2 }}.{{ field }}){%if generate_alias%} AS {{ field }}{% endif %}
{% endmacro %}

{% macro database_engine_fields(engine_field, alias="A") %}
    {#
        takes input as "mysql" or "mysql+pymysql", and break it down into two fields
        `database_engine` taking everything before the `+`and `database_driver`
        preserves the full details.
    #}
    {% set field = alias + "." + engine_field %}
    CASE
      WHEN INSTR({{ field }}, '+') > 0 THEN SUBSTR({{ field }}, 0, INSTR({{ field }}, '+')-1)
      ELSE {{ field }}
    END AS database_engine,
    {{ field }} AS database_driver,
{% endmacro %}

{% macro latest_id_cross_partition(source_table, dttm_column="ds") %}
    SELECT A.*
    FROM {{ source_table }} A
    JOIN (
          SELECT id, workspace_id, MAX(ds) AS max_ds
          FROM {{ source_table }}
          WHERE ds >=DATE('1970-01-01')
          GROUP BY 1, 2
    ) B ON A.id = B.id AND A.workspace_id = B.workspace_id AND A.ds = B.max_ds
    WHERE A.ds >= DATE('1970-01-01')
{% endmacro %}

{% macro growth_accounting_case_expr(dt_expr, days, framing="yesterday") %}
    CASE
        WHEN DATE_DIFF({{ dt_expr }}, B.first_event, DAY) BETWEEN 0 AND {{ days - 1 }} THEN 'new'
        WHEN {{ framing }}_l{{ days }} > 0 AND l{{ days }} > 0 THEN 'retained'
        WHEN {{ framing }}_l{{ days }} > 0 AND l{{ days }} = 0 THEN 'churned'
        WHEN {{ framing }}_l{{ days }} = 0 AND l{{ days }} > 0 THEN 'resurected'
        WHEN {{ framing }}_l{{ days }} = 0 AND l{{ days }} = 0 THEN 'passive'
        ELSE 'NA'
    END
{% endmacro %}

{% macro rem_across(var_list, script_string, final_comma) %}
{#
  At some point in the future we should make this macro more extensible by
  turning the aliasing into an option or whatever
#}
  {% for v in var_list %}
  {{v}} as {{ v | replace(script_string, '') }}
  {%- if not loop.last %},{% endif %}
  {%- if loop.last and final_comma|default(false) %},{% endif %}
  {% endfor %}

{% endmacro %}

{% macro is_generic_email_condition(field='primary_email_domain') %}
  ({{ field }} IN ('gmail.com', 'hotmail.com', 'outlook.com', '126.com', '163.com') OR
  {{ field }} LIKE '%.yahoo.%')
{% endmacro %}

{% macro fiscal_quarter(date_field) %}
  FORMAT_DATE("%Y Q%Q", DATE_ADD({{ date_field }}, INTERVAL -1 MONTH))
{% endmacro %}

{% macro format_quarter(dt) %}
EXTRACT(QUARTER FROM DATE({{ dt }})) || 'Q' || EXTRACT(YEAR FROM DATE({{ dt }}))
{% endmacro %}
