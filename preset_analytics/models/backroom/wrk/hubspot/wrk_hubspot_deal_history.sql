{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

WITH date_spine AS (
  SELECT
    dt
    , DATE_ADD(dt, INTERVAL 1 DAY) AS next_dt
  FROM {{ ref('date_spine') }}
  {% if is_incremental() %}
  WHERE {{ generate_incremental_statement(this, this_date_col='dt') }}
  {% endif %}
),

timeline AS (
-- A summary per deal_id of when each stage was entered
	SELECT
		deal_id,
		MIN(CASE WHEN deal_stage_short IN ('stage0', 'stage1', 'stage2', 'stage3', 'stage4', 'stage5', 'stage6') THEN stage_start_dttm END ) AS timeline_entered_stage0,
		MIN(CASE WHEN deal_stage_short IN ('stage1', 'stage2', 'stage3', 'stage4', 'stage5', 'stage6') THEN stage_start_dttm END ) AS timeline_entered_stage1,
		MIN(CASE WHEN deal_stage_short IN ('stage2', 'stage3', 'stage4', 'stage5', 'stage6') THEN stage_start_dttm END ) AS timeline_entered_stage2,
		MIN(CASE WHEN deal_stage_short IN ('stage3', 'stage4', 'stage5', 'stage6') THEN stage_start_dttm END ) AS timeline_entered_stage3,
		MIN(CASE WHEN deal_stage_short IN ('stage4', 'stage5', 'stage6') THEN stage_start_dttm END ) AS timeline_entered_stage4,
		MIN(CASE WHEN deal_stage_short IN ('stage5', 'stage6') THEN stage_start_dttm END ) AS timeline_entered_stage5,
		MIN(CASE WHEN deal_stage_short IN ('stage6') THEN stage_start_dttm END ) AS timeline_entered_stage6,
		MIN(CASE WHEN deal_stage_short IN ('won') THEN stage_start_dttm END ) AS timeline_entered_won,
		MIN(CASE WHEN deal_stage_short IN ('lost') THEN stage_start_dttm END ) AS timeline_entered_lost,
	FROM {{ ref('wrk_hubspot_deal_stage_history') }}
	GROUP BY deal_id
),

deal_history AS (
  SELECT
    ds.dt,
    ds.next_dt,

    -- fields from `deal`
    a.deal_id,
    a.dealname AS deal_name,
    g.preset_team_id AS preset_team_id,
    CASE WHEN LOWER(a.dealtype) LIKE '%expansion%' THEN 'expansion' ELSE a.dealtype END AS deal_type,
    DATE(a.closedate) AS deal_close_date,
    a.how_do_they_want_to_use_bi_ AS primary_use_case,
    a.secondary_use_case AS secondary_use_case,
    DATE(a.createdate) AS deal_created_date,

    -- from deal_stage_history CTE
    c.deal_stage_label AS deal_stage,
    c.deal_stage_short,
    c.deal_stage_code,
    c.deal_stage_order,
    c.deal_stage_probability,
    c.stage_start_dttm,
    c.stage_end_dttm,
    c.deal_pipeline_label AS pipeline,

    --historical amount
    CAST(amount.new_value AS NUMERIC) AS deal_amount,
    CAST(hs_predicted_amount.new_value AS NUMERIC) AS hs_predicted_amount,

    --historical seats
    SAFE_CAST(number_of_seats.new_value AS NUMERIC) AS number_of_seats,
    CASE
      WHEN c.deal_stage_code = '9979814' /*stage=Closed Won*/
        AND c.deal_pipeline_label = 'Growth' /*hs pipeline*/
      THEN CAST(amount.new_value AS NUMERIC)
    END AS growth_arr,
    CASE
      WHEN ds.dt >= DATE('2022-03-18')
        AND c.deal_stage_code NOT IN (
          '16506026', --stage=Closed Won
          '16506027' --stage=Closed Lost
        )
        AND c.deal_pipeline_label = 'Renewals' /*hs pipeline*/
          THEN CAST(target_amount_to_renew.new_value AS NUMERIC)

      -- We changed data entry in Hubspot on 2022-03-18,
      -- before that day is an approximation
      WHEN ds.dt < DATE('2022-03-18')
        AND c.deal_stage_code = '9979814' /*stage=Closed Won*/
        AND c.deal_pipeline_label = 'Growth' /*hs pipeline*/
          THEN CAST(amount.new_value AS NUMERIC)
      END AS arr,
    CASE
      WHEN c.deal_stage_code = '9979814' /*stage=Closed Won*/
        AND c.deal_pipeline_label = 'Growth' /*hs pipeline*/
          THEN SAFE_CAST(number_of_seats.new_value AS NUMERIC)
      END AS sold_seats,

    -- licenses
    CAST(number_of_creator_licenses.new_value AS NUMERIC) AS number_of_creator_licenses,
    CAST(number_of_viewer_licenses.new_value AS NUMERIC) AS number_of_viewer_licenses,
    CAST(number_of_embedded_view_licenses.new_value AS NUMERIC) AS number_of_embedded_view_licenses,

    -- company
    g.company_id,
    g.name AS company_name,
    g.type AS company_type,

    -- deal creator
    CONCAT(i.first_name, ' ', i.last_name) AS deal_creator,
    i.first_name AS deal_creator_first_name,
    i.email_address AS deal_creator_email,

    -- deal owner
    CONCAT(j.first_name, ' ', j.last_name) AS owner,
    j.first_name AS owner_first_name,
    j.email_address AS owner_email,

    a.close_lost_details AS close_lost_details,
    a.close_lost_reason AS close_lost_reason,
    a.closed_won_reason AS closed_won_reason,
    hs_manual_forecast_category.new_value AS forecast_category,
    hs_forecast_probability.new_value AS deal_forecast_probabily,
    {{ fiscal_quarter('DATE(a.closedate)') }} AS fiscal_quarter_close,
    {{ fiscal_quarter('DATE(a.createdate)') }} AS fiscal_quarter_created,

    DATE(a.contract_start_date) AS contract_start_date,
    DATE(a.contract_end_date) AS contract_end_date,
  FROM date_spine AS ds
  LEFT JOIN {{ ref('hubspot__deals') }} AS a
    ON DATE(ds.dt) >= DATE(a.createdate)
  LEFT JOIN {{ ref('stg_hubspot__deal_company') }} AS h
    ON a.deal_id = h.deal_id
  LEFT JOIN {{ ref('hubspot__companies') }} AS g
    ON h.company_id = g.company_id
  LEFT JOIN {{ ref('wrk_hubspot_deal_stage_history') }} AS c
    ON a.deal_id = c.deal_id
  -- deal property history (
  {% set deal_property_fields = [
    'number_of_seats',
    'amount',
    'target_amount_to_renew',
    'hs_predicted_amount',
    'owner_id',
    'hs_manual_forecast_category',
    'hs_forecast_probability',
    'created_by_sdr_',
    'number_of_creator_licenses',
    'number_of_viewer_licenses',
    'number_of_embedded_view_licenses',
  ] %}
  {% for field in deal_property_fields %}
  LEFT JOIN {{ ref('hubspot__deal_history') }} AS {{ field }}
    ON {{ field }}.field_name = '{{ field }}'
      AND a.deal_id = {{ field }}.deal_id
      AND ds.next_dt >= DATE({{ field }}.valid_from)
      AND ds.next_dt < COALESCE(DATE({{ field }}.valid_to), DATE('9999-01-01'))
  {% endfor %}
  -- ) deal property history
  LEFT JOIN {{ ref('stg_hubspot__owner') }} AS i
    ON SAFE_CAST(created_by_sdr_.new_value AS NUMERIC) = i.owner_id
  LEFT JOIN {{ ref('stg_hubspot__owner') }} AS j
    ON SAFE_CAST(owner_id.new_value AS NUMERIC) = j.owner_id
  WHERE NOT a.is_deleted
    AND NOT g.is_deleted
),

windowed AS (
  SELECT
    *,
    LAG(deal_stage) OVER (PARTITION BY dt, deal_id ORDER BY stage_start_dttm ASC) AS previous_deal_stage,
    LAG(deal_close_date) OVER (PARTITION BY dt, deal_id ORDER BY stage_start_dttm ASC) AS previous_deal_close_date,
    LAG(deal_stage_short) OVER (PARTITION BY dt, deal_id ORDER BY stage_start_dttm ASC) AS previous_deal_stage_short,
    LAG(deal_stage_code) OVER (PARTITION BY dt, deal_id ORDER BY stage_start_dttm ASC) AS previous_deal_stage_code,
    LAG(stage_start_dttm) OVER (PARTITION BY dt, deal_id ORDER BY stage_start_dttm ASC) AS previous_stage_start_dttm,
    LAG(deal_stage_order) OVER (PARTITION BY dt, deal_id ORDER BY stage_start_dttm ASC) AS previous_deal_stage_order,
  FROM deal_history
)

SELECT
  A.* EXCEPT(next_dt),
  CASE
    WHEN A.deal_stage_short != A.previous_deal_stage_short AND A.deal_stage_short IN ('won', 'lost') THEN A.deal_stage_short
    WHEN A.previous_deal_stage IS NULL THEN 'new'
    WHEN A.previous_deal_stage_order < A.deal_stage_order THEN 'promotion'
    WHEN A.previous_deal_stage_order > A.deal_stage_order THEN 'demotion'
    WHEN deal_close_date != previous_deal_close_date THEN 'close_date'
    ELSE 'N/A'
  END AS change_type,
  {{ fiscal_quarter('DATE(B.timeline_entered_stage1)') }} AS fiscal_quarter_entered_stage1,
  B.* EXCEPT(deal_id),
FROM windowed AS A
LEFT JOIN timeline AS B
  ON A.deal_id = B.deal_id
WHERE next_dt >= DATE(A.stage_start_dttm)
  AND next_dt < DATE(A.stage_end_dttm)
