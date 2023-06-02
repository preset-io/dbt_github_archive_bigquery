-- The deal history with label lookups and better aliases
WITH deal_history AS (
SELECT
    d.deal_id,
    d.date_stage_entered AS stage_start_dttm,
    d.date_stage_exited AS stage_end_dttm,
    d.is_stage_active AS is_active,
    CASE
        WHEN dps.pipeline_stage_label LIKE 'Stage 0.5%'
            THEN 'stage1'
        WHEN deal_stage_name = '17462897'
            THEN 'stage2'
        WHEN dps.pipeline_stage_label LIKE 'Stage %'
            THEN REPLACE(LEFT(LOWER(dps.pipeline_stage_label), 7), ' ', '')
        WHEN deal_stage_name = '9979814' OR LOWER(REPLACE(dps.pipeline_stage_label, ' ', '')) LIKE '%closedwon%'
            THEN 'won'
        WHEN deal_stage_name = '9979815' OR LOWER(REPLACE(dps.pipeline_stage_label, ' ', '')) LIKE '%closedlost%'
            THEN 'lost'
        WHEN LOWER(dps.pipeline_stage_label) LIKE 'strategic%'
            THEN 'strategic'
        WHEN LOWER(dps.pipeline_stage_label) LIKE '%contract%sent%'
            THEN 'stage5'
        ELSE LOWER(dps.pipeline_stage_label)
    END AS deal_stage_short,
    d.deal_stage_name AS deal_stage_code,
    dps.pipeline_stage_label AS deal_stage_label,
    d.pipeline_label AS deal_pipeline_label,
    dps.display_order AS deal_stage_order,
    d.hs_deal_stage_probability AS deal_stage_probability,
FROM {{ ref('hubspot__deal_stages') }} AS d
LEFT JOIN {{ ref('stg_hubspot__deal_pipeline_stage') }} AS dps
    ON d.deal_pipeline_id = dps.deal_pipeline_id
        AND d.deal_stage_name = dps.deal_pipeline_stage_id
WHERE deal_id IS NOT NULL
),
filter AS (
  SELECT * FROM deal_history WHERE deal_stage_short IS NOT NULL
),
previous_stage AS (
  SELECT
      *,
      LAG(deal_stage_short) OVER (PARTITION BY deal_id ORDER BY stage_start_dttm ASC) AS prev,
  FROM filter
)
SELECT
    * EXCEPT(prev),
FROM previous_stage
WHERE deal_stage_short != COALESCE(prev, '')
