{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'},
) }}

{% set process_to_dt = latest_dt(ref('superset_event_log'), "dt", previous_day=False) %}



WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    WHERE dt <= DATE("{{ process_to_dt }}")
    {% if is_incremental() %}
        AND {{ generate_incremental_statement(this, this_date_col='ds') }}
    {% endif %}
),

active_entity_agg AS (
    SELECT
        dt AS ds,
        team_id,
        COALESCE(SUM(CASE WHEN entity_type = 'user' THEN daily_active END), 0) AS dau,
        COALESCE(SUM(CASE WHEN entity_type = 'user' THEN weekly_active END), 0) AS wau,
        COALESCE(SUM(CASE WHEN entity_type = 'user' THEN monthly_active END), 0) AS mau,
        COALESCE(SUM(CASE WHEN entity_type = 'user' THEN quarterly_active END), 0) AS qau,
        COALESCE(SUM(CASE WHEN entity_type = 'dashboard' THEN daily_active END), 0) AS da_dashboard,
        COALESCE(SUM(CASE WHEN entity_type = 'dashboard' THEN weekly_active  END), 0) AS wa_dashboard,
        COALESCE(SUM(CASE WHEN entity_type = 'dashboard' THEN monthly_active END), 0) AS ma_dashboard,
        COALESCE(SUM(CASE WHEN entity_type = 'dashboard' AND NOT is_example THEN daily_active END), 0) AS da_dashboard_noex,
        COALESCE(SUM(CASE WHEN entity_type = 'dashboard' AND NOT is_example THEN weekly_active END), 0) AS wa_dashboard_noex,
        COALESCE(SUM(CASE WHEN entity_type = 'dashboard' AND NOT is_example THEN monthly_active END), 0) AS ma_dashboard_noex,
        COALESCE(SUM(CASE WHEN entity_type = 'user' THEN visits_7d END), 0) AS visits_7d,
        COALESCE(SUM(CASE WHEN entity_type = 'user' THEN visits_28d END), 0) AS visits_28d,
    FROM {{ ref('wrk_egaf_summary') }}
    -- records aggregated at the full team level
    WHERE workspace_hash = 'ALL WORKSPACES'
    GROUP BY 1, 2
),

manager_invite_agg AS (
    SELECT
        ds,
        team_id,
        COALESCE(COUNT((CASE invite_status WHEN 'PENDING' THEN creator_user_id END)), 0) AS invite_pending,
        COALESCE(COUNT(DISTINCT (CASE invite_status WHEN 'ACCEPTED' THEN accepted_by_user_id END)), 0) AS invite_accepted,
        COALESCE(COUNT(*), 0) AS invite_sent,
    FROM {{ ref('manager_invite_history') }}
    GROUP BY 1, 2
),

database_connection_agg AS (
    SELECT
        date_spine.dt AS ds,
        db_conn.team_id,
        COUNT(DISTINCT CASE WHEN NOT db_conn.is_example THEN db_conn.database_connection_key END) AS non_example_database_count,
        COUNT(DISTINCT db_conn.database_connection_key) AS database_count,
        ARRAY_AGG(DISTINCT db_conn.database_driver) AS connected_database_type_array,
    FROM date_spine
    LEFT JOIN {{ ref('superset_database_connection_history') }} AS db_conn
        ON date_spine.dt >= db_conn.ds
    GROUP BY 1, 2
),

first_event_agg AS (
    -- Getting the first and most recent visit for the team
    SELECT
        NULLIF(team_id, -1) AS team_id,
        MIN(first_event) AS first_event,
        MAX(most_recent_event) AS most_recent_event,
    FROM {{ ref('wrk_egaf_first_event') }}
    WHERE entity_type = 'user'
    GROUP BY 1
),

manager_workspace_agg AS (
    SELECT
        ds,
        team_id,
        COALESCE(COUNT(*), 0) AS workspace_count,
    FROM {{ ref('wrk_manager_workspace') }}
    GROUP BY 1, 2
),

chart_agg AS (
    SELECT
        ds,
        team_id,
        COUNT(DISTINCT CASE WHEN NOT is_example THEN chart_key END) AS chart_noex_count,
        COUNT(DISTINCT chart_key) AS chart_count,
    FROM {{ ref('src_superset_slice') }}
    GROUP BY 1, 2
),

dashboard_agg AS (
    SELECT
        ds,
        team_id,
        COUNT(DISTINCT CASE WHEN NOT is_example THEN dashboard_key END) AS dashboard_noex_count,
        COUNT(DISTINCT dashboard_key) AS dashboard_count,
    FROM {{ ref('src_superset_dashboard') }}
    WHERE NOT is_example
    GROUP BY 1, 2
),

team_mem_array AS (
    -- Getting a list of email domains for the team!
    SELECT
        A.ds,
        A.team_id,
        CASE MAX(B.company_size_rank)
            WHEN 1 THEN "SELF_EMPLOYED"
            WHEN 2 THEN "EMPLOYEES_2_5"
            WHEN 3 THEN "EMPLOYEES_6_100"
            WHEN 4 THEN "EMPLOYEES_101_1000"
            WHEN 5 THEN "EMPLOYEES_1001_10000"
            WHEN 6 THEN "EMPLOYEES_10001_PLUS"
        END AS company_size,
        ARRAY_AGG(DISTINCT CAST(A.user_id AS STRING)) AS team_member_id_array,
        ARRAY_AGG(DISTINCT CAST(B.hs_contact_id AS STRING) IGNORE NULLS) AS team_member_hs_contact_id_array,
        COUNT(DISTINCT CASE WHEN B.is_creator THEN B.user_id END) AS num_creator_seats,
        COUNT(DISTINCT CASE WHEN B.is_viewer THEN B.user_id END) AS num_viewer_seats,
    FROM {{ ref('src_manager_team_membership') }} AS A
    INNER JOIN {{ ref('wrk_manager_user') }} AS B
        ON A.ds = B.ds
            AND A.user_id = B.user_id
    GROUP BY 1, 2
),

events_agg AS (
    SELECT
        date_spine.dt AS ds,
        CAST(egaf.entity_id AS INT) AS entity_id,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE_DIFF(date_spine.dt, egaf.dt, DAY) BETWEEN 0 AND 6 THEN egaf.dt END ), 0) AS l7,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE_DIFF(date_spine.dt, egaf.dt, DAY) BETWEEN 0 AND 27 THEN egaf.dt END ), 0) AS l28,
        COALESCE(COUNT(*), 0) AS ltd_visits,
    FROM date_spine
    LEFT JOIN {{ ref('wrk_egaf_events') }} AS egaf
        ON egaf.dt <= date_spine.dt
    WHERE egaf.entity_type = 'team'
    GROUP BY 1, 2
),

MTH AS (
  SELECT
      date_spine.dt AS ds,
      A.* EXCEPT (ds),
      B.dau,
      B.wau,
      B.mau,
      RANK() OVER (
        PARTITION BY date_spine.dt, A.is_preset
        ORDER BY COALESCE(B.mau, 0) + (COALESCE(I.ltd_visits, 0) / 100000000) DESC
      ) AS mau_rank,
      B.da_dashboard,
      B.wa_dashboard,
      B.ma_dashboard,
      B.da_dashboard_noex,
      B.wa_dashboard_noex,
      B.ma_dashboard_noex,
      C.invite_pending,
      C.invite_accepted,
      C.invite_sent,
      A.team_billing_status = 'FREE' AND C.invite_sent < 5 AS has_available_invite_derived,
      COALESCE(D.non_example_database_count, 0) AS non_example_database_count,
      COALESCE(D.database_count, 0) AS database_count,
      D.connected_database_type_array,
      B.visits_7d,
      B.visits_28d,
      F.first_event AS first_visit,
      F.most_recent_event AS most_recent_visit,
      G.workspace_count,
      COALESCE(H.dashboard_noex_count, 0) AS dashboard_noex_count,
      COALESCE(H.dashboard_count, 0) AS dashboard_count,
      COALESCE(J.chart_noex_count, 0) AS chart_noex_count,
      COALESCE(J.chart_count, 0) AS chart_count,
      COALESCE(I.ltd_visits, 0) AS ltd_visits,
      I.l7,
      I.l28,
      NOT A.is_preset
        AND COALESCE(D.non_example_database_count, 0) > 0
        AND COALESCE(H.dashboard_noex_count, 0) > 0
        AND A.team_members >= 2
        AND COALESCE(I.ltd_visits, 0) >= 3
      AS is_activated,
      CASE WHEN COALESCE(D.non_example_database_count, 0) > 0 THEN 40 ELSE 0 END +
        CASE WHEN A.team_members >= 2 THEN 20 ELSE 0 END +
        CASE WHEN COALESCE(H.dashboard_noex_count, 0) > 0 THEN 20 ELSE 0 END +
        CASE WHEN COALESCE(J.chart_noex_count, 0) > 0 THEN 20 ELSE 0 END
      AS activation_score,
      K.embedded_dashboards_viewed,
      K.shared_dashboards_viewed,
      K.onboarding_score
  FROM date_spine
  LEFT JOIN {{ ref('wrk_manager_team') }} AS A
      ON date_spine.dt = DATE(A.ds)
  LEFT JOIN active_entity_agg AS B
      ON date_spine.dt = DATE(B.ds)
          AND A.team_id = B.team_id
  LEFT JOIN manager_invite_agg AS C
      ON date_spine.dt = C.ds
          AND A.team_id = C.team_id
  LEFT JOIN database_connection_agg AS D
      ON date_spine.dt = D.ds
          AND A.team_id = D.team_id
  LEFT JOIN first_event_agg AS F -- needs reworking
      ON A.team_id = F.team_id
  LEFT JOIN manager_workspace_agg AS G
      ON date_spine.dt = G.ds
          AND A.team_id = G.team_id
  LEFT JOIN dashboard_agg AS H
      ON date_spine.dt = H.ds
          AND A.team_id = H.team_id
  LEFT JOIN events_agg AS I
      ON date_spine.dt = I.ds
          AND A.team_id = I.entity_id
  LEFT JOIN chart_agg AS J
      ON date_spine.dt = J.ds
          AND A.team_id = J.team_id
  LEFT JOIN {{ ref( 'wrk_onboarding_score' ) }} AS K
      ON date_spine.dt = K.ds
          AND A.team_id = K.team_id
  )

SELECT
  MTH.*,
  CASE
    WHEN mau_rank = 1 THEN '01'
    WHEN mau_rank <= 5 THEN '02-05'
    WHEN mau_rank <= 10 THEN '05-10'
    WHEN mau_rank <= 25 THEN '10-25'
    WHEN mau_rank <= 50 THEN '26-50'
    WHEN mau_rank <= 100 THEN '51-100'
    ELSE '>100'
  END AS mau_rank_group,
  TA.company_size,
  TA.team_member_id_array,
  TA.team_member_hs_contact_id_array,
  TA.num_creator_seats,
  TA.num_viewer_seats,
  MTH.is_activated AND
    NOT MTH.is_email_domain_generic AND
    MTH.team_billing_status_derived = 'FREE' AND
    MTH.ma_dashboard > 0
    AS is_team_monetizable,
FROM
  MTH
LEFT JOIN
  team_mem_array AS TA
ON MTH.ds = TA.ds
  AND MTH.team_id = TA.team_id
