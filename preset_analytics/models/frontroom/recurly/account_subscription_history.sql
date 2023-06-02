{{ config(materialized="table") }}

{% set arr_mrr = ['arr', 'mrr', 'post_rt_arr', 'post_rt_mrr'] %}
{% set time_frames = [28, 56, 84, 168, 364] %}
{% set mrr_expr = """
CASE
    WHEN z.email IS NOT NULL THEN Z.amount / 12
    WHEN DATE(expires_at) <= A.dt THEN NULL
    WHEN plan_code = 'professional-ga-launch-annual' THEN subtotal / 12
    ELSE subtotal
END
""" %}
{% set post_rt_mrr_expr = """
CASE
    WHEN z.email IS NOT NULL THEN Z.amount / 12
    WHEN DATE(expires_at) < A.dt THEN NULL
    WHEN plan_code = 'professional-ga-launch-annual' THEN subtotal / 12
    ELSE subtotal
END
""" %}

{#
    Hard coding upgrades where customers move to sales led
    fields are `email`, `effective_date` and `amount`
#}
{% set upgrades = [

    ['accounts+preset@balena.io', '2022-05-01', '2099-01-01', 13880, 'paying', 'upgrade'],
    ['andrew@stridefunding.com', '2022-06-16', '2099-01-01', 14500, 'paying', 'upgrade'],
    ['takamitsu.tanaka@roccapital.com', '2022-08-13', '2099-01-01', 13400, 'paying', 'upgrade'],
    ['shira@riverside.fm', '2022-11-02', '2099-01-01', 5760, 'paying', 'upgrade'],
    ['vidette.pires@trajectorservices.com', '2022-11-02', '2099-01-01', 10100, 'paying', 'upgrade'],
    ['tech-billing@placemakr.com', '2023-01-24', '2099-01-01', 10800, 'paying', 'upgrade'],
] %}

WITH raw_qry AS (
    SELECT
        DATE(A.dt) AS dt,
        CASE WHEN Z.email IS NULL AND DATE(expires_at) <= A.dt THEN TRUE ELSE FALSE END AS is_expired,
        CASE
            WHEN Z.status IS NOT NULL THEN Z.status
            WHEN DATE(expires_at) <= A.dt THEN 'expired'
            -- Logic is off-by-one day post reverse trial
            WHEN A.dt < DATE('2022-12-01') AND ((trial_ends_at IS NULL AND COALESCE(subtotal, 0) > 0) OR DATE(trial_ends_at) <= A.dt) THEN 'paying'
            WHEN A.dt >= DATE('2022-12-01') AND ((trial_ends_at IS NULL AND COALESCE(subtotal, 0) > 0) OR DATE(trial_ends_at) < A.dt) THEN 'paying'
            ELSE 'trial'
        END AS trial_status,
        CASE
            WHEN Z.status IS NOT NULL THEN Z.status
            WHEN DATE(expires_at) <= A.dt THEN 'expired'
            WHEN (trial_ends_at IS NULL AND COALESCE(subtotal, 0) > 0) OR DATE(trial_ends_at) < A.dt THEN 'paying'
            ELSE 'trial'
        END AS post_rt_trial_status,
        subtotal AS raw_amount,
        CASE WHEN COALESCE(DATE(expires_at), DATE('2050-01-01')) < DATE(trial_ends_at) THEN DATE(expires_at) ELSE DATE(trial_ends_at) END  AS trial_ended_at,
        {{ mrr_expr }} AS mrr,
        {{ post_rt_mrr_expr }} AS post_rt_mrr,
        {{ mrr_expr }} * 12 AS arr,
        {{ post_rt_mrr_expr }} * 12 AS post_rt_arr,
        A.* EXCEPT (effective_from, effective_to, dt),
        B.* EXCEPT (account_id, plan_id, effective_from, effective_to, dt, subtotal),
        C.* EXCEPT (effective_from, effective_to, dt),
        D.contact_id AS hs_contact_id,
        COALESCE(Z.upgrade_status, 'standard') AS upgrade_status,
    FROM {{ ref('account_history') }} AS A
    JOIN {{ ref('subscription_history') }} AS B ON A.account_id = B.account_id AND A.dt = B.dt
    JOIN {{ ref('plan_history') }} AS C ON B.plan_id = C.plan_id AND A.dt = C.dt
    LEFT JOIN {{ ref('wrk_map_email_to_hs_contact') }} AS D ON A.email = D.email
    LEFT JOIN (
        {% for email, eff_dt, eff_to, amount, status, upgrade_status in upgrades %}
        SELECT
          '{{ email }}' AS email,
          DATE('{{ eff_dt }}') AS eff_dt,
          DATE('{{ eff_to }}') AS eff_to,
          {{ amount }} AS amount,
          '{{ status }}' AS status,
          '{{ upgrade_status }}' AS upgrade_status
            {% if not loop.last %}
                UNION ALL
            {% endif %}
        {% endfor %}
    ) AS Z ON A.email = Z.email AND A.dt >= Z.eff_dt AND A.dt < Z.eff_to
),
rr_qry AS (
    SELECT
        *,
        {% for s in arr_mrr %}
            COALESCE(CASE WHEN trial_status = 'paying' THEN {{ s }} END, 0) AS {{ s }}_paying,
            COALESCE(CASE WHEN trial_status = 'trial' THEN {{ s }} END, 0) AS {{ s }}_trial,
        {% endfor %}
    FROM raw_qry
),
windowed AS (
    SELECT
        *,
        {% for i in time_frames %}
            {% for s in arr_mrr %}
                COALESCE(
                    LAG({{ s }}_paying, {{ i }})
                    OVER (PARTITION BY account_id, subscription_id ORDER BY dt ASC)
                    , 0
                ) AS {{ s }}_{{ i }}d_ago,
            {% endfor %}
        {% endfor %}
    FROM rr_qry
)
SELECT
    *,
    {% for i in time_frames %}
        {% for s in arr_mrr %}
            CASE WHEN {{ s }}_{{ i }}d_ago = 0 THEN {{ s }}_paying ELSE 0 END AS {{ s }}_{{ i }}d_new,
            CASE WHEN {{ s }}_{{ i }}d_ago > 0 AND {{ s }}_paying <= 0 THEN -{{ s }}_{{ i }}d_ago ELSE 0 END AS {{ s }}_{{ i }}d_churn,
            CASE WHEN {{ s }}_{{ i }}d_ago > 0 AND {{ s }}_paying > 0 AND {{ s }}_{{ i }}d_ago > {{ s }}_paying THEN {{ s }}_paying - {{ s }}_{{ i }}d_ago ELSE 0 END AS {{ s }}_{{ i }}d_contraction,
            CASE WHEN {{ s }}_{{ i }}d_ago > 0 AND {{ s }}_paying > 0 AND {{ s }}_{{ i }}d_ago < {{ s }}_paying THEN {{ s }}_paying - {{ s }}_{{ i }}d_ago ELSE 0 END AS {{ s }}_{{ i }}d_expansion,
            CASE WHEN {{ s }}_{{ i }}d_ago > 0 AND {{ s }}_{{ i }}d_ago = {{ s }}_paying THEN {{ s }}_paying - {{ s }}_{{ i }}d_ago ELSE 0 END AS {{ s }}_{{ i }}d_flat,
            CASE WHEN {{ s }}_{{ i }}d_ago > 0 THEN {{ s }}_paying - {{ s }}_{{ i }}d_ago ELSE 0 END AS nrr_change_{{ i }}d_for_{{ s }},
        {% endfor %}
        CASE WHEN trial_status = 'expired' AND DATE_DIFF(dt, trial_ended_at, DAY) < {{ i }} THEN 1 ELSE 0 END as lost_{{ i }}d_deals,
        CASE WHEN trial_status = 'paying' AND DATE_DIFF(dt, trial_ended_at, DAY) < {{ i }} THEN 1 ELSE 0 END as won_{{ i }}d_deals,
        CASE WHEN post_rt_trial_status = 'expired' AND DATE_DIFF(dt, trial_ended_at, DAY) < {{ i }} THEN 1 ELSE 0 END as lost_post_rt_{{ i }}d_deals,
        CASE WHEN post_rt_trial_status = 'paying' AND DATE_DIFF(dt, trial_ended_at, DAY) < {{ i }} THEN 1 ELSE 0 END as won_post_rt_{{ i }}d_deals,
    {% endfor %}
    CASE WHEN LOWER(plan_name) LIKE '%monthly%' THEN mrr_paying / 22 ELSE mrr_paying / 20 END AS sold_seats,
    CASE WHEN is_expired THEN 1 ELSE 0 END as lost_deals,
    CASE WHEN NOT is_expired AND trial_status = 'paying' THEN 1 ELSE 0 END as won_deals,
    CASE WHEN NOT is_expired AND post_rt_trial_status = 'paying' THEN 1 ELSE 0 END as won_post_rt_deals,
FROM windowed A
