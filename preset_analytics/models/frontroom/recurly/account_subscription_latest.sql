{{ config(materialized="table") }}
SELECT *
FROM {{ ref('account_subscription_history') }}
WHERE dt = DATE('{{ latest_dt(ref('account_subscription_history'), "dt", previous_day=False) }}')
