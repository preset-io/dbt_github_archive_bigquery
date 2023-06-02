{{ config(materialized='table') }}

WITH manager_user_onboarding_latest AS (
    {{ latest_partition(ref('src_manager_user_onboarding')) }}
)

SELECT *
FROM manager_user_onboarding_latest
