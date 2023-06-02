{{ config(materialized="view") }}

SELECT
    github_username,
    user_short_name,
    organization,
    preset_team,
    is_bot,
    sponsor,
    notes,
FROM {{ ref('seed_github_user_enrichment') }}
