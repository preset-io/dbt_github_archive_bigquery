{{ config(materialized="table") }}

SELECT
    poll_response,
    poll_text_response,
    poll_id
FROM {{ ref('seed_pendo_poll_response') }}
