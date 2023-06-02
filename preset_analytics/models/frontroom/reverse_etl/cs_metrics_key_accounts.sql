{{ config(
    materialized='table'
  )
}}

SELECT
  *
FROM
 {{ ref( 'cs_metrics_key_accounts_history' ) }}
 WHERE
  dt = (SELECT MAX(dt) FROM {{ ref( 'cs_metrics_key_accounts_history' ) }} )
