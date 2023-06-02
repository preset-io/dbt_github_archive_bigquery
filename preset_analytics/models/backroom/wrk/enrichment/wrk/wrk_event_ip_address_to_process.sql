SELECT
  'https://geolocation-db.com/json/' || ip_address AS rest_endpoint
FROM {{ ref('wrk_event_ip_address_all') }}
WHERE preprocessed_at = (SELECT MAX(preprocessed_at) FROM {{ ref('wrk_event_ip_address_all') }})
