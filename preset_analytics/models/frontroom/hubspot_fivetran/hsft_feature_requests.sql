{{
  config(
    alias='feature_request',
    materialized='view'
  )
}}
SELECT
  -- Fields likely to be used
	id AS feature_request_id,
	property_category AS feature_request_category,
	property_request_name AS feature_request_name,
	property_type AS feature_request_type,
	property_subtype AS feature_request_subtype,
	property_link AS feature_request_link,
	DATE(property_hs_createdate) AS feature_request_created_dt,
	property_hs_object_id AS hs_object_id,
	property_notes AS feature_request_notes,
	property_importance AS feature_request_importance,
	property_logged_in_aha AS feature_request_logged_in_aha,

  -- Unlikely to be used by users
	property_hs_created_by_user_id,
	property_hs_lastmodifieddate,
	property_hs_updated_by_user_id,
	_fivetran_synced,
	updated_at,
	archived,
	archived_at,
	created_at,
FROM {{ source('fivetran_hubspot', 'feature_requests') }}
