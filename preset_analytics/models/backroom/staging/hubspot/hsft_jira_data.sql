{{
  config(
    alias='jira_data',
    schema='wrk',
    materialized='table'
  )
}}

SELECT
    contact.contact_id AS hs_contact_id,
    company.company_id AS hs_company_id,
    LOWER(contact.email) AS email,
    contact.billing_status,
    company.level_of_support,
    company.renewal_date
FROM {{ ref('hubspot__contacts') }} AS contact
LEFT JOIN {{ ref('hubspot__companies') }} AS company
    ON CAST(contact.ce_company_id AS INT) = company.company_id
