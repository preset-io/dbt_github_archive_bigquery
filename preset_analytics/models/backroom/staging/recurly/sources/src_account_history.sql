/*
account state definitions:

active:
Account has an active, expired, or canceled subscription but has not been removed from the Preset system

inactive:
Account has either an expired or canceled subscription and has been removed from the Preset system

*/

SELECT
    id AS account_id,
    --This field allows us to do a nice join with manager_team.team_hash
    REPLACE(code, 'team-production-', '') AS team_hash,

    updated_at AS account_updated_at,
    account_city,
    account_country,
    account_first_name,
    account_last_name,
    account_phone,
    account_postal_code,
    account_region,
    account_street_1,
    account_street_2,
    bill_to,
    LOWER(cc_emails) AS cc_emails,
    code AS account_code,
    company,
    created_at AS account_created_at,
    deleted_at AS account_deleted_at,
    LOWER(email) AS email,
    exemption_certificate,
    first_name,
    hosted_login_token,
    last_name,
    preferred_locale,
    state AS account_state,
    tax_exempt,
    username,
    vat_number,
    CASE
      WHEN
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at) = 1
        AND created_at != updated_at
      THEN CAST(created_at AS DATETIME)
      ELSE CAST(updated_at AS DATETIME)
    END AS effective_from,
    email LIKE '%sophieyou%' OR
      email LIKE '%@preset.io' OR
      email LIKE 's.lemena%' OR
      code LIKE 'Seb LeMenager'
      AS is_preset,
    LEAD(CAST(updated_at AS DATETIME)) OVER (PARTITION BY id ORDER BY updated_at) AS effective_to,
FROM {{ source('recurly', 'account_history') }}
