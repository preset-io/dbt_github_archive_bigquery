SELECT
    id AS plan_id,
    updated_at AS plan_updated_at,
    accounting_code,
    auto_renew AS plan_auto_renew,
    code AS plan_code,
    created_at AS plan_created_at,
    deleted_at AS deleted_at,
    description AS plan_description,
    hosted_pages_bypass_confirmation,
    hosted_pages_cancel_url,
    hosted_pages_display_quantity,
    hosted_pages_success_url,
    interval_length,
    interval_unit,
    name AS plan_name,
    setup_fee_accounting_code,
    state AS plan_state,
    tax_code AS plan_tax_code,
    tax_exempt AS plan_tax_exempt,
    total_billing_cycles AS plan_total_billing_cycles,
    trial_length AS plan_trial_length,
    trial_unit AS plan_trial_unit,
    CASE
      WHEN
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at) = 1
        AND created_at != updated_at
      THEN CAST(created_at AS DATETIME)
      ELSE CAST(updated_at AS DATETIME)
    END AS effective_from,
    LEAD(CAST(updated_at AS DATETIME)) OVER (PARTITION BY id ORDER BY updated_at) AS effective_to,
FROM {{ source('recurly', 'plan_history') }}
