/*
subscription states defined:

future
Future subscriptions have a start date in the future. The most common use case
for a future subscription is a B2B contract where the subscription is agreed
to start on a specific date. The customer is not invoiced until the start date.

active
Active subscriptions are both regular paying subscriptions and subscriptions
currently in a trial.

canceled
Canceled subscriptions will automatically expire at the term renewal date.
A subscription could be in a canceled state because the customer chose to
cancel their auto-renewing subscription or the subscription is set to expire
at the end of their current term.

expired
Expired subscriptions are churned subscriptions that cannot be reactivated.
A subscription can be expired due to involuntary churn by the dunning cycle or
voluntary churn by canceling.

*/

SELECT
    id AS subscription_id,
    updated_at AS subscription_updated_at,
    account_id,
    activated_at,
    add_ons_total,
    auto_renew,
    bank_account_authorized_at,
    canceled_at,
    collection_method,
    created_at AS subscription_created_at,
    currency,
    current_period_ends_at,
    current_period_started_at,
    current_term_ends_at,
    current_term_started_at,
    customer_notes,
    expiration_reason,
    expires_at,
    net_terms,
    object,
    paused_at,
    plan_id,
    po_number,
    quantity,
    remaining_billing_cycles,
    remaining_pause_cycles,
    renewal_billing_cycles,
    shipping_address_id,
    started_with_gift,
    state AS subscription_state,
    subtotal,
    terms_and_conditions,
    total_billing_cycles,
    trial_ends_at,
    trial_started_at,
    unit_amount,
    uuid AS subsription_uuid,
    CASE
      WHEN
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at) = 1
        AND created_at != updated_at
      THEN CAST(created_at AS DATETIME)
      ELSE CAST(updated_at AS DATETIME)
    END AS effective_from,
    LEAD(CAST(updated_at AS DATETIME)) OVER (PARTITION BY id ORDER BY updated_at) AS effective_to,
FROM {{ source('recurly', 'subscription_history') }}
