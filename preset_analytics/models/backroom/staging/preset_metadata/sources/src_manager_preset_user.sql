SELECT
    ds,
    id AS user_id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    active,
    last_login,
    login_count,
    fail_login_count,
    onboarded,
    company_name,
    company_category,
    company_size,
    description,
    is_preset,
    CASE
        WHEN LOWER(email) = 'moolie.pu@moonactive.con' THEN 'moolie.pu@moonactive.com'
        ELSE LOWER(email)
    END AS email,
    CASE
        WHEN REGEXP_CONTAINS(first_name, r'<script src=https:\/\/(.*?)>')
            THEN REGEXP_EXTRACT(first_name, r'<script src=https:\/\/(.*?)>')
        ELSE first_name
    END AS first_name,
    CASE
        WHEN REGEXP_CONTAINS(last_name, r'<script src=https:\/\/(.*?)>')
            THEN REGEXP_EXTRACT(last_name, r'<script src=https:\/\/(.*?)>')
        ELSE last_name
    END AS last_name,
    email_marketing,
    is_self_registration,
FROM {{ ref('src_manager_preset_user_dedup') }}
WHERE ds >= {{ var("start_date") }}
