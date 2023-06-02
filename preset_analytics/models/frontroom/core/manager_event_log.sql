{% set rules = [
    ['/accept_invitation/?%', 'accept-invitation'],
    ['/api/v1/auth-providers/by-email/?%', 'auth-providers.by-email'],
    ['/api/v1/teams/%/groups/?', 'teams.groups'],
    ['/api/v1/teams/%/groups/workspace_roles/%/?', 'teams.groups.workspace-roles'],
    ['/api/v1/teams/%/invites/?', 'teams.invites'],
    ['/api/v1/teams/%/invites/accept/%/?', 'teams.invites'],
    ['/api/v1/teams/%/invites/many/?', 'teams.invites'],
    ['/api/v1/teams/%/payment/billing-info?%', 'teams.payment.billing-info'],
    ['/api/v1/teams/%/payment/downgrade?%', 'teams.payment.downgrade'],
    ['/api/v1/teams/%/payment/plans?%', 'teams.payment.plans'],
    ['/api/v1/teams/%/payment/subscriptions?', 'teams.payment.subscriptions'],
    ['/api/v1/teams/%/payment/transactions?', 'teams.payment.transations'],
    ['/api/v1/teams/%/scim/v2?%', 'teams.scim'],
    ['/api/v1/teams/%/workspaces/%/memberships?%', 'teams.workspaces.membership'],
    ['/api/v1/teams/billing_status/?', 'teams.billing-status'],
    ['/api/v1/teams/register/onboard/?', 'teams.register.onboarding'],
    ['/api/v1/teams/%/?', 'teams.id'],
    ['/api/v1/users/exists/%', 'users.exists'],
    ['/api/v1/users/register/onboard/?', 'users.register.onboarding'],
    ['/app/?%', 'app'],
    ['/app/%/workspace/%/roles?', 'workspace.roles'],
    ['/app/teams/%?%', 'teams'],
    ['/app/teams/%/members?%', 'teams.members'],
    ['/app/teams/%/members?%', 'teams.members'],
    ['/app/teams/%/settings?%', 'teams.settings'],
    ['/app/teams/%/stats?', 'teams.stats'],
    ['/app/teams/%/users?%', 'teams.users'],
    ['/google-login-redirect/?%', 'google-login-redirect'],
    ['/login/callback?%', 'login.callback'],
    ['/login%', 'login'],
    ['/logout?%', 'logout'],
    ['/register/onboarding/?next=%', 'register.onboarding'],
    ['/static-admin/%', 'static.admin'],
] %}

SELECT
    CASE
        {% for like_pattern, action in rules %}
        WHEN request_url LIKE '{{ like_pattern }}' THEN '{{ action }}'
        {% endfor %}
        ELSE REPLACE(REGEXP_EXTRACT(request_url, '^[/]*(.*)[/]+[[?.*]]*$'), '/', '.')
    END AS action,
    sent_at,
    team_id,
    CAST(timestamp AS DATETIME) AS dttm,
    CAST(sent_at AS DATETIME) AS sent_at_dttm,
    user_id,
    anonymous_id,
    request_url,
FROM {{ source('manager_events_production', 'manager_events') }}
