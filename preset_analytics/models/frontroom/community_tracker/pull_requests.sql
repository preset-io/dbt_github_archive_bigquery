{%set xpaths = [
    ['user', '$.login', 'username'],
    ['user', '$.id', 'user_id'],
]%}
{{ airbyte_github_union_all('pull_requests', xpaths) }}
