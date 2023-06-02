{{
  config(
    materialized='table'
  )
}}

WITH PRC as (
  SELECT
      repo,
      id,
      MAX(actor) AS pr_creator,
      MIN(dttm) AS pr_created_dttm,
  FROM {{ source('github', 'actions_sync') }}
  WHERE action = 'pr_created'
  GROUP BY 1, 2
)

SELECT
    GA.action,
    GA.dttm,
    GA.actor,
    GA.labels,
    GA.id,
    GA.type,
    GA.duration,
    GA.org,
    GA.title,
    GA.closed_at,
    GA.closed_by,
    GA.is_bot,
    GA.version,
    GA.repo,
    GA.parent_type,
    GA.parent_id,
    GA.pr_comments,
    GA.pr_review_comments,
    GA.pr_commits,
    GA.pr_additions,
    GA.pr_deletions,
    GA.pr_changed_files,
    GA.processed_on,
    GUE.user_short_name,
    GUE.organization AS user_organization,
    GUE.preset_team,
    GUE.is_bot AS user_is_bot,
    GUE.sponsor,
    PRC.pr_creator,
    PRC.pr_created_dttm,
FROM {{ source('github', 'actions_sync') }} AS GA
LEFT JOIN {{ ref('github_user_enrichment') }} AS GUE
  ON GA.actor = GUE.github_username
LEFT JOIN PRC -- Temporary, should be added to scraping data
  ON GA.repo = PRC.repo
    AND GA.parent_id = PRC.id
    AND GA.parent_type = 'pr'
-- Don't include self-reviews
WHERE NOT COALESCE(pr_creator = actor AND action = 'pr_review', FALSE)
