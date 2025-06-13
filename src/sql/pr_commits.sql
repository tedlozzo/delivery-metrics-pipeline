WITH pr AS (
SELECT
    pr."number",
    pr."title",
    pr."state",
    pr."created_at",
    pr."merged_at",
    c."sha",
    c."commit_date",
    c."message",
    regexp_extract(pr."title", '(?i)(KAFKA-[0-9]{1,5})', 1) AS jira_key
FROM
    pull_requests pr
LEFT JOIN
    pull_request_commits c
ON
    c."pull_request_id" = pr."id"
WHERE
    pr."state" IN ('closed')
    AND pr."merged_at" IS NOT NULL
)
SELECT * a from pr
WHERE jira_key <> '' AND jira_key IS NOT NULL
