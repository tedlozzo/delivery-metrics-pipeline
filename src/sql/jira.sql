SELECT
    *
FROM
    (
        SELECT
            jira_issues.key,
            jira_changelog.to_string AS status,
            jira_changelog.created
        FROM
            jira_issues
            LEFT JOIN jira_changelog ON jira_issues.key = jira_changelog.issue_key
        WHERE
            jira_issues.key = 'KAFKA-15604'
            AND jira_changelog.field = 'status'
    ) PIVOT (
        MIN(created) FOR status IN (?)
    ) AS pivoted_statuses