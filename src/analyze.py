import csv
import logging
from os import path
import duckdb

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query():
    con = duckdb.connect("github_data.duckdb", read_only=True)
    script_dir = path.dirname(path.abspath(__file__))
    result_path = path.join(script_dir, "result.csv")

    try:
        # Step 1: Get all unique status values (sorted for consistent column order)
        df_statuses = con.execute("""
            SELECT DISTINCT to_string AS status
            FROM jira_changelog
            WHERE field = 'status'
            ORDER BY to_string
        """).df()

        status_list = df_statuses['status'].tolist()

        if not status_list:
            logger.warning("No statuses found in the changelog.")
            return

        # Step 2: Build status literal list for PIVOT
        status_literals = ', '.join(f"'{status}'" for status in status_list)

        if not status_literals:
            logger.warning("No valid status literals for PIVOT.")
            return

        # Step 3: Compose MIN and MAX pivot SQL using all issues
        sql = f"""
        WITH min_created AS (
            SELECT *
            FROM (
                SELECT
                    jira_issues.key,
                    jira_issues.fields.summary AS summary,
                    jira_issues.fields.status.name AS current_status,
                    jira_issues.fields.issue_type.name AS issue_type,
                    jira_issues.fields.assignee.display_name AS assignee,
                    jira_changelog.to_string AS status,
                    jira_changelog.created
                FROM
                    jira_issues
                LEFT JOIN
                    jira_changelog ON jira_issues.key = jira_changelog.issue_key
                WHERE
                    jira_changelog.field = 'status'
            )
            PIVOT (
                MIN(created)
                FOR status IN ({status_literals})
            )
        ),
        max_created AS (
            SELECT *
            FROM (
                SELECT
                    jira_issues.key,
                    jira_changelog.to_string AS status,
                    jira_changelog.created
                FROM
                    jira_issues
                LEFT JOIN
                    jira_changelog ON jira_issues.key = jira_changelog.issue_key
                WHERE
                    jira_changelog.field = 'status'
            )
            PIVOT (
                MAX(created)
                FOR status IN ({status_literals})
            )
        )
        SELECT
            min_created.key,
            min_created.summary,
            min_created.current_status,
            min_created.issue_type,
            min_created.assignee
            {''.join(f', min_created."{status}" AS "{status}_min", max_created."{status}" AS "{status}_max"' for status in status_list)}
        FROM min_created
        JOIN max_created ON min_created.key = max_created.key
        ORDER BY min_created.key
        """

        # Step 4: Execute and save
        df = con.execute(sql).df()

        # Remove quotes at the beginning and end of each string. Artifacts from json_dump
        df = df.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)

        # Save the cleaned DataFrame to CSV
        df.to_csv(result_path, index=False)
        print(df)

    except Exception as e:
        logger.warning(f"DuckDB query failed: {e}")
        return None

if __name__ == "__main__":
    query()
