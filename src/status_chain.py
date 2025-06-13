import duckdb
import pandas as pd
import os

def extract_status_chains():
    con = duckdb.connect("github_data.duckdb", read_only=True)

    sql = """
   WITH issue_created AS (
    SELECT
        key AS issue_key,
        json_extract_string(fields, 'created')::TIMESTAMP AS created
    FROM jira_issues
),
issue_types AS (
    SELECT
        key AS issue_key,
        json_extract_string(json_extract(fields, 'issuetype'), 'name') AS issue_type
    FROM jira_issues
    WHERE
    json_extract_string(json_extract(fields, 'status'), 'name') IN ('Closed', 'Done', 'Resolved', 'Patch available', 'Fixed', 'Released')
),
status_transitions AS (
    SELECT
        issue_key,
        created,
        replace(from_string, 'Resolved', 'Closed') AS from_string,
        replace(to_string, 'Resolved', 'Closed') AS to_string,
        ROW_NUMBER() OVER (
            PARTITION BY issue_key
            ORDER BY created
        ) AS step
    FROM jira_changelog
    WHERE field = 'status'
),
first_transition_per_issue AS (
    SELECT
        issue_key,
        from_string,
        created,
        ROW_NUMBER() OVER (
            PARTITION BY issue_key
            ORDER BY created
        ) AS row_num
    FROM status_transitions
),
synthetic_created_transitions AS (
    SELECT
        ic.issue_key,
        0 AS step,
        'Created' AS source,
        ft.from_string AS target,
        ic.created AS transition_timestamp
    FROM issue_created ic
    JOIN first_transition_per_issue ft
        ON ic.issue_key = ft.issue_key
    WHERE ft.row_num = 1
),
real_transitions AS (
    SELECT
        issue_key,
        step,
        from_string AS source,
        to_string AS target,
        created AS transition_timestamp
    FROM status_transitions
),
combined_transitions AS (
    SELECT * FROM synthetic_created_transitions
    UNION ALL
    SELECT * FROM real_transitions
),
chain_lengths AS (
    SELECT
        issue_key,
        MAX(step) + 1 AS chain_length
    FROM combined_transitions
    GROUP BY issue_key
),
transitions_with_durations AS (
    SELECT
        *,
        LAG(transition_timestamp) OVER (
            PARTITION BY issue_key ORDER BY step
        ) AS previous_timestamp
    FROM combined_transitions
)
SELECT
    t.issue_key,
    t.step,
    t.source,
    t.target,
    t.previous_timestamp,
    t.transition_timestamp,
    EXTRACT(EPOCH FROM (t.transition_timestamp - t.previous_timestamp)) AS duration_seconds,
    cl.chain_length,
    it.issue_type
FROM transitions_with_durations t
JOIN chain_lengths cl ON t.issue_key = cl.issue_key
JOIN issue_types it ON t.issue_key = it.issue_key
ORDER BY t.issue_key, t.step;


    """

    df = con.execute(sql).df()

    def build_chain_classification(df):
        chain_map = {}
        issue_chain_labels = []

        for issue_key, group in df.groupby("issue_key"):
            raw_chain = group[["source", "target"]].values.tolist()

            # Remove immediate repetition
            seen = []
            for src, tgt in raw_chain:
                if not seen or seen[-1][1] != tgt:
                    seen.append((src, tgt))

            # Create a chain label
            flat_chain = " > ".join([seen[0][0]] + [tgt for _, tgt in seen])

            # Assign ID
            if flat_chain not in chain_map:
                chain_map[flat_chain] = len(chain_map) + 1
            issue_chain_labels.append((issue_key, flat_chain, chain_map[flat_chain]))

        return pd.DataFrame(issue_chain_labels, columns=["issue_key", "chain", "chain_id"])

    df_chains = build_chain_classification(df)
    df_final = df.merge(df_chains, on="issue_key")

    output_path = os.path.join(os.getcwd(), "output/status_chains_for_sankey.csv")
    df_final.to_csv(output_path, index=False)
    print(f"Saved Sankey-compatible data to {output_path}")

if __name__ == "__main__":
    extract_status_chains()
