import os
import requests
import duckdb
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

GITHUB_REPO = os.getenv("GITHUB_REPO")  # format: "owner/repo"
GITHUB_API_KEY = os.getenv("GITHUB_API_KEY")
DB_PATH = os.getenv("DUCKDB_PATH", "github_data.duckdb")

assert GITHUB_REPO, "Missing GITHUB_REPO env var"
assert GITHUB_API_KEY, "Missing GITHUB_TOKEN env var"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_API_KEY}",
    "Accept": "application/vnd.github+json"
}

def get_last_updated_at(con):
    con.execute("CREATE TABLE IF NOT EXISTS pull_requests AS SELECT * FROM (SELECT 0 AS id) WHERE FALSE")
    result = con.execute("SELECT MAX(updated_at) FROM pull_requests").fetchone()
    return result[0] if result[0] is not None else "2000-01-01T00:00:00Z"

def fetch_pull_requests(since):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls"
    params = {
        "state": "all",
        "sort": "updated",
        "direction": "asc",
        "per_page": 100,
        "page": 1
    }

    results = []
    while True:
        response = requests.get(url, headers=HEADERS, params={**params, "page": params["page"]})
        response.raise_for_status()
        data = response.json()

        filtered = [pr for pr in data if pr['updated_at'] > since]
        if not filtered:
            break

        results.extend(filtered)
        if len(data) < 100:
            break

        params["page"] += 1

    return results

def upsert_pull_requests(con, prs):
    if not prs:
        print("No new data to upsert.")
        return

    import pandas as pd
    df = pd.json_normalize(prs)
    df = df[[
        "id", "number", "title", "user.login", "state", "created_at", "updated_at", "closed_at", "merged_at", "html_url"
    ]].rename(columns={"user.login": "user_login"})

    con.execute("""
        CREATE TABLE IF NOT EXISTS pull_requests (
            id BIGINT PRIMARY KEY,
            number INT,
            title TEXT,
            user_login TEXT,
            state TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            closed_at TIMESTAMP,
            merged_at TIMESTAMP,
            html_url TEXT
        )
    """)

    con.execute("CREATE OR REPLACE TEMP TABLE _new AS SELECT * FROM df")
    con.execute("""
        INSERT INTO pull_requests
        SELECT * FROM _new
        ON CONFLICT (id) DO UPDATE SET
            number = excluded.number,
            title = excluded.title,
            user_login = excluded.user_login,
            state = excluded.state,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            merged_at = excluded.merged_at,
            html_url = excluded.html_url
    """)

def main():
    con = duckdb.connect(DB_PATH)
    last_updated = get_last_updated_at(con)
    print(f"Last updated_at: {last_updated}")

    prs = fetch_pull_requests(since=last_updated)
    print(f"Fetched {len(prs)} new PR(s).")

    upsert_pull_requests(con, prs)

if __name__ == "__main__":
    main()
