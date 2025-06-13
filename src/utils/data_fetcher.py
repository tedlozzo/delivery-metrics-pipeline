import duckdb
import pandas as pd
from typing import List, Dict, Any

import requests
from utils.git_provider import GitProvider

class DataFetcher:
    def __init__(self, provider: GitProvider, db_path: str):
        self.provider = provider
        self.db_path = db_path

    def get_last_updated_at(self, con):
        """Fetch the last updated_at timestamp from the pull_requests table."""
        try:
            # Check if table exists first
            result = con.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = 'pull_requests'
            """).fetchone()
            
            if result is None:
                print("pull_requests table does not exist yet.")
                return "2000-01-01T00:00:00Z"
            
            result = con.execute("SELECT MAX(updated_at) FROM pull_requests").fetchone()
            if result[0] is not None:
                # Convert datetime to ISO string format
                return result[0].isoformat() + 'Z'
        except Exception as e:
            print(f"Error fetching last updated_at: {e}")
        return "2000-01-01T00:00:00Z"

    def upsert_pull_requests(self, con, prs: List[Dict[str, Any]]):
        if not prs:
            print("No new pull requests to upsert.")
            return
        
        # Normalize data using provider
        normalized_prs = [self.provider.normalize_pull_request(pr) for pr in prs]
        df = pd.DataFrame(normalized_prs)
        
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
            );
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

    def upsert_pr_commits(self, con, pull_request_id: int, pull_number: int, commits: List[Dict[str, Any]]):
        if not commits:
            print(f"No commits found for PR #{pull_number}")
            return

        # Normalize data using provider
        normalized_commits = [self.provider.normalize_commit(commit) for commit in commits]

        # Add pull request reference to each commit
        for commit in normalized_commits:
            commit['pull_request_id'] = pull_request_id
            commit['pull_request_number'] = pull_number

        # Create DataFrame
        df = pd.DataFrame(normalized_commits)

        # Debug: Print DataFrame info
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"DataFrame dtypes before conversion:\n{df.dtypes}")

        # Ensure data types are correct before inserting
        df = df.astype({
            'sha': 'string',
            'pull_request_id': 'int64',
            'pull_request_number': 'int64',
            'author_name': 'string',
            'author_email': 'string',
            'committer_name': 'string',
            'committer_email': 'string',
            'message': 'string',
            'html_url': 'string'
        })

        # Convert timestamp columns properly
        df['author_date'] = pd.to_datetime(df['author_date'], errors='coerce')
        df['commit_date'] = pd.to_datetime(df['commit_date'], errors='coerce')

        # Debug: Print DataFrame info after conversion
        print(f"DataFrame dtypes after conversion:\n{df.dtypes}")
        print(f"DataFrame sample data:\n{df.head()}")

        # Create the table if it doesn't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS pull_request_commits (
                sha TEXT,
                pull_request_id BIGINT,
                pull_request_number INT,
                author_name TEXT,
                author_email TEXT,
                author_date TIMESTAMP,
                committer_name TEXT,
                committer_email TEXT,
                commit_date TIMESTAMP,
                message TEXT,
                html_url TEXT,
                PRIMARY KEY (sha, pull_request_id),
                FOREIGN KEY (pull_request_id) REFERENCES pull_requests(id)
            );
        """)

        # Explicitly specify the column order for the temporary table
        column_order = [
            'sha',
            'pull_request_id',
            'pull_request_number',
            'author_name',
            'author_email',
            'author_date',
            'committer_name',
            'committer_email',
            'commit_date',
            'message',
            'html_url'
        ]

        # Reorder the DataFrame columns to match the table schema
        df = df[column_order]


        # Insert data into the table
        con.execute("CREATE OR REPLACE TEMP TABLE _new_pr_commits AS SELECT * FROM df")
        con.execute(f"""
            INSERT INTO pull_request_commits ({', '.join(column_order)})
            SELECT {', '.join(column_order)} FROM _new_pr_commits
            ON CONFLICT (sha, pull_request_id) DO UPDATE SET
                pull_request_number = excluded.pull_request_number,
                author_name = excluded.author_name,
                author_email = excluded.author_email,
                author_date = excluded.author_date,
                committer_name = excluded.committer_name,
                committer_email = excluded.committer_email,
                commit_date = excluded.commit_date,
                message = excluded.message,
                html_url = excluded.html_url
        """)

    def fetch_and_upsert_pull_requests_with_commits(self, con, since):
        page = 1
        total_prs = 0
        
        while True:
            prs = self.provider.fetch_pull_requests(since, page)
            if not prs:
                break
            
            print(f"Processing {len(prs)} pull requests from page {page}")
            self.upsert_pull_requests(con, prs)
            
            # Fetch commits for each pull request
            for pr in prs:
                pr_id = pr["id"]
                pr_number = pr["number"]
                print(f"Fetching commits for PR #{pr_number}")
                
                # Fetch all commits for this PR (handle pagination)
                commit_page = 1
                all_commits = []
                
                while True:
                    try:
                        commits = self.provider.fetch_commits_for_pull_request(pr_number, commit_page)
                        if not commits:
                            break
                        
                        all_commits.extend(commits)
                        
                        # If we got less than the max per page, we're done
                        if len(commits) < 100:
                            break
                        
                        commit_page += 1
                    except Exception as e:
                        print(f"Error fetching commits for PR #{pr_number}: {e}")
                        break
                
                if all_commits:
                    print(f"Found {len(all_commits)} commits for PR #{pr_number}")
                    self.upsert_pr_commits(con, pr_id, pr_number, all_commits)
            
            total_prs += len(prs)
            
            if len(prs) < 100:
                break
            
            page += 1
        
        print(f"Total pull requests processed: {total_prs}")

    def run(self):
        con = duckdb.connect(self.db_path)
        
        # Fetch pull requests with their commits
        last_updated = self.get_last_updated_at(con)
        print(f"Last updated_at: {last_updated}")
        self.fetch_and_upsert_pull_requests_with_commits(con, since=last_updated)
        
        con.close()

    def fetch_pull_requests(self, last_updated: str, page: int = 1, per_page: int = 100):
        url = f"https://api.github.com/repos/{self.repo}/pulls"
        params = {
            "state": "all",  # Fetch all pull requests (open, closed, merged)
            "sort": "updated",  # Sort by the updated_at field
            "direction": "asc",  # Ascending order to process oldest first
            "per_page": per_page,
            "page": page
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        prs = response.json()

        # Filter pull requests locally based on the last_updated timestamp
        filtered_prs = [
            pr for pr in prs
            if pr["updated_at"] > last_updated
        ]

        return filtered_prs