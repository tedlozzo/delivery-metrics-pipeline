import duckdb
import pandas as pd
from typing import List, Dict, Any
from utils.git_provider import GitProvider

class DataFetcher:
    def __init__(self, provider: GitProvider, db_path: str):
        self.provider = provider
        self.db_path = db_path
    
    def get_last_updated_at(self, con):
        """Fetch the last updated_at timestamp from the pull_requests table."""
        try:
            result = con.execute("SELECT MAX(updated_at) FROM pull_requests").fetchone()
            if result[0] is not None:
                return result[0]
        except Exception as e:
            print(f"Error fetching last updated_at: {e}")
        return "2000-01-01T00:00:00Z"
    
    def get_last_commit_date(self, con):
        """Fetch the last commit date from the commits table."""
        try:
            result = con.execute("SELECT MAX(commit_date) FROM commits").fetchone()
            if result[0] is not None:
                return result[0]
        except Exception as e:
            print(f"Error fetching last commit date: {e}")
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
    
    def upsert_commits(self, con, commits: List[Dict[str, Any]]):
        if not commits:
            print("No new commits to upsert.")
            return
        
        # Normalize data using provider
        normalized_commits = [self.provider.normalize_commit(commit) for commit in commits]
        df = pd.DataFrame(normalized_commits)
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                sha TEXT PRIMARY KEY,
                author_name TEXT,
                author_email TEXT,
                author_date TIMESTAMP,
                committer_name TEXT,
                committer_email TEXT,
                commit_date TIMESTAMP,
                message TEXT,
                html_url TEXT
            );
        """)
        
        con.execute("CREATE OR REPLACE TEMP TABLE _new_commits AS SELECT * FROM df")
        con.execute("""
            INSERT INTO commits
            SELECT * FROM _new_commits
            ON CONFLICT (sha) DO UPDATE SET
                author_name = excluded.author_name,
                author_email = excluded.author_email,
                author_date = excluded.author_date,
                committer_name = excluded.committer_name,
                committer_email = excluded.committer_email,
                commit_date = excluded.commit_date,
                message = excluded.message,
                html_url = excluded.html_url
        """)
    
    def fetch_and_upsert_pull_requests(self, con, since):
        page = 1
        total_prs = 0
        
        while True:
            prs = self.provider.fetch_pull_requests(since, page)
            if not prs:
                break
            
            print(f"Processing {len(prs)} pull requests from page {page}")
            self.upsert_pull_requests(con, prs)
            total_prs += len(prs)
            
            if len(prs) < 100:
                break
            
            page += 1
        
        print(f"Total pull requests processed: {total_prs}")
    
    def fetch_and_upsert_commits(self, con, since):
        page = 1
        total_commits = 0
        
        while True:
            commits = self.provider.fetch_commits(since, page)
            if not commits:
                print("No more commits to process.")
                break
            
            print(f"Processing {len(commits)} commits from page {page}")
            self.upsert_commits(con, commits)
            total_commits += len(commits)
            
            if len(commits) < 100:
                break
            
            page += 1
        
        print(f"Total commits processed: {total_commits}")
    
    def run(self):
        con = duckdb.connect(self.db_path)
        
        # Fetch pull requests
        last_updated = self.get_last_updated_at(con)
        print(f"Last updated_at: {last_updated}")
        self.fetch_and_upsert_pull_requests(con, since=last_updated)
        
        # Fetch commits
        last_commit_date = self.get_last_commit_date(con)
        print(f"Last commit date: {last_commit_date}")
        self.fetch_and_upsert_commits(con, since=last_commit_date)