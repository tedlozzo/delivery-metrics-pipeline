import requests
from datetime import datetime, timezone
from typing import List, Dict, Any
from utils.git_provider import GitProvider

class GitHubProvider(GitProvider):
    def __init__(self, repo: str, api_key: str):
        self.repo = repo
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
    
    def fetch_pull_requests(self, last_updated: str, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        url = f"https://api.github.com/repos/{self.repo}/pulls"
        params = {
            "state": "all",  # Fetch all pull requests (open, closed, merged)
            "sort": "updated",  # Sort by the updated_at field
            "direction": "desc",  # Descending order to process newest first
            "per_page": per_page,
            "page": page
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        prs = response.json()

        # Convert last_updated to a timezone-aware datetime object
        last_updated_dt = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # Filter pull requests locally based on the last_updated timestamp
        filtered_prs = []
        for pr in prs:
            pr_updated = datetime.strptime(pr['updated_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if pr_updated <= last_updated_dt:
                break  # Stop fetching if we reach pull requests older than last_updated
            filtered_prs.append(pr)

        return filtered_prs

    def fetch_commits_for_pull_request(self, pull_number: int, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch commits for a specific pull request."""
        url = f"https://api.github.com/repos/{self.repo}/pulls/{pull_number}/commits"
        params = {
            "per_page": per_page,
            "page": page
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def normalize_pull_request(self, pr: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": pr["id"],
            "number": pr["number"],
            "title": pr["title"],
            "user_login": pr["user"]["login"],
            "state": pr["state"],
            "created_at": pr["created_at"],
            "updated_at": pr["updated_at"],
            "closed_at": pr["closed_at"],
            "merged_at": pr["merged_at"],
            "html_url": pr["html_url"]
        }

    def normalize_commit(self, commit: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "sha": commit["sha"],
            "author_name": commit["commit"]["author"]["name"],
            "author_email": commit["commit"]["author"]["email"],
            "author_date": commit["commit"]["author"]["date"],
            "committer_name": commit["commit"]["committer"]["name"],
            "committer_email": commit["commit"]["committer"]["email"],
            "commit_date": commit["commit"]["committer"]["date"],
            "message": commit["commit"]["message"],
            "html_url": commit["html_url"]
        }