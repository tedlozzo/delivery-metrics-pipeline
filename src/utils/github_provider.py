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
    
    def fetch_pull_requests(self, since: str, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        url = f"https://api.github.com/repos/{self.repo}/pulls"
        params = {
            "state": "all",
            "sort": "updated", 
            "direction": "asc",
            "per_page": per_page,
            "page": page
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Handle since date comparison - ensure both datetimes are timezone-aware
        if isinstance(since, str):
            # Parse the since string and make it timezone-aware if it isn't
            if since.endswith('Z'):
                since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            else:
                since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
        else:
            since_dt = since
            
        filtered = []
        for pr in data:
            # Parse PR updated_at and make it timezone-aware
            pr_updated = datetime.strptime(pr['updated_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if pr_updated > since_dt:
                filtered.append(pr)
        
        return filtered

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