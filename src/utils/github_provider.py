import requests
from datetime import datetime
from typing import List, Dict, Any
from utils.git_provider import GitProvider

class GitHubProvider(GitProvider):
    def __init__(self, repo: str, api_key: str):
        self.repo = repo
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/vnd.github+json"
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
        
        # Filter by since date
        filtered = [
            pr for pr in data
            if datetime.strptime(pr['updated_at'], "%Y-%m-%dT%H:%M:%SZ") > since
        ]
        
        return filtered
    
    def fetch_commits(self, since: str, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        url = f"https://api.github.com/repos/{self.repo}/commits"
        params = {
            "since": since,
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