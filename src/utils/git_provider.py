from abc import ABC, abstractmethod
from typing import List, Dict, Any

class GitProvider(ABC):
    @abstractmethod
    def fetch_pull_requests(self, since: str, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch pull requests from the provider."""
        pass
    
    @abstractmethod
    def fetch_commits_for_pull_request(self, pull_number: int, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch commits for a specific pull request."""
        pass
    
    @abstractmethod
    def normalize_pull_request(self, pr: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize PR data to common format."""
        pass
    
    @abstractmethod
    def normalize_commit(self, commit: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize commit data to common format."""
        pass