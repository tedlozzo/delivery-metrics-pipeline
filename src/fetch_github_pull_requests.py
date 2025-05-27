import os
from dotenv import load_dotenv
from utils.github_provider import GitHubProvider
from utils.data_fetcher import DataFetcher

load_dotenv()

def main():
    # Configuration
    GITHUB_REPO = os.getenv("GITHUB_REPO")
    GITHUB_API_KEY = os.getenv("GITHUB_API_KEY")
    DB_PATH = os.getenv("DUCKDB_PATH", "github_data.duckdb")
    
    assert GITHUB_REPO, "Missing GITHUB_REPO env var"
    assert GITHUB_API_KEY, "Missing GITHUB_API_KEY env var"
    
    # Create provider and fetcher
    provider = GitHubProvider(GITHUB_REPO, GITHUB_API_KEY)
    fetcher = DataFetcher(provider, DB_PATH)
    
    # Run the data pipeline
    fetcher.run()

if __name__ == "__main__":
    main()
