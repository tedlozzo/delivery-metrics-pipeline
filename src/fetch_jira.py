import os
import requests
import json
import logging
import duckdb
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

from utils.jira_utils import validate_jira_config, setup_jira_auth, format_jira_updated_for_jql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()

JIRA_BASE_URL = os.getenv('JIRA_BASE_URL')
JIRA_API_VERSION = os.getenv('JIRA_API_VERSION', '2')
JIRA_PROJECT_KEY = os.getenv('JIRA_PROJECT_KEY')
DB_PATH = os.getenv('DUCKDB_PATH', 'jira_data.duckdb')

class JiraFetcher:
    def __init__(self):
        validate_jira_config()
        self.auth_headers = setup_jira_auth()

    def get_last_updated(self, con) -> Optional[str]:
        try:
            result = con.execute("""
                SELECT MAX(JSON_EXTRACT_STRING(fields, '$.updated')) as max_updated 
                FROM jira_issues
            """).fetchone()
            if result and result[0]:
                return format_jira_updated_for_jql(result[0])
        except Exception as e:
            logger.info(f"No existing data found or error querying: {e}")
        return None
    
    def fetch_issues(self, start_at: int = 0, max_results: int = 100, 
                    latest_update: Optional[str] = None) -> Dict[str, Any]:
        """Fetch issues from JIRA API."""
        url = f"{JIRA_BASE_URL}rest/api/{JIRA_API_VERSION}/search"
        
        # Build JQL query
        if latest_update is None:
            jql = f'project = "{JIRA_PROJECT_KEY}" order by updated ASC'
        else:
            jql = f'project = "{JIRA_PROJECT_KEY}" AND updated >= "{latest_update}" order by updated ASC'
        
        params = {
            'jql': jql,
            'startAt': start_at,
            'maxResults': max_results,
            'fields': '*all'
        }
        
        logger.info(f"Fetching issues: {jql} | Start: {start_at} | Max: {max_results}")
        
        response = requests.get(url, headers=self.auth_headers, params=params)
        self._handle_response_errors(response)
        
        return response.json()
    
    def fetch_issue_changelog(self, issue_key: str) -> List[Dict[str, Any]]:
        """Fetch changelog for a specific issue."""
        if JIRA_API_VERSION == '2':
            # For API v2, use expand=changelog
            url = f"{JIRA_BASE_URL}rest/api/{JIRA_API_VERSION}/issue/{issue_key}"
            params = {'expand': 'changelog'}
            
            response = requests.get(url, headers=self.auth_headers, params=params)
            self._handle_response_errors(response)
            data = response.json()
            
            changelog = data.get('changelog', {})
            return changelog.get('histories', [])
        else:
            # For API v3, use dedicated changelog endpoint
            url = f"{JIRA_BASE_URL}rest/api/{JIRA_API_VERSION}/issue/{issue_key}/changelog"
            all_entries = []
            start_at = 0
            max_results = 100
            
            while True:
                params = {'startAt': start_at, 'maxResults': max_results}
                response = requests.get(url, headers=self.auth_headers, params=params)
                self._handle_response_errors(response)
                data = response.json()
                
                all_entries.extend(data['values'])
                if start_at + max_results >= data['total']:
                    break
                start_at += max_results
            
            return all_entries
    
    def _handle_response_errors(self, response):
        """Handle common API response errors."""
        if response.status_code == 401:
            logger.error("Authentication failed. Check JIRA_EMAIL and JIRA_AUTH_TOKEN.")
            raise Exception("Authentication failed")
        elif response.status_code == 404:
            logger.error("API endpoint not found. Check JIRA_BASE_URL and JIRA_API_VERSION.")
            raise Exception("API endpoint not found")
        elif response.status_code == 403:
            logger.error("Access forbidden. Check permissions for the JIRA project.")
            raise Exception("Access forbidden")
        elif response.status_code != 200:
            logger.error(f"API request failed: {response.status_code} - {response.text}")
            raise Exception(f"API request failed: {response.status_code}")
        
        response.raise_for_status()
    
    def setup_database(self, con):
        """Setup database tables."""
        # Main issues table with JSON fields
        con.execute("""
            CREATE TABLE IF NOT EXISTS jira_issues (
                key TEXT PRIMARY KEY,
                id TEXT,
                fields JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Flat changelog table
        con.execute("""
            CREATE TABLE IF NOT EXISTS jira_changelog (
                id TEXT,
                issue_key TEXT,
                created TIMESTAMP,
                author_account_id TEXT,
                author_display_name TEXT,
                field TEXT,
                field_type TEXT,
                from_value TEXT,
                from_string TEXT,
                to_value TEXT,
                to_string TEXT,
                PRIMARY KEY (id, field)
            )
        """)
        
        # Links table without strict foreign key constraints
        con.execute("""
            CREATE TABLE IF NOT EXISTS jira_links (
                source_issue_key TEXT,
                target_issue_key TEXT,
                link_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_issue_key, target_issue_key, link_type)
            )
        """)
    
    def upsert_issues(self, con, issues: List[Dict[str, Any]]):
        """Upsert issues into the database."""
        if not issues:
            logger.info("No issues to upsert.")
            return
        
        # Prepare data for upsert
        issue_data = []
        for issue in issues:
            issue_data.append({
                'key': issue['key'],
                'id': issue['id'],
                'fields': json.dumps(issue['fields'])
            })
        
        df = pd.DataFrame(issue_data)
        con.execute("CREATE OR REPLACE TEMP TABLE _new_issues AS SELECT * FROM df")
        
        # Upsert using ON CONFLICT - fix CURRENT_TIMESTAMP to now()
        con.execute("""
            INSERT INTO jira_issues (key, id, fields, updated_at)
            SELECT key, id, fields::JSON, now() FROM _new_issues
            ON CONFLICT (key) DO UPDATE SET
                id = excluded.id,
                fields = excluded.fields,
                updated_at = now()
        """)
    
    def upsert_changelog(self, con, issue_key: str, changelog_entries: List[Dict[str, Any]]):
        """Upsert changelog entries into the database."""
        if not changelog_entries:
            return
        
        flat_changelog_data = []
        for entry in changelog_entries:
            changelog_id = entry['id']
            created = entry['created']
            author_account_id = entry.get('author', {}).get('accountId', '')
            author_display_name = entry.get('author', {}).get('displayName', '')
            
            # Flatten changelog items
            for item in entry.get('items', []):
                flat_changelog_data.append({
                    'id': changelog_id,
                    'issue_key': issue_key,
                    'created': created,
                    'author_account_id': author_account_id,
                    'author_display_name': author_display_name,
                    'field': item.get('field', ''),
                    'field_type': item.get('fieldtype', ''),
                    'from_value': item.get('from', ''),
                    'from_string': item.get('fromString', ''),
                    'to_value': item.get('to', ''),
                    'to_string': item.get('toString', '')
                })
        
        if flat_changelog_data:
            df = pd.DataFrame(flat_changelog_data)
            con.execute("CREATE OR REPLACE TEMP TABLE _new_flat_changelog AS SELECT * FROM df")
            
            con.execute("""
                INSERT INTO jira_changelog (
                    id, issue_key, created, author_account_id, author_display_name,
                    field, field_type, from_value, from_string, to_value, to_string
                )
                SELECT id, issue_key, created::TIMESTAMP, author_account_id, author_display_name,
                       field, field_type, from_value, from_string, to_value, to_string
                FROM _new_flat_changelog
                ON CONFLICT (id, field) DO UPDATE SET
                    created = excluded.created,
                    author_account_id = excluded.author_account_id,
                    author_display_name = excluded.author_display_name,
                    from_value = excluded.from_value,
                    from_string = excluded.from_string,
                    to_value = excluded.to_value,
                    to_string = excluded.to_string
            """)
    
    def upsert_links(self, con, links: List[Dict[str, str]]):
        """Upsert issue links into the database."""
        if not links:
            logger.info("No links to upsert.")
            return
        
        # Prepare data for upsert
        df = pd.DataFrame(links)
        con.execute("CREATE OR REPLACE TEMP TABLE _new_links AS SELECT * FROM df")
        
        con.execute("""
            INSERT INTO jira_links (source_issue_key, target_issue_key, link_type, created_at)
            SELECT source_issue_key, target_issue_key, link_type, now() FROM _new_links
            ON CONFLICT (source_issue_key, target_issue_key, link_type) DO NOTHING
        """)
    
    def extract_links(self, issue: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract issue links from the fields.issuelinks field."""
        links = []
        issue_key = issue['key']
        issue_links = issue.get('fields', {}).get('issuelinks', [])
        
        for link in issue_links:
            link_type = link.get('type', {}).get('name', '')  # e.g., "Blocks", "Relates to"
            
            # Check for outward and inward links
            if 'outwardIssue' in link:
                links.append({
                    'source_issue_key': issue_key,
                    'target_issue_key': link['outwardIssue']['key'],
                    'link_type': link_type
                })
            if 'inwardIssue' in link:
                links.append({
                    'source_issue_key': link['inwardIssue']['key'],
                    'target_issue_key': issue_key,
                    'link_type': link_type
                })
        
        return links
    
    def run(self):
        """Main execution method."""
        con = duckdb.connect(DB_PATH)
        self.setup_database(con)
        
        # Get last updated timestamp
        last_updated = self.get_last_updated(con)
        logger.info(f"Last updated timestamp: {last_updated}")
        
        start_at = 0
        max_results = 100
        total_processed = 0
        
        while True:
            # Fetch issues
            response = self.fetch_issues(start_at, max_results, last_updated)
            issues = response.get('issues', [])
            
            if not issues:
                logger.info("No more issues to process.")
                break
            
            logger.info(f"Processing {len(issues)} issues from offset {start_at}")
            
            # Fetch changelog and links for each issue
            for issue in issues:
                issue_key = issue['key']
                changelog = self.fetch_issue_changelog(issue_key)
                # links = self.extract_links(issue)
                
                # Upsert issue, changelog, and links separately
                self.upsert_issues(con, [issue])
                self.upsert_changelog(con, issue_key, changelog)
                # self.upsert_links(con, links)
            
            total_processed += len(issues)
            logger.info(f"Total issues processed: {total_processed}")
            
            # Check if we've fetched all available issues
            if start_at + max_results >= response['total']:
                break
            
            start_at += max_results
        
        logger.info(f"Completed. Total issues processed: {total_processed}")
        con.close()

def main():
    try:
        fetcher = JiraFetcher()
        fetcher.run()
    except Exception as e:
        logger.error(f"Error running JIRA fetcher: {e}")
        raise

if __name__ == "__main__":
    main()
