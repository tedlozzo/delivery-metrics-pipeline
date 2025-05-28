import os
import base64
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

def validate_jira_config():
    required_vars = ['JIRA_BASE_URL', 'JIRA_PROJECT_KEY']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        raise ValueError(f"Missing required environment variables: {missing}")
    base_url = os.getenv('JIRA_BASE_URL')
    if not base_url.endswith('/'):
        os.environ['JIRA_BASE_URL'] = base_url + '/'

def setup_jira_auth():
    JIRA_EMAIL = os.getenv('JIRA_EMAIL')
    JIRA_AUTH_TOKEN = os.getenv('JIRA_AUTH_TOKEN')
    if not JIRA_EMAIL or not JIRA_AUTH_TOKEN:
        logger.warning("No authentication credentials provided")
        return {}
    auth_string = f"{JIRA_EMAIL}:{JIRA_AUTH_TOKEN}"
    auth_bytes = base64.b64encode(auth_string.encode('utf-8'))
    auth_header = auth_bytes.decode('utf-8')
    return {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json",
        "X-Atlassian-Token": "no-check"
    }

def format_jira_updated_for_jql(ts: Optional[str]) -> Optional[str]:
    """Convert JIRA updated timestamp to JQL-compatible format."""
    if not ts:
        return None
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%z")
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        logger.warning(f"Could not parse timestamp '{ts}': {e}")
        return None