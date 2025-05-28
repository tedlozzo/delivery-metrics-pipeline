# Delivery Metrics Pipeline

This repository provides a robust, extensible data pipeline for fetching and storing delivery metrics from GitHub and JIRA into a DuckDB database. It is designed for reliability, incremental updates, and easy analytics.

---

## Features

- **GitHub Integration**: Fetches pull requests and commits, supports upsert and deduplication.
- **JIRA Integration**: Fetches issues and changelogs, stores all issue fields as JSON, supports upsert and deduplication.
- **DuckDB Storage**: All data is stored in DuckDB tables for fast analytics and easy querying.
- **Extensible Design**: Strategy pattern for Git providers (GitHub, GitLab-ready), utility modules for JIRA.
- **Incremental Updates**: Only fetches new or updated records since the last run.
- **Logging & Error Handling**: Informative logs and robust error handling throughout.

---

## Folder Structure

```
delivery-metrics-pipeline/
├── src/
│   ├── fetch_github_pull_requests.py
│   ├── fetch_jira.py
│   └── utils/
│       ├── github_provider.py
│       ├── data_fetcher.py
│       └── jira_utils.py
├── .env
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-org/delivery-metrics-pipeline.git
cd delivery-metrics-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the root directory with the following variables:

#### For GitHub:

```
GITHUB_REPO=your-org/your-repo
GITHUB_API_KEY=your_github_token
DUCKDB_PATH=github_data.duckdb
```

#### For JIRA:

```
JIRA_EMAIL=your-email@example.com
JIRA_AUTH_TOKEN=your-jira-api-token
JIRA_BASE_URL=https://your-domain.atlassian.net/
JIRA_PROJECT_KEY=YOURPROJECT
DUCKDB_PATH=jira_data.duckdb
```

---

## Usage

### Fetch GitHub Pull Requests and Commits

```bash
python src/fetch_github_pull_requests.py
```

### Fetch JIRA Issues and Changelogs

```bash
python src/fetch_jira.py
```

---

## Database Schema

### GitHub

- **pull_requests**: All PR metadata (JSON fields, upserted by PR ID)
- **commits**: All commit metadata (upserted by SHA)
- **pr_commits**: Junction table linking PRs and commits

### JIRA

- **jira_issues**: All issue fields as JSON, upserted by issue key
- **jira_changelog**: Structured changelog entries, upserted by changelog ID

---

## Query Examples

**Get all commits for a PR:**
```sql
SELECT c.*
FROM pull_requests pr
JOIN pr_commits pc ON pr.id = pc.pr_id
JOIN commits c ON pc.commit_sha = c.sha
WHERE pr.number = 123;
```

**Get all JIRA issues updated in the last 7 days:**
```sql
SELECT key, JSON_EXTRACT_STRING(fields, '$.summary') AS summary
FROM jira_issues
WHERE JSON_EXTRACT_STRING(fields, '$.updated') >= strftime('%Y-%m-%dT%H:%M:%S.000+0000', now() - INTERVAL 7 DAY);
```

---

## Extending

- To add GitLab or other providers, implement the provider interface and plug into the strategy pattern.
- To add new analytics, simply query the DuckDB database.
