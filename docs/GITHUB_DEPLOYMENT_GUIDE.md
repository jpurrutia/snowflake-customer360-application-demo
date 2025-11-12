# GitHub Deployment Guide - Snowflake Customer 360 Analytics Platform

**Version**: 1.0
**Date**: 2025-11-12
**Purpose**: Step-by-step guide for deploying code from GitHub to Snowflake

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Deployment Options](#deployment-options)
4. [Option 1: Native Git Integration (Recommended)](#option-1-native-git-integration-recommended)
5. [Option 2: GitHub Actions CI/CD](#option-2-github-actions-cicd)
6. [Option 3: Manual Deployment](#option-3-manual-deployment)
7. [Ongoing Workflow](#ongoing-workflow)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

---

## Overview

This guide covers three methods for deploying your Snowflake Customer 360 Analytics Platform from GitHub to Snowflake:

| Method | Setup Time | Automation | Best For |
|--------|------------|------------|----------|
| **Native Git Integration** | 30 min | Manual FETCH | Learning, testing |
| **GitHub Actions** | 1 hour | Fully automated | Production deployments |
| **Manual Deployment** | 15 min | None | Quick testing |

### What Gets Deployed

- **Streamlit Application**: 4-tab dashboard (`streamlit/`)
- **SQL Scripts**: Setup, ML, bronze layer scripts (`snowflake/`)
- **dbt Models**: Via dbt Cloud (see separate guide)

---

## Prerequisites

### 1. GitHub Account & Repository

- Create a GitHub account: https://github.com/signup
- Repository visibility:
  - **Public**: Free, no authentication needed
  - **Private**: Requires Personal Access Token

### 2. Snowflake Account

- Trial or production account
- Role with privileges:
  - `CREATE API INTEGRATION`
  - `CREATE GIT REPOSITORY`
  - `CREATE STREAMLIT`
  - `CREATE SECRET` (for private repos)
- Recommended role: `ACCOUNTADMIN` (for setup), `DATA_ANALYST` (for usage)

### 3. Local Setup

```bash
# Verify git is installed
git --version

# Verify Python is installed
python --version  # Should be 3.10+

# Verify SnowSQL is installed (optional but recommended)
snowsql --version
```

### 4. GitHub Personal Access Token (Private Repos Only)

If your repository is **private**, you'll need a Personal Access Token:

1. Go to: https://github.com/settings/tokens
2. Click "Generate new token" → "Generate new token (classic)"
3. Select scopes:
   - ✅ `repo` (Full control of private repositories)
4. Set expiration (recommend 90 days)
5. Click "Generate token"
6. **Copy the token** (you won't see it again!)

---

## Deployment Options

### Decision Tree

```
Do you want automated deployment on git push?
├─ YES → Use GitHub Actions (Option 2)
└─ NO
   ├─ Do you want version-controlled deployment?
   │  ├─ YES → Use Native Git Integration (Option 1)
   │  └─ NO → Use Manual Deployment (Option 3)
```

---

## Option 1: Native Git Integration (Recommended)

**Pros**:
- Native Snowflake feature (no external tools)
- Version-controlled deployments
- Can execute SQL scripts from Git
- One-time setup

**Cons**:
- Manual FETCH required after git push
- No automated testing

### Step 1: Push Code to GitHub

```bash
cd /Users/jpurrutia/projects/snowflake-panel-demo

# Initialize git (if not already done)
git init
git add .
git commit -m "Initial commit: Customer 360 Analytics Platform"

# Add remote (replace with your GitHub username/org)
git remote add origin https://github.com/<your-username>/snowflake-panel-demo.git

# Push to GitHub
git push -u origin main
```

**Verify**: Visit https://github.com/<your-username>/snowflake-panel-demo and confirm files are visible.

### Step 2: Create Git Integration in Snowflake

Run the SQL setup script:

```bash
# Download the script first if you haven't pushed to GitHub yet
snowsql -a <account> -u <user> -f snowflake/setup/06_create_git_integration.sql
```

Or manually run these SQL commands:

```sql
USE ROLE ACCOUNTADMIN;

-- 1. Create API Integration
CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<your-username>/')
  ENABLED = TRUE;

-- 2. Create Secret (PRIVATE REPOS ONLY - skip for public repos)
CREATE OR REPLACE SECRET github_secret
  TYPE = password
  USERNAME = '<github-username>'
  PASSWORD = '<github-personal-access-token>';

-- 3. Create Git Repository
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- For PUBLIC repositories:
CREATE OR REPLACE GIT REPOSITORY snowflake_panel_demo_repo
  ORIGIN = 'https://github.com/<your-username>/snowflake-panel-demo'
  API_INTEGRATION = github_api_integration;

-- For PRIVATE repositories:
CREATE OR REPLACE GIT REPOSITORY snowflake_panel_demo_repo
  ORIGIN = 'https://github.com/<your-username>/snowflake-panel-demo'
  API_INTEGRATION = github_api_integration
  GIT_CREDENTIALS = github_secret;

-- 4. Fetch code from GitHub
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- 5. Verify files are accessible
LS @snowflake_panel_demo_repo/branches/main/;
LS @snowflake_panel_demo_repo/branches/main/streamlit/;
```

**Expected Output**:
```
API_INTEGRATION: github_api_integration created
GIT_REPOSITORY: snowflake_panel_demo_repo created
Files visible in LS command
```

### Step 3: Deploy Streamlit App

```sql
CREATE OR REPLACE STREAMLIT customer_360_app
  ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/main/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH'
  COMMENT = 'Customer 360 Analytics Dashboard';

-- Get app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app') as app_url;
```

**Expected Output**:
```
STREAMLIT: customer_360_app created
APP_URL: https://<account>.snowflakecomputing.com/streamlit/CUSTOMER_ANALYTICS.GOLD.customer_360_app
```

### Step 4: Test the Deployment

1. **Open Streamlit app** using the URL from Step 3
2. **Test all 4 tabs**:
   - Segment Explorer
   - Customer 360
   - AI Assistant
   - Campaign Simulator
3. **Verify data loads** correctly

### Step 5: Ongoing Updates

When you make code changes:

```bash
# 1. Make changes locally
vim streamlit/app.py

# 2. Commit and push to GitHub
git add .
git commit -m "Update Streamlit UI"
git push origin main

# 3. Sync Snowflake with GitHub
snowsql -a <account> -u <user> -q "
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;
ALTER STREAMLIT customer_360_app REFRESH;
"

# 4. Verify changes in Streamlit app
```

---

## Option 2: GitHub Actions CI/CD

**Pros**:
- Fully automated deployment on git push
- Can run tests before deployment
- Production-grade workflow
- No manual FETCH needed

**Cons**:
- Requires GitHub Actions setup
- Needs to configure secrets
- Slightly more complex

### Step 1: Push Code to GitHub (Same as Option 1)

```bash
cd /Users/jpurrutia/projects/snowflake-panel-demo
git add .
git commit -m "Initial commit with GitHub Actions"
git push origin main
```

### Step 2: Configure GitHub Secrets

1. **Go to your repository on GitHub**
2. **Navigate to**: Settings → Secrets and variables → Actions
3. **Click "New repository secret"**
4. **Add these secrets**:

| Secret Name | Value | Example |
|-------------|-------|---------|
| `SNOWFLAKE_ACCOUNT` | Your account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Your Snowflake username | `analyst_user` |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password | `YourSecurePassword123!` |
| `SNOWFLAKE_ROLE` | Role to use | `DATA_ANALYST` |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | Database name | `CUSTOMER_ANALYTICS` |
| `SNOWFLAKE_SCHEMA` | Schema name | `GOLD` |

**Important**: Never commit passwords or credentials to Git. Always use GitHub Secrets.

### Step 3: Verify GitHub Actions Workflow

The workflow file is already created at `.github/workflows/deploy-streamlit.yml`.

**Review the workflow**:

```yaml
name: Deploy Streamlit to Snowflake

on:
  push:
    branches: [main]
    paths: ['streamlit/**']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Snowflake CLI
        run: pip install snowflake-cli-labs

      - name: Deploy
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          # ... other secrets
        run: |
          cd streamlit
          snow streamlit deploy --name customer_360_app --replace
```

### Step 4: Trigger Deployment

```bash
# Make a change to trigger deployment
echo "# Updated" >> streamlit/README.md
git add streamlit/README.md
git commit -m "Trigger deployment test"
git push origin main
```

**Monitor deployment**:
1. Go to GitHub repository → Actions tab
2. Click on the running workflow
3. Watch deployment progress
4. If successful, app is deployed to Snowflake

### Step 5: Verify Deployment

```sql
-- Check Streamlit app status
SHOW STREAMLIT APPS LIKE 'customer_360_app';

-- Get app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app');
```

### Step 6: Ongoing Updates

With GitHub Actions, deployment is automatic:

```bash
# 1. Make changes locally
vim streamlit/tabs/segment_explorer.py

# 2. Commit and push
git add .
git commit -m "Update Segment Explorer filters"
git push origin main

# 3. GitHub Actions automatically:
#    - Runs tests (if configured)
#    - Deploys to Snowflake
#    - Refreshes Streamlit app

# 4. Check deployment status on GitHub Actions tab
```

---

## Option 3: Manual Deployment

**Pros**:
- Simplest setup
- No Git integration needed
- Good for quick testing

**Cons**:
- No version control integration
- Manual file upload required
- No automation

### Step 1: Upload Files to Snowflake Stage

```sql
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- Create internal stage
CREATE STAGE IF NOT EXISTS streamlit_stage;

-- Upload files (from SnowSQL)
PUT file:///Users/jpurrutia/projects/snowflake-panel-demo/streamlit/* @streamlit_stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### Step 2: Deploy Streamlit App

```sql
CREATE OR REPLACE STREAMLIT customer_360_app
  ROOT_LOCATION = '@streamlit_stage/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
```

### Step 3: Ongoing Updates

```bash
# 1. Make changes locally
vim streamlit/app.py

# 2. Upload to stage
snowsql -a <account> -u <user> -q "
PUT file:///Users/jpurrutia/projects/snowflake-panel-demo/streamlit/app.py @CUSTOMER_ANALYTICS.GOLD.streamlit_stage/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"

# 3. Refresh Streamlit app
snowsql -a <account> -u <user> -q "
ALTER STREAMLIT CUSTOMER_ANALYTICS.GOLD.customer_360_app REFRESH;
"
```

---

## Ongoing Workflow

### Daily Development Workflow

**Using Native Git Integration**:
```bash
# Morning: Start working
git pull origin main

# Make changes
vim streamlit/app.py

# Test locally
cd streamlit && streamlit run app.py

# Commit and push
git add .
git commit -m "Add new feature"
git push origin main

# Sync Snowflake
snowsql -a <account> -u <user> -q "
ALTER GIT REPOSITORY CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo FETCH;
ALTER STREAMLIT CUSTOMER_ANALYTICS.GOLD.customer_360_app REFRESH;
"
```

**Using GitHub Actions**:
```bash
# Morning: Start working
git pull origin main

# Make changes
vim streamlit/app.py

# Test locally
cd streamlit && streamlit run app.py

# Commit and push (automatic deployment)
git add .
git commit -m "Add new feature"
git push origin main

# Check GitHub Actions for deployment status
# App is automatically deployed and refreshed
```

### Branching Strategy

**Recommended Git Flow**:

```bash
# Feature development
git checkout -b feature/new-filter
# ... make changes ...
git commit -m "Add new filter to Segment Explorer"
git push origin feature/new-filter

# Create pull request on GitHub
# Review and test
# Merge to main → triggers deployment (if using GitHub Actions)
```

**Branch Deployment in Snowflake**:

```sql
-- Deploy from feature branch (testing)
CREATE OR REPLACE STREAMLIT customer_360_app_dev
  ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/feature/new-filter/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';

-- Switch back to main branch (production)
CREATE OR REPLACE STREAMLIT customer_360_app
  ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/main/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
```

---

## Troubleshooting

### Issue 1: Git Repository Not Found

**Error**:
```
SQL execution error: Git repository not found or not accessible.
```

**Solutions**:

1. **Check repository URL**:
   ```sql
   DESC GIT REPOSITORY snowflake_panel_demo_repo;
   -- Verify ORIGIN matches your GitHub URL
   ```

2. **For private repos, verify authentication**:
   ```sql
   SHOW SECRETS LIKE 'github_secret';
   -- Ensure secret exists

   -- Test GitHub PAT is valid
   -- Visit: https://github.com/settings/tokens
   -- Regenerate if expired
   ```

3. **Check API integration**:
   ```sql
   SHOW API INTEGRATIONS LIKE 'github_api_integration';
   DESC API INTEGRATION github_api_integration;
   -- Ensure ENABLED = true
   ```

### Issue 2: Streamlit App Not Loading

**Error**:
```
Streamlit app not found or files missing
```

**Solutions**:

1. **Verify files are accessible**:
   ```sql
   LS @snowflake_panel_demo_repo/branches/main/streamlit/;
   -- Should show app.py and tabs/ directory
   ```

2. **Check ROOT_LOCATION path**:
   ```sql
   DESC STREAMLIT customer_360_app;
   -- Verify ROOT_LOCATION points to correct directory
   ```

3. **Refresh app**:
   ```sql
   ALTER STREAMLIT customer_360_app REFRESH;
   ```

### Issue 3: GitHub Actions Deployment Failed

**Error**: Workflow fails with authentication error

**Solutions**:

1. **Verify GitHub Secrets**:
   - Go to repository → Settings → Secrets
   - Check all required secrets are present
   - Verify no typos in secret names

2. **Test Snowflake credentials locally**:
   ```bash
   snowsql -a <account> -u <user> -p <password>
   SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();
   ```

3. **Check GitHub Actions logs**:
   - Go to Actions tab
   - Click on failed workflow
   - Review error messages

4. **Common fixes**:
   ```yaml
   # If using account with region:
   SNOWFLAKE_ACCOUNT: abc12345.us-east-1

   # If using legacy account locator:
   SNOWFLAKE_ACCOUNT: abc12345
   ```

### Issue 4: Cannot FETCH from GitHub

**Error**:
```
Network error or timeout when fetching from remote repository
```

**Solutions**:

1. **Check network connectivity**:
   ```sql
   -- Snowflake must be able to reach github.com
   -- Check with your IT department if behind corporate firewall
   ```

2. **Verify GitHub is accessible**:
   - Visit: https://github.com/<your-repo>
   - Ensure repository is public OR you have valid credentials

3. **Regenerate GitHub PAT**:
   - Old tokens may have expired
   - https://github.com/settings/tokens

### Issue 5: Files Not Syncing

**Error**: Changes pushed to GitHub but not reflected in Snowflake

**Solutions**:

1. **Run FETCH manually**:
   ```sql
   ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;
   ```

2. **Refresh Streamlit app**:
   ```sql
   ALTER STREAMLIT customer_360_app REFRESH;
   ```

3. **Check which commit is loaded**:
   ```sql
   DESC GIT REPOSITORY snowflake_panel_demo_repo;
   -- Compare COMMIT_SHA with GitHub latest commit
   ```

---

## Best Practices

### Security

1. **Never commit credentials**:
   - Use `.gitignore` for `.env` files
   - Use GitHub Secrets for CI/CD
   - Use Snowflake Secrets for Git credentials

2. **Use least-privilege roles**:
   - Setup: `ACCOUNTADMIN`
   - Deployment: `DATA_ENGINEER`
   - Runtime: `DATA_ANALYST`

3. **Rotate credentials regularly**:
   - GitHub PAT: Every 90 days
   - Snowflake passwords: Every 90 days

### Performance

1. **Use shallow clones** (default):
   - Snowflake only clones latest commit
   - Faster FETCH operations

2. **Minimize FETCH frequency**:
   - Don't fetch on every request
   - Use tasks for periodic sync (hourly)

3. **Cache Streamlit connections**:
   - Already implemented in `streamlit/app.py`
   - Use `@st.cache_resource`

### Collaboration

1. **Use branches for features**:
   ```bash
   git checkout -b feature/new-dashboard
   # ... develop ...
   git push origin feature/new-dashboard
   # Create PR → Review → Merge to main
   ```

2. **Test in dev environment first**:
   ```sql
   -- Deploy to dev schema
   CREATE STREAMLIT customer_360_app_dev
     ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/develop/streamlit'
     ...;
   ```

3. **Document changes**:
   - Use clear commit messages
   - Update README.md
   - Add comments in code

### Deployment

1. **Use GitHub Actions for production**:
   - Automated deployment on merge to main
   - Run tests before deployment
   - Consistent deployment process

2. **Test locally before pushing**:
   ```bash
   cd streamlit
   streamlit run app.py
   # Test all features
   ```

3. **Monitor deployments**:
   - GitHub Actions dashboard
   - Snowflake query history
   - Streamlit app logs

---

## Next Steps

After completing GitHub deployment:

1. ✅ **Set up dbt Cloud** (optional):
   - See: [docs/DBT_CLOUD_SETUP_GUIDE.md](DBT_CLOUD_SETUP_GUIDE.md)
   - Automated dbt runs on git push

2. ✅ **Configure monitoring**:
   - Snowflake resource monitors
   - GitHub Actions notifications
   - Streamlit error tracking

3. ✅ **Document your workflow**:
   - Create team runbook
   - Document deployment process
   - Share access with team

---

## Reference

### Useful Commands

```sql
-- Sync with GitHub
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- Refresh Streamlit app
ALTER STREAMLIT customer_360_app REFRESH;

-- List files in repo
LS @snowflake_panel_demo_repo/branches/main/;

-- Execute SQL from repo
EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/setup/01_create_database.sql;

-- Get app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app');

-- Check deployment history
SELECT * FROM TABLE(INFORMATION_SCHEMA.STREAMLIT_EVENT_HISTORY(
  STREAMLIT_NAME => 'customer_360_app'
)) ORDER BY TIMESTAMP DESC LIMIT 10;
```

### Documentation Links

- **Snowflake Git Integration**: https://docs.snowflake.com/en/developer-guide/git/git-overview
- **Streamlit in Snowflake**: https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit
- **GitHub Actions**: https://docs.github.com/en/actions
- **Snowflake CLI**: https://docs.snowflake.com/en/developer-guide/snowflake-cli/index

---

**End of GitHub Deployment Guide**
