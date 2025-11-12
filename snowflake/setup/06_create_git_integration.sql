/*
=============================================================================
SNOWFLAKE GIT INTEGRATION SETUP
=============================================================================

Purpose: Connect Snowflake to GitHub repository for version-controlled
         code deployment and automated synchronization.

Prerequisites:
1. Repository uploaded to GitHub
2. GitHub Personal Access Token (for private repos)
   - Generate at: https://github.com/settings/tokens
   - Required scopes: repo (full control of private repositories)
3. ACCOUNTADMIN role or appropriate privileges

Usage:
    snowsql -a <account> -u <user> -f snowflake/setup/06_create_git_integration.sql

Documentation: docs/GITHUB_DEPLOYMENT_GUIDE.md
=============================================================================
*/

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- STEP 1: Create API Integration for GitHub
-- =============================================================================

-- This enables Snowflake to communicate with GitHub's API
-- IMPORTANT: Replace <your-github-org> with your GitHub username or organization

CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<your-github-org>/')
  ALLOWED_AUTHENTICATION_SECRETS = ()
  ENABLED = TRUE
  COMMENT = 'API integration for GitHub repository access';

-- Verify integration was created
SHOW API INTEGRATIONS LIKE 'github_api_integration';

-- View integration details
DESC API INTEGRATION github_api_integration;

-- =============================================================================
-- STEP 2: Create Secret for GitHub Authentication (PRIVATE REPOS ONLY)
-- =============================================================================

-- If your repository is PUBLIC, you can skip this step.
-- If PRIVATE, you need to create a secret with your GitHub credentials.

-- IMPORTANT:
-- 1. Generate a Personal Access Token at: https://github.com/settings/tokens
-- 2. Select scope: repo (full control of private repositories)
-- 3. Replace <github-username> and <github-personal-access-token> below

/*
CREATE OR REPLACE SECRET github_secret
  TYPE = password
  USERNAME = '<github-username>'
  PASSWORD = '<github-personal-access-token>'
  COMMENT = 'GitHub authentication credentials for private repository access';

-- Verify secret was created
SHOW SECRETS LIKE 'github_secret';
*/

-- =============================================================================
-- STEP 3: Create Git Repository Object
-- =============================================================================

-- This creates a reference to your GitHub repository within Snowflake
-- The repository will be cloned (shallow clone) into Snowflake

USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- IMPORTANT: Replace with your actual GitHub repository URL
-- Format: https://github.com/<username-or-org>/<repository-name>

-- FOR PUBLIC REPOSITORIES:
CREATE OR REPLACE GIT REPOSITORY snowflake_panel_demo_repo
  ORIGIN = 'https://github.com/<your-github-org>/snowflake-panel-demo'
  API_INTEGRATION = github_api_integration
  COMMENT = 'Customer 360 Analytics Platform - GitHub repository integration';

-- FOR PRIVATE REPOSITORIES (uncomment and use this instead):
/*
CREATE OR REPLACE GIT REPOSITORY snowflake_panel_demo_repo
  ORIGIN = 'https://github.com/<your-github-org>/snowflake-panel-demo'
  API_INTEGRATION = github_api_integration
  GIT_CREDENTIALS = github_secret
  COMMENT = 'Customer 360 Analytics Platform - GitHub repository integration (private)';
*/

-- Verify repository was created
SHOW GIT REPOSITORIES LIKE 'snowflake_panel_demo_repo';

-- View repository details
DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- =============================================================================
-- STEP 4: Fetch Latest Code from GitHub
-- =============================================================================

-- This pulls the latest commits from the remote repository
-- Run this whenever you push new code to GitHub

ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- =============================================================================
-- STEP 5: Verify Repository Contents
-- =============================================================================

-- List files in the main branch
LS @snowflake_panel_demo_repo/branches/main/;

-- List files in specific directories
LS @snowflake_panel_demo_repo/branches/main/streamlit/;
LS @snowflake_panel_demo_repo/branches/main/snowflake/;
LS @snowflake_panel_demo_repo/branches/main/dbt_customer_analytics/;

-- =============================================================================
-- STEP 6: Deploy Streamlit App from Git Repository
-- =============================================================================

-- Option A: Deploy Streamlit app directly from Git repository
-- This creates a Streamlit app that always uses the latest code from Git

CREATE OR REPLACE STREAMLIT customer_360_app
  ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/main/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH'
  COMMENT = 'Customer 360 Analytics Dashboard - deployed from GitHub';

-- View Streamlit app details
SHOW STREAMLIT APPS LIKE 'customer_360_app';
DESC STREAMLIT customer_360_app;

-- Get Streamlit app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app') as streamlit_url;

-- =============================================================================
-- STEP 7: Execute SQL Scripts from Git Repository (Optional)
-- =============================================================================

-- You can execute SQL files directly from the Git repository
-- This is useful for running setup scripts or migrations

-- Example: Execute database setup script
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/setup/01_create_database.sql;

-- Example: Execute ML model training script
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/ml/01_train_churn_model.sql;

-- =============================================================================
-- STEP 8: Set Up Automated Fetch (Optional)
-- =============================================================================

-- Create a task to automatically fetch from GitHub every hour
-- This keeps your Snowflake repository in sync with GitHub

/*
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE TASK auto_fetch_github
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour at minute 0
AS
  ALTER GIT REPOSITORY CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo FETCH;

-- Start the task
ALTER TASK auto_fetch_github RESUME;

-- Verify task is running
SHOW TASKS LIKE 'auto_fetch_github';

-- Check task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  TASK_NAME => 'auto_fetch_github',
  SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

-- To pause the task:
-- ALTER TASK auto_fetch_github SUSPEND;

-- To delete the task:
-- DROP TASK auto_fetch_github;
*/

-- =============================================================================
-- USEFUL COMMANDS FOR ONGOING MANAGEMENT
-- =============================================================================

-- Fetch latest code from GitHub (run after pushing new code)
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- Switch to a different branch
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo SET BRANCH = 'develop';

-- Switch to a specific tag
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo SET TAG = 'v1.0.0';

-- Switch to a specific commit
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo SET COMMIT = '<commit-sha>';

-- View current branch/tag/commit
-- DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- List all branches available
-- LS @snowflake_panel_demo_repo/branches/;

-- Refresh Streamlit app after fetching new code
-- ALTER STREAMLIT customer_360_app REFRESH;

-- =============================================================================
-- TROUBLESHOOTING
-- =============================================================================

-- Check API integration status
-- SHOW API INTEGRATIONS LIKE 'github_api_integration';
-- DESC API INTEGRATION github_api_integration;

-- Check if repository is accessible
-- LS @snowflake_panel_demo_repo/branches/main/;

-- Check Streamlit app logs
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.STREAMLIT_EVENT_HISTORY(
--   STREAMLIT_NAME => 'customer_360_app'
-- ))
-- ORDER BY TIMESTAMP DESC
-- LIMIT 100;

-- Common Issues:
-- 1. "Git repository not found" - Check ORIGIN URL is correct
-- 2. "Authentication failed" - Regenerate GitHub PAT with correct scopes
-- 3. "Streamlit app not found" - Check ROOT_LOCATION path matches repo structure
-- 4. "Files not visible" - Run ALTER GIT REPOSITORY ... FETCH;

-- =============================================================================
-- CLEANUP (IF NEEDED)
-- =============================================================================

-- To remove Git integration (WARNING: This will delete all Git objects)
/*
DROP STREAMLIT IF EXISTS customer_360_app;
DROP TASK IF EXISTS auto_fetch_github;
DROP GIT REPOSITORY IF EXISTS snowflake_panel_demo_repo;
DROP SECRET IF EXISTS github_secret;
DROP API INTEGRATION IF EXISTS github_api_integration;
*/

-- =============================================================================
-- NEXT STEPS
-- =============================================================================

/*
After completing this setup:

1. ✅ Test Streamlit app:
   - Get URL: SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app');
   - Open in browser and verify all 4 tabs work

2. ✅ Push code changes to GitHub:
   - git add .
   - git commit -m "Update feature"
   - git push origin main

3. ✅ Sync Snowflake with GitHub:
   - ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;
   - ALTER STREAMLIT customer_360_app REFRESH;

4. ✅ (Optional) Set up GitHub Actions for automated deployment:
   - See: .github/workflows/deploy-streamlit.yml
   - Configure GitHub secrets for SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

5. ✅ (Optional) Connect dbt Cloud:
   - See: docs/DBT_CLOUD_SETUP_GUIDE.md

For complete documentation, see:
- docs/GITHUB_DEPLOYMENT_GUIDE.md
- docs/ONBOARDING_GUIDE.md
*/

-- =============================================================================
-- END OF SCRIPT
-- =============================================================================

SELECT 'Git integration setup complete! Run FETCH to sync latest code from GitHub.' as status;
