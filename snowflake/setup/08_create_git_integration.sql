/*
=============================================================================
SNOWFLAKE GIT INTEGRATION - REFERENCE DOCUMENTATION
=============================================================================

⚠️  DO NOT RUN THIS SCRIPT - FOR REFERENCE ONLY ⚠️

This file documents the Git integration that was created via Snowflake UI.

Current Setup:
- Git workspace: Created via Snowflake UI (Data > Git Repositories)
- API Integration: GITHUB_API_INTEGRATION (created via UI)
- Repository: https://github.com/jpurrutia/snowflake-panel-demo (public)
- Deployment: GitHub Actions automatically deploys Streamlit on push to main

Purpose: Shows DDL for existing Git integration and useful management queries

Documentation: docs/GITHUB_DEPLOYMENT_GUIDE.md
=============================================================================
*/

-- =============================================================================
-- CURRENT DEPLOYMENT WORKFLOW
-- =============================================================================
-- 1. Push code to GitHub: git push origin main
-- 2. GitHub Actions workflow triggers automatically
-- 3. Streamlit app deploys to Snowflake
-- 4. No manual FETCH needed - fully automated!

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- REFERENCE: API Integration DDL (Already Exists)
-- =============================================================================

-- This API integration was created via Snowflake UI
-- DO NOT RUN - Shown here for reference only

/*
CREATE OR REPLACE API INTEGRATION github_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/jpurrutia/')
    ENABLED = true
    ALLOWED_AUTHENTICATION_SECRETS = all
    COMMENT = 'API integration for GitHub repository access';
*/


-- =============================================================================
-- USEFUL QUERIES: Check API Integration Status
-- =============================================================================

-- View all API integrations
SHOW API INTEGRATIONS LIKE 'github_api_integration';

-- View integration details
DESC API INTEGRATION github_api_integration;

-- =============================================================================
-- NOTE: GitHub Secret Not Needed (Public Repository)
-- =============================================================================

-- Since the repository is public, no GitHub Personal Access Token is needed.
-- If you later make the repository private, you would create a secret like this:

/*
CREATE OR REPLACE SECRET github_secret
  TYPE = password
  USERNAME = 'jpurrutia'
  PASSWORD = '<github-personal-access-token>'
  COMMENT = 'GitHub authentication credentials for private repository access';

-- Then update the GIT REPOSITORY to use the secret:
ALTER GIT REPOSITORY snowflake_panel_demo_repo SET GIT_CREDENTIALS = github_secret;
*/

-- =============================================================================
-- REFERENCE: Git Repository DDL (Already Exists)
-- =============================================================================

-- This Git repository object was created via Snowflake UI
-- DO NOT RUN - Shown here for reference only

USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

/*
CREATE OR REPLACE GIT REPOSITORY snowflake_panel_demo_repo
  ORIGIN = 'https://github.com/jpurrutia/snowflake-customer360-application-demo'
  API_INTEGRATION = github_api_integration
  COMMENT = 'Customer 360 Analytics Platform - GitHub repository integration';
*/

-- =============================================================================
-- USEFUL QUERIES: Check Git Repository Status
-- =============================================================================

-- View all Git repositories
SHOW GIT REPOSITORIES LIKE 'snowflake_panel_demo_repo';

-- View repository details
DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- =============================================================================
-- USEFUL QUERIES: Manual Fetch from GitHub (If Needed)
-- =============================================================================

-- NOTE: With GitHub Actions, this is automated and usually not needed
-- This pulls the latest commits from the remote repository manually

-- ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- =============================================================================
-- USEFUL QUERIES: Verify Repository Contents
-- =============================================================================

-- List files in the main branch
LS @snowflake_panel_demo_repo/branches/main/;

-- List files in specific directories
LS @snowflake_panel_demo_repo/branches/main/streamlit/;
LS @snowflake_panel_demo_repo/branches/main/snowflake/;
LS @snowflake_panel_demo_repo/branches/main/dbt_customer_analytics/;

-- =============================================================================
-- REFERENCE: Streamlit App Deployment (Automated via GitHub Actions)
-- =============================================================================

-- NOTE: Streamlit app is deployed automatically via GitHub Actions
-- The workflow in .github/workflows/deploy-streamlit.yml handles deployment
-- No manual CREATE STREAMLIT command is needed

-- For reference, the DDL would look like:
/*
CREATE OR REPLACE STREAMLIT customer_360_app
  ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/main/streamlit'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH'
  COMMENT = 'Customer 360 Analytics Dashboard - deployed from GitHub';
*/

-- View Streamlit app details
SHOW STREAMLIT APPS LIKE 'customer_360_app';
DESC STREAMLIT customer_360_app;

-- Get Streamlit app URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app') as streamlit_url;

-- =============================================================================
-- OPTIONAL: Execute SQL Scripts from Git Repository
-- =============================================================================

-- You can execute SQL files directly from the Git repository
-- This is useful for running setup scripts or migrations

-- Example: Execute database setup script
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/setup/01_create_database.sql;

-- Example: Execute ML model training script
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/ml/01_train_churn_model.sql;

-- =============================================================================
-- OPTIONAL: Set Up Automated Fetch Task
-- =============================================================================

-- NOTE: With GitHub Actions, automated fetch is usually not needed
-- But if you want Snowflake to periodically check for updates, you can create a task

-- Example: Create a task to fetch from GitHub every hour
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

-- Fetch latest code from GitHub (if not using GitHub Actions)
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- Switch to a different branch
ALTER GIT REPOSITORY snowflake_panel_demo_repo SET BRANCH = 'develop';

-- Switch to a specific tag
ALTER GIT REPOSITORY snowflake_panel_demo_repo SET TAG = 'v1.0.0';

-- Switch to a specific commit
ALTER GIT REPOSITORY snowflake_panel_demo_repo SET COMMIT = '<commit-sha>';

-- View current branch/tag/commit
DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- List all branches available
LS @snowflake_panel_demo_repo/branches/;

-- Refresh Streamlit app after fetching new code (if not using GitHub Actions)
ALTER STREAMLIT customer_360_app REFRESH;

-- =============================================================================
-- TROUBLESHOOTING
-- =============================================================================

-- Check API integration status
SHOW API INTEGRATIONS LIKE 'github_api_integration';
DESC API INTEGRATION github_api_integration;

-- Check if repository is accessible
LS @snowflake_panel_demo_repo/branches/main/;

-- Check Streamlit app logs
SELECT * FROM TABLE(INFORMATION_SCHEMA.STREAMLIT_EVENT_HISTORY(
  STREAMLIT_NAME => 'customer_360_app'
))
ORDER BY TIMESTAMP DESC
LIMIT 100;

-- Common Issues:
-- 1. "Git repository not found" - Check ORIGIN URL is correct
-- 2. "Authentication failed" - Regenerate GitHub PAT with correct scopes (if private repo)
-- 3. "Streamlit app not found" - Check ROOT_LOCATION path matches repo structure
-- 4. "Files not visible" - Run ALTER GIT REPOSITORY ... FETCH;
-- 5. "GitHub Actions deployment fails" - Check GitHub secrets and workflow configuration

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
Current Deployment Workflow:

1. ✅ Push code changes to GitHub:
   - git add .
   - git commit -m "Update feature"
   - git push origin main

2. ✅ GitHub Actions automatically deploys:
   - Workflow: .github/workflows/deploy-streamlit.yml
   - Deploys Streamlit app to Snowflake automatically
   - No manual FETCH or REFRESH needed!

3. ✅ Test Streamlit app:
   - Get URL: SELECT SYSTEM$GET_STREAMLIT_APP_URL('customer_360_app');
   - Open in browser and verify all 4 tabs work

4. ✅ (Optional) Manual sync (if not using GitHub Actions):
   - ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;
   - ALTER STREAMLIT customer_360_app REFRESH;

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
