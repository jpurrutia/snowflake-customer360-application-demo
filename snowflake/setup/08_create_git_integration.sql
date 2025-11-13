-- ============================================================================
-- Snowflake Git Integration Setup - REPRODUCIBLE
-- ============================================================================
-- Purpose: Create API integration and Git repository for Customer 360 Platform
-- Repository: https://github.com/jpurrutia/snowflake-customer360-application-demo
-- Run this script to set up Git integration from scratch
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- STEP 1: Create API Integration for GitHub
-- ============================================================================

-- Create API integration to allow Snowflake to access GitHub repositories
CREATE OR REPLACE API INTEGRATION github_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/jpurrutia/')
    ENABLED = true
    COMMENT = 'API integration for GitHub repository access';

-- Verify API integration was created
SHOW API INTEGRATIONS LIKE 'github_api_integration';

-- View integration details
DESC API INTEGRATION github_api_integration;

-- ============================================================================
-- STEP 2: Create Git Repository Object
-- ============================================================================

USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- Create Git repository object pointing to GitHub repo
CREATE OR REPLACE GIT REPOSITORY snowflake_panel_demo_repo
  ORIGIN = 'https://github.com/jpurrutia/snowflake-customer360-application-demo'
  API_INTEGRATION = github_api_integration
  COMMENT = 'Customer 360 Analytics Platform - GitHub repository integration';

-- Verify Git repository was created
SHOW GIT REPOSITORIES LIKE 'snowflake_panel_demo_repo';

-- View repository details
DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- ============================================================================
-- STEP 3: Fetch Latest Code from GitHub
-- ============================================================================

-- Pull the latest code from the main branch
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- ============================================================================
-- STEP 4: Verify Repository Contents
-- ============================================================================

-- List all files in main branch (root)
LS @snowflake_panel_demo_repo/branches/main/;

-- List files in key directories
LS @snowflake_panel_demo_repo/branches/main/streamlit/;
LS @snowflake_panel_demo_repo/branches/main/snowflake/;
LS @snowflake_panel_demo_repo/branches/main/dbt_customer_analytics/;
LS @snowflake_panel_demo_repo/branches/main/snowflake/procedures/;
LS @snowflake_panel_demo_repo/branches/main/snowflake/orchestration/;

-- ============================================================================
-- STEP 5: Grant Permissions
-- ============================================================================

-- Grant usage on Git repository to DATA_ENGINEER role
GRANT USAGE ON GIT REPOSITORY snowflake_panel_demo_repo TO ROLE DATA_ENGINEER;
GRANT READ ON GIT REPOSITORY snowflake_panel_demo_repo TO ROLE DATA_ENGINEER;

-- ============================================================================
-- OPTIONAL: GitHub Authentication Secret (For Private Repositories)
-- ============================================================================

-- NOTE: The repository is currently public, so no secret is needed.
-- If you make the repository private, create a GitHub Personal Access Token
-- with 'repo' scope and create a secret like this:

/*
CREATE OR REPLACE SECRET github_secret
  TYPE = password
  USERNAME = 'jpurrutia'
  PASSWORD = '<github-personal-access-token>'
  COMMENT = 'GitHub authentication credentials for private repository access';

-- Update the Git repository to use the secret
ALTER GIT REPOSITORY snowflake_panel_demo_repo SET GIT_CREDENTIALS = github_secret;

-- Fetch again with credentials
ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;
*/

-- ============================================================================
-- USEFUL MANAGEMENT COMMANDS
-- ============================================================================

-- Fetch latest code from GitHub (run anytime to sync)
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;

-- Switch to a different branch
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo SET BRANCH = 'develop';

-- Switch to a specific tag
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo SET TAG = 'v1.0.0';

-- Switch to a specific commit
-- ALTER GIT REPOSITORY snowflake_panel_demo_repo SET COMMIT = '<commit-sha>';

-- View current branch/tag/commit
-- DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- List all available branches
-- LS @snowflake_panel_demo_repo/branches/;

-- ============================================================================
-- EXECUTE SQL SCRIPTS FROM GIT REPOSITORY
-- ============================================================================

-- You can execute SQL files directly from the Git repository
-- This is useful for running scripts stored in version control

-- Example: Execute transaction generation script
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/data_generation/generate_transactions.sql;

-- Example: Execute ML model training script
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/ml/03_train_churn_model.sql;

-- Example: Execute EDA validation
-- EXECUTE IMMEDIATE FROM @snowflake_panel_demo_repo/branches/main/snowflake/eda/03_post_generation_validation.sql;

-- ============================================================================
-- OPTIONAL: Automated Fetch Task
-- ============================================================================

-- Create a task to automatically fetch from GitHub every hour
-- This keeps your Snowflake Git repository in sync with GitHub

/*
CREATE OR REPLACE TASK GOLD.auto_fetch_github
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour at minute 0
  COMMENT = 'Automatically fetch latest code from GitHub'
AS
  ALTER GIT REPOSITORY CUSTOMER_ANALYTICS.GOLD.snowflake_panel_demo_repo FETCH;

-- Start the task
ALTER TASK GOLD.auto_fetch_github RESUME;

-- Verify task is running
SHOW TASKS LIKE 'auto_fetch_github';

-- Check task execution history
SELECT
    name,
    state,
    scheduled_time,
    query_start_time,
    completed_time,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE name = 'AUTO_FETCH_GITHUB'
ORDER BY scheduled_time DESC;

-- To pause the task:
-- ALTER TASK GOLD.auto_fetch_github SUSPEND;

-- To delete the task:
-- DROP TASK IF EXISTS GOLD.auto_fetch_github;
*/

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

-- Issue: "Git repository does not exist or not authorized"
-- Solution: Verify API integration exists and Git repository was created

-- Check API integration
-- SHOW API INTEGRATIONS LIKE 'github_api_integration';
-- DESC API INTEGRATION github_api_integration;

-- Check Git repository
-- SHOW GIT REPOSITORIES LIKE 'snowflake_panel_demo_repo';
-- DESC GIT REPOSITORY snowflake_panel_demo_repo;

-- Issue: "Cannot access repository files"
-- Solution: Run FETCH to pull latest code

-- ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH;
-- LS @snowflake_panel_demo_repo/branches/main/;

-- Issue: "Authentication failed"
-- Solution: If private repo, create and set GitHub secret (see STEP 5)

-- Issue: "Files not found in expected location"
-- Solution: Verify repository URL and branch name

-- SELECT SYSTEM$GIT_REPOSITORY_URL('snowflake_panel_demo_repo');

-- ============================================================================
-- CLEANUP (IF NEEDED)
-- ============================================================================

-- To completely remove Git integration:
/*
DROP GIT REPOSITORY IF EXISTS snowflake_panel_demo_repo;
DROP SECRET IF EXISTS github_secret;
DROP API INTEGRATION IF EXISTS github_api_integration;
*/

-- ============================================================================
-- NEXT STEPS AFTER SETUP
-- ============================================================================

/*
After running this script successfully:

1. ✓ Git integration is set up and code is synced from GitHub

2. Deploy stored procedure:
   snowsql -c default -f snowflake/procedures/generate_customers.sql

3. Deploy dbt project:
   snowsql -c default -f snowflake/dbt/deploy_dbt_project.sql

4. Create task orchestration:
   snowsql -c default -f snowflake/orchestration/pipeline_tasks.sql

5. Deploy Streamlit app (if not using GitHub Actions):
   CREATE OR REPLACE STREAMLIT customer_360_app
     ROOT_LOCATION = '@snowflake_panel_demo_repo/branches/main/streamlit'
     MAIN_FILE = 'app.py'
     QUERY_WAREHOUSE = 'COMPUTE_WH';

6. Test pipeline:
   EXECUTE TASK GOLD.generate_customer_data;

For GitHub Actions deployment:
- See: .github/workflows/deploy-streamlit.yml
- Streamlit app deploys automatically on push to main

For complete documentation:
- docs/GITHUB_DEPLOYMENT_GUIDE.md
- docs/ONBOARDING_GUIDE.md
*/

-- ============================================================================
-- Display Confirmation
-- ============================================================================

SELECT '✓ Git integration setup complete!' AS status;
SELECT 'Repository: https://github.com/jpurrutia/snowflake-customer360-application-demo' AS repo;
SELECT 'Run: ALTER GIT REPOSITORY snowflake_panel_demo_repo FETCH; to sync latest code' AS sync_command;
