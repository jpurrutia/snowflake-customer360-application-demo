# dbt Cloud Setup Guide - OPTIONAL REFERENCE

**Version**: 1.0
**Date**: 2025-11-12
**Status**: ⚠️ **OPTIONAL** - This project uses local dbt runs

---

## ⚠️ Important Notice

**This guide is for REFERENCE ONLY.**

The Snowflake Customer 360 Analytics Platform is designed to run **dbt locally** without requiring dbt Cloud. This guide is provided as optional documentation if you want to explore dbt Cloud's features in the future.

### What You're Currently Using

✅ **Local dbt execution**:
```bash
cd dbt_customer_analytics
dbt run
dbt test
```

✅ **GitHub for version control**
✅ **Snowflake for data warehouse**
✅ **Manual or scheduled dbt runs** (via cron, Airflow, etc.)

### When to Consider dbt Cloud

Consider dbt Cloud if you need:
- Automated scheduling without setting up cron/Airflow
- Built-in CI/CD for pull requests
- Hosted documentation website
- Multi-user collaboration in a web IDE
- Job monitoring and alerting dashboard

**Cost**: Free tier (1 developer), $100+/mo for teams

---

## Quick Comparison

| Feature | Local dbt (Current) | dbt Cloud (Optional) |
|---------|---------------------|----------------------|
| Run dbt transformations | ✅ `dbt run` | ✅ Scheduled jobs |
| Version control | ✅ Git/GitHub | ✅ Git/GitHub |
| Testing | ✅ `dbt test` | ✅ Automated tests |
| Documentation | ✅ `dbt docs generate` | ✅ Hosted website |
| Scheduling | ⚠️ Manual or cron | ✅ Built-in scheduler |
| CI on PRs | ⚠️ GitHub Actions | ✅ Built-in |
| Cost | ✅ Free | ⚠️ Free (1 dev), $100+/mo |
| Setup complexity | ✅ Simple | ⚠️ Moderate |

---

## If You Decide to Use dbt Cloud

### Quick Setup via Snowflake Partner Connect (5 minutes)

1. **In Snowflake**: Admin → Partner Connect → dbt Cloud
2. **Click "Connect"**
3. **Sign up for dbt Cloud** (redirected automatically)
4. **Connect GitHub repository**
5. **Update connection to use `CUSTOMER_ANALYTICS` database** (instead of auto-created `PC_DBT_DB`)
6. **Create production job**: Daily at 6am, runs `dbt run && dbt test`

### Documentation

For complete dbt Cloud setup instructions, see official docs:
- **dbt Cloud Overview**: https://docs.getdbt.com/docs/dbt-cloud/cloud-overview
- **Snowflake + dbt Cloud**: https://docs.getdbt.com/docs/cloud/connect-data-platform/connect-snowflake
- **Partner Connect**: https://docs.snowflake.com/en/user-guide/ecosystem-partner-connect

---

## Recommended Approach for This Project

**Stick with local dbt** and use one of these scheduling options:

### Option 1: Manual Runs (Simple)
```bash
# Run when needed
cd dbt_customer_analytics
dbt run
dbt test
```

### Option 2: Cron Job (Automated)
```bash
# Add to crontab
crontab -e

# Add line to run dbt daily at 6am
0 6 * * * cd /Users/jpurrutia/projects/snowflake-panel-demo/dbt_customer_analytics && dbt run && dbt test
```

### Option 3: GitHub Actions (CI/CD)
Create `.github/workflows/run-dbt.yml`:
```yaml
name: Run dbt

on:
  push:
    branches: [main]
    paths: ['dbt_customer_analytics/**']

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - run: pip install dbt-snowflake
      - name: Run dbt
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          cd dbt_customer_analytics
          dbt run
          dbt test
```

---

## Summary

**You do NOT need dbt Cloud for this project.** Local dbt execution is simpler and free.

This guide is here for reference if you want to explore dbt Cloud features in the future.

---

**End of Optional dbt Cloud Reference**
