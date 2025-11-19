# Enable Choropleth Maps in SpendSight

This guide explains how to enable interactive PyDeck choropleth maps for geographic data visualization in SpendSight.

https://discuss.streamlit.io/t/plotly-choropleth-not-displaying-in-streamlit/117991/2

## Current Status

🟡 **Choropleth maps are implemented but disabled**
- Bar charts currently handle all geographic visualizations
- Maps require External Access Integration (not available on trial accounts)
- All code is ready - just needs to be uncommented and configured

---

## Prerequisites

⚠️ **Required:**
- **Paid Snowflake account** (NOT trial account)
- **ACCOUNTADMIN** or **SECURITYADMIN** role access
- Streamlit app deployed in Snowflake

❌ **Will NOT work on:**
- Snowflake trial accounts
- Accounts without external network access permissions

---

## Step 1: Create External Access Integration

**Run these SQL commands as ACCOUNTADMIN:**

```sql
-- Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Use your warehouse
USE WAREHOUSE COMPUTE_WH;

-- Use your database
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA GOLD;

-- Step 1: Create Network Rule for external hosts
CREATE OR REPLACE NETWORK RULE geojson_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'eric.clst.org',           -- US states GeoJSON source
    'api.mapbox.com',          -- Mapbox API
    'a.tiles.mapbox.com',      -- Mapbox tile server A
    'b.tiles.mapbox.com'       -- Mapbox tile server B
  );

-- Step 2: Create External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION geojson_integration
  ALLOWED_NETWORK_RULES = (geojson_network_rule)
  ENABLED = TRUE;

-- Step 3: Grant usage to DATA_ANALYST role (or your app role)
GRANT USAGE ON INTEGRATION geojson_integration TO ROLE DATA_ANALYST;

-- Verify creation
SHOW INTEGRATIONS LIKE 'geojson_integration';
```

**Expected output:**
```
+---------------------+---------------------+---------+
| name                | type                | enabled |
+---------------------+---------------------+---------+
| geojson_integration | EXTERNAL_ACCESS     | true    |
+---------------------+---------------------+---------+
```

---

## Step 2: Configure Streamlit App

1. **Go to your Streamlit app in Snowflake**
2. **Open App Settings** (three dots menu → Settings)
3. **Click "External networks" tab**
4. **Select the integration:**
   - Choose `geojson_integration` from dropdown
5. **Save and redeploy**

---

## Step 3: Code is Already Ready!

✅ **No code changes needed!** The choropleth code is already implemented and will automatically enable itself when PyDeck becomes available.

**How it works:**
- Code tries to import PyDeck at startup
- If import fails (trial account), `PYDECK_AVAILABLE = False`
- Choropleth option is hidden from users
- If import succeeds (paid account with pydeck installed), maps automatically appear!

**What's already in the code:**
```python
# Automatic detection
try:
    import pydeck as pdk
    import requests
    PYDECK_AVAILABLE = True
except ImportError:
    PYDECK_AVAILABLE = False

# Conditional feature enablement
if PYDECK_AVAILABLE and any('state' in col.lower() for col in geo_cols):
    suggestions.append('choropleth_usa')
```

This means maps will "just work" once you complete Steps 1-2!

---

## Step 4: Add PyDeck Package (if not already added)

If your `requirements.txt` doesn't have PyDeck:

**In `streamlit/requirements.txt`, add:**
```
pydeck==0.8.0
```

**Current requirements.txt should include:**
```
streamlit==1.30.0
snowflake-connector-python[pandas]==3.5.0
pandas==2.1.4
plotly==5.18.0
pydeck==0.8.0
python-dotenv==1.0.0
```

---

## Step 4: Test Choropleth Maps

1. **Redeploy the Streamlit app** in Snowflake
2. **Navigate to AI Assistant tab**
3. **Ask a geographic question:**
   - "What is the average lifetime value by state?"
   - "Which states have the highest churn risk?"
4. **Switch to Chart view**
5. **Select "🗺️ US Map (Choropleth)"** from dropdown
6. **Verify the map renders** with colored states

**Expected result:**
- Interactive map of USA
- States colored by metric value (green → yellow → red gradient)
- Hover tooltips showing state name and formatted values
- Dark theme matching app style

---

## Troubleshooting

### Error: "Failed to load US states map data"

**Cause:** External Access Integration not configured properly

**Solutions:**
1. Verify integration exists: `SHOW INTEGRATIONS LIKE 'geojson_integration';`
2. Check it's enabled in Streamlit app settings (External networks tab)
3. Verify network rule includes `eric.clst.org`
4. Try redeploying the app

### Error: "ModuleNotFoundError: No module named 'pydeck'"

**Cause:** PyDeck not in requirements.txt or not installed

**Solutions:**
1. Check `streamlit/requirements.txt` includes `pydeck==0.8.0`
2. Redeploy the app to install packages
3. If using Snowflake Packages UI, search for "pydeck" and add it

### Map shows legend but no geography

**Cause:** Mapbox tile servers blocked or CSP issue

**Solutions:**
1. Verify network rule includes all Mapbox hosts:
   - `api.mapbox.com`
   - `a.tiles.mapbox.com`
   - `b.tiles.mapbox.com`
2. Check External Access Integration is granted to correct role
3. Ensure app is using the integration (External networks tab in settings)

### Error: "External access is not supported for trial accounts"

**Cause:** Running on Snowflake trial account

**Solution:**
- Upgrade to paid Snowflake account
- OR keep using bar charts (they work great for geographic data!)

---

## Rollback to Bar Charts Only

If you encounter issues and want to disable maps:

**Option 1: Remove PyDeck package** (easiest)
- Remove `pydeck==0.8.0` from `requirements.txt`
- Redeploy
- Choropleth will automatically disappear from options

**Option 2: Keep PyDeck but disable map option**
- In `ai_assistant.py` line 97, change:
  ```python
  if PYDECK_AVAILABLE and any('state' in col.lower() for col in geo_cols):
  ```
  to:
  ```python
  if False and PYDECK_AVAILABLE and any('state' in col.lower() for col in geo_cols):
  ```

Bar charts will continue to work perfectly for all geographic queries.

---

## Architecture Notes

**How it works:**
1. User asks geographic question (e.g., "lifetime value by state")
2. Cortex Analyst generates SQL and returns state-level data
3. `suggest_chart_type()` detects state column → suggests choropleth
4. `fetch_us_states_geojson()` downloads GeoJSON from eric.clst.org (cached 1 hour)
5. DataFrame values merged into GeoJSON properties
6. PyDeck `GeoJsonLayer` renders map with Mapbox tiles
7. Interactive tooltips show state name + formatted metric

**Data flow:**
```
Cortex Analyst
    ↓
SQL Query → Snowflake → DataFrame (state codes + values)
    ↓
fetch_us_states_geojson() → eric.clst.org → GeoJSON (cached)
    ↓
Merge data into GeoJSON properties
    ↓
PyDeck GeoJsonLayer → Mapbox tiles → st.pydeck_chart()
    ↓
Interactive map displayed
```

**External dependencies:**
- `eric.clst.org` - US states GeoJSON (public domain, 2010 Census data)
- `api.mapbox.com` - Mapbox API for map tiles
- `a.tiles.mapbox.com`, `b.tiles.mapbox.com` - Tile servers

**Caching:**
- GeoJSON cached for 1 hour (`@st.cache_data(ttl=3600)`)
- Reduces external requests and improves performance

---

## Support

**Issues?**
1. Check troubleshooting section above
2. Verify all prerequisites met (paid account, ACCOUNTADMIN access)
3. Review Snowflake documentation: https://docs.snowflake.com/en/developer-guide/streamlit/limitations

**Questions?**
- Review code in `streamlit/tabs/ai_assistant.py` lines 157-270
- Check function `fetch_us_states_geojson()` at line 121
- Examine `render_chart()` choropleth block at line 157

---

## Summary Checklist

When you upgrade from trial to paid account, follow these steps:

✅ **Step 1:** Run SQL commands as ACCOUNTADMIN (create External Access Integration)
✅ **Step 2:** Configure External Access Integration in Streamlit app settings
✅ **Step 3:** Ensure `pydeck==0.8.0` in `requirements.txt` (already there!)
✅ **Step 4:** Redeploy app
✅ **Step 5:** Test with geographic query ("What is the average lifetime value by state?")
✅ **Step 6:** Verify "🗺️ US Map (Choropleth)" appears in Chart Type dropdown
✅ **Step 7:** Select it and verify map renders with colored states

**That's it!** No code changes needed - choropleth maps will automatically enable. 🎉
