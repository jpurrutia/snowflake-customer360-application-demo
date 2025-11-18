import streamlit as st
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os


# Page configuration
st.set_page_config(
    page_title="SpendSight | Powered by Snowflake",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for accessible Snowflake theme (following Snowflake accessibility guidelines)
st.markdown("""
<style>
    /* Snowflake Brand Colors - Accessible Palette */
    :root {
        --snowflake-blue: #29B5E8;        /* Use with BLACK text (28pt+ only) */
        --navy-blue: #0E3E66;             /* Use with WHITE text */
        --midnight: #000000;               /* Use with WHITE text */
        --dark-bg: #0a1628;               /* Dark blue background */
        --card-bg: #12263f;               /* Card background */
        --medium-gray: #94a3b8;           /* Light gray for text */
        --light-gray: #1e3a5f;            /* Slightly lighter than card */
        --white: #ffffff;
        --accent-orange: #FF9F36;         /* Use with BLACK text */
    }

    /* Main app background - dark theme */
    .stApp {
        background-color: var(--dark-bg);
        color: var(--white);
    }

    /* Main content background */
    .main .block-container {
        background-color: var(--card-bg);
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        margin-top: 1rem;
    }

    /* Headers - White text for dark theme */
    h1, h2, h3 {
        color: var(--white) !important;
        font-weight: 600;
    }

    /* Metric cards - dark theme */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 600;
        color: var(--snowflake-blue);
    }

    [data-testid="stMetricLabel"] {
        font-weight: 500;
        color: var(--medium-gray);
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }

    [data-testid="stMetricDelta"] {
        font-size: 0.875rem;
        color: var(--medium-gray);
    }

    /* Sidebar - dark theme */
    [data-testid="stSidebar"] {
        background-color: var(--card-bg);
        border-right: 1px solid var(--light-gray);
    }

    [data-testid="stSidebar"] h3 {
        color: var(--white) !important;
        font-size: 1rem;
    }

    [data-testid="stSidebar"] .stMarkdown {
        color: var(--medium-gray);
    }

    /* Buttons - Snowflake blue for dark theme */
    .stButton > button {
        background-color: var(--light-gray);
        color: var(--white);
        border: 1px solid var(--snowflake-blue);
        border-radius: 6px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        font-size: 1rem;
        transition: all 0.2s ease;
    }

    .stButton > button:hover {
        background-color: var(--snowflake-blue);
        color: var(--dark-bg);
        box-shadow: 0 2px 8px rgba(41, 181, 232, 0.3);
    }

    .stButton > button[kind="primary"] {
        background-color: var(--snowflake-blue);
        color: var(--dark-bg);
        font-weight: 600;
        border: none;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: #1A9FCC;
    }

    /* Radio buttons - dark theme */
    [data-testid="stRadio"] label {
        font-size: 1rem;
        color: var(--white);
    }

    /* Text inputs - dark theme */
    input, textarea, select {
        background-color: var(--light-gray) !important;
        border: 2px solid var(--light-gray) !important;
        border-radius: 4px;
        font-size: 1rem;
        color: var(--white) !important;
    }

    input:focus, textarea:focus, select:focus {
        border-color: var(--snowflake-blue) !important;
        box-shadow: 0 0 0 2px rgba(41, 181, 232, 0.2);
    }

    /* Info/Warning/Success boxes - accessible colors */
    .stAlert {
        border-radius: 6px;
        border-left: 4px solid;
        font-size: 1rem;
    }

    [data-testid="stAlert"][data-baseweb="notification"] {
        background-color: rgba(41, 181, 232, 0.1);
    }

    /* Tables - dark theme */
    [data-testid="stDataFrame"] {
        font-size: 0.95rem;
        background-color: var(--card-bg) !important;
    }

    [data-testid="stDataFrame"] th {
        background-color: var(--light-gray) !important;
        color: var(--white) !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }

    [data-testid="stDataFrame"] td {
        color: var(--medium-gray) !important;
        background-color: var(--card-bg) !important;
    }

    /* Professional font */
    html, body, [class*="css"] {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica', 'Arial', sans-serif;
        color: var(--white);
    }

    /* Plotly charts - dark theme */
    .js-plotly-plot {
        background-color: transparent !important;
    }

    .js-plotly-plot .plotly .modebar {
        background-color: transparent !important;
    }

    /* Selectbox and multiselect - dark theme */
    [data-baseweb="select"] {
        background-color: var(--light-gray) !important;
    }

    [data-baseweb="select"] > div {
        background-color: var(--light-gray) !important;
        color: var(--white) !important;
    }

    /* Expander - dark theme */
    [data-testid="stExpander"] {
        background-color: var(--light-gray);
        border: 1px solid var(--light-gray);
    }

    [data-testid="stExpander"] summary {
        color: var(--white);
    }
</style>
""", unsafe_allow_html=True)

# ============= CONNECTION MANAGEMENT =============


@st.cache_resource
def get_snowflake_connection():
    """Create cached Snowflake connection using Streamlit's built-in method"""
    try:
        # For Streamlit in Snowflake, use st.connection() which handles auth automatically
        # This provides access to the session token needed for Cortex Analyst
        conn = st.connection("snowflake")
        # Return the raw connection object for compatibility
        return conn.raw_connection
    except Exception as e:
        # Fallback to environment variable-based connection for local development
        try:
            conn = snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                warehouse="COMPUTE_WH",
                database="CUSTOMER_ANALYTICS",
                schema="GOLD",
                role="DATA_ANALYST",
                client_session_keep_alive=True,
            )
            return conn
        except Exception as e2:
            st.error(f"Failed to connect to Snowflake: {e2}")
            st.stop()


def execute_query(query, params=None):
    """Execute Snowflake query with error handling"""
    conn = get_snowflake_connection()

    try:
        cursor = conn.cursor()

        # Note: Removed ALTER SESSION as it's not supported in Streamlit in Snowflake
        # Query timeout is managed by Snowflake's default settings

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Fetch results with size limit
        results = cursor.fetchmany(10000)
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)

        cursor.close()
        return df

    except ProgrammingError as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()
    except DatabaseError as e:
        if "timeout" in str(e).lower():
            st.warning("Query timed out. Try filtering to a smaller dataset.")
        else:
            st.error(f"Database error: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        return pd.DataFrame()


# ============= HEADER =============

st.markdown("""
<div style="padding: 1rem 0;">
    <h1 style="margin: 0; font-size: 2.5rem;">🔍 SpendSight</h1>
    <p style="color: var(--medium-gray); margin: 0.5rem 0 0 0; font-size: 1.1rem;">Powered by Snowflake Cortex AI · Post-Acquisition Credit Card Customer Intelligence</p>
</div>
""", unsafe_allow_html=True)

# ============= SIDEBAR NAVIGATION =============

with st.sidebar:
    st.markdown("### 🔍 SpendSight")
    st.markdown("##### Customer Analytics Platform")
    st.markdown("---")

    st.markdown("### 📊 Navigation")
    page = st.radio(
        "Select View",
        [
            "🤖 AI Assistant",
            "🔍 Customer Deep Dive",
            "👥 Segment Explorer",
            "📢 Campaign Performance"
        ],
        index=0,
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("### ℹ️ System Info")
    st.caption(f"**Database:** CUSTOMER_ANALYTICS")
    st.caption(f"**Schema:** GOLD")
    st.caption(f"**Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    st.caption("**Powered by:** Snowflake Cortex AI")

# ============= MAIN CONTENT =============

if page == "🤖 AI Assistant":
    from tabs import ai_assistant

    ai_assistant.render(execute_query, get_snowflake_connection())

elif page == "🔍 Customer Deep Dive":
    from tabs import customer_360

    customer_360.render(execute_query, get_snowflake_connection())

elif page == "👥 Segment Explorer":
    from tabs import segment_explorer

    segment_explorer.render(execute_query)

elif page == "📢 Campaign Performance":
    from tabs import campaign_simulator

    campaign_simulator.render(execute_query, get_snowflake_connection())
