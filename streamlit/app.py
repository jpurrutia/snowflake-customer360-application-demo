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
    page_title="Customer 360 Analytics | Powered by Snowflake",
    page_icon="❄️",
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
        --medium-gray: #5B5B5B;           /* Use with WHITE text */
        --light-gray: #f8f9fa;            /* Use with BLACK text */
        --white: #ffffff;
        --accent-orange: #FF9F36;         /* Use with BLACK text */
    }

    /* Main app background - soft, easy on eyes */
    .stApp {
        background-color: var(--light-gray);
    }

    /* Main content background */
    .main .block-container {
        background-color: var(--white);
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
        margin-top: 1rem;
    }

    /* Headers - Navy blue with white text for accessibility */
    h1, h2, h3 {
        color: var(--navy-blue) !important;
        font-weight: 600;
    }

    /* Metric cards - accessible contrast */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 600;
        color: var(--navy-blue);
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
    }

    /* Sidebar - clean and professional */
    [data-testid="stSidebar"] {
        background-color: var(--white);
        border-right: 1px solid #e0e0e0;
    }

    [data-testid="stSidebar"] h3 {
        color: var(--navy-blue) !important;
        font-size: 1rem;
    }

    /* Buttons - Navy blue for better contrast */
    .stButton > button {
        background-color: var(--navy-blue);
        color: var(--white);
        border: none;
        border-radius: 6px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        font-size: 1rem;
        transition: all 0.2s ease;
    }

    .stButton > button:hover {
        background-color: var(--midnight);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
    }

    .stButton > button[kind="primary"] {
        background-color: var(--snowflake-blue);
        color: var(--midnight);
        font-weight: 600;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: #1A9FCC;
    }

    /* Radio buttons - better visibility */
    [data-testid="stRadio"] label {
        font-size: 1rem;
        color: var(--midnight);
    }

    /* Text inputs - clear borders */
    input, textarea, select {
        border: 2px solid #e0e0e0 !important;
        border-radius: 4px;
        font-size: 1rem;
    }

    input:focus, textarea:focus, select:focus {
        border-color: var(--navy-blue) !important;
        box-shadow: 0 0 0 2px rgba(14, 62, 102, 0.1);
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

    /* Tables - better readability */
    [data-testid="stDataFrame"] {
        font-size: 0.95rem;
    }

    [data-testid="stDataFrame"] th {
        background-color: var(--light-gray) !important;
        color: var(--navy-blue) !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }

    /* Professional font */
    html, body, [class*="css"] {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Helvetica', 'Arial', sans-serif;
        color: var(--midnight);
    }

    /* Plotly charts - remove harsh white backgrounds */
    .js-plotly-plot {
        background-color: transparent !important;
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
<div class="main-header">
    <h1>❄️ Customer 360 Analytics</h1>
    <p>Powered by Snowflake Cortex AI · Post-Acquisition Credit Card Customer Intelligence</p>
</div>
""", unsafe_allow_html=True)

# ============= SIDEBAR NAVIGATION =============

with st.sidebar:
    st.markdown("### ❄️ Customer 360")
    st.markdown("##### Analytics Platform")
    st.markdown("---")

    st.markdown("### 📊 Navigation")
    page = st.radio(
        "Select View",
        [
            "📈 Executive Dashboard",
            "👥 Segment Explorer",
            "🔍 Customer Deep Dive",
            "🤖 AI Assistant",
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

if page == "📈 Executive Dashboard":
    from tabs import executive_dashboard

    executive_dashboard.render(execute_query)

elif page == "👥 Segment Explorer":
    from tabs import segment_explorer

    segment_explorer.render(execute_query)

elif page == "🔍 Customer Deep Dive":
    from tabs import customer_360

    customer_360.render(execute_query, get_snowflake_connection())

elif page == "🤖 AI Assistant":
    from tabs import ai_assistant

    ai_assistant.render(execute_query, get_snowflake_connection())

elif page == "📢 Campaign Performance":
    from tabs import campaign_simulator

    campaign_simulator.render(execute_query, get_snowflake_connection())
