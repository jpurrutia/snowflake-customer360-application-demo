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

# Custom CSS for Snowflake-inspired professional theme
st.markdown("""
<style>
    /* Snowflake Brand Colors */
    :root {
        --snowflake-blue: #29B5E8;
        --snowflake-dark-blue: #1A73E8;
        --snowflake-navy: #0E3E66;
        --midnight: #000000;
        --medium-gray: #5B5B5B;
        --accent-orange: #FF9F36;
        --accent-purple: #7254A3;
    }

    /* Main header styling */
    .main-header {
        background: linear-gradient(135deg, var(--snowflake-blue) 0%, var(--snowflake-dark-blue) 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }

    .main-header h1 {
        color: white !important;
        font-weight: 600;
        margin: 0;
        font-size: 2.5rem;
    }

    .main-header p {
        color: rgba(255, 255, 255, 0.9);
        font-size: 1.1rem;
        margin: 0.5rem 0 0 0;
    }

    /* Metric cards */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 600;
        color: var(--snowflake-dark-blue);
    }

    [data-testid="stMetricLabel"] {
        font-weight: 500;
        color: var(--medium-gray);
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }

    /* Button styling */
    .stButton > button {
        background-color: var(--snowflake-blue);
        color: white;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 2rem;
        font-weight: 500;
        transition: all 0.3s ease;
    }

    .stButton > button:hover {
        background-color: var(--snowflake-dark-blue);
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.3);
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 6px 6px 0 0;
        padding: 10px 20px;
        font-weight: 500;
    }

    .stTabs [aria-selected="true"] {
        background-color: var(--snowflake-blue);
        color: white;
    }

    /* Card-like containers */
    div[data-testid="stExpander"] {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }

    /* Info boxes */
    .stAlert {
        border-radius: 8px;
        border-left: 4px solid var(--snowflake-blue);
    }

    /* Professional font */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
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
