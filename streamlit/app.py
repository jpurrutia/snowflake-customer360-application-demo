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
    page_title="Customer 360 Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============= CONNECTION MANAGEMENT =============

@st.cache_resource
def get_snowflake_connection():
    """Create cached Snowflake connection"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse='COMPUTE_WH',
            database='CUSTOMER_ANALYTICS',
            schema='GOLD',
            role='DATA_ANALYST',
            client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        st.stop()

def execute_query(query, params=None):
    """Execute Snowflake query with error handling"""
    conn = get_snowflake_connection()

    try:
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60")

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

st.title("📊 Customer 360 Analytics Platform")
st.markdown("**Post-Acquisition Credit Card Customer Intelligence**")
st.markdown("---")

# ============= SIDEBAR NAVIGATION =============

with st.sidebar:
    st.header("Navigation")
    page = st.radio(
        "Select View",
        ["Segment Explorer", "Customer 360", "AI Assistant", "Campaign Performance"],
        index=0
    )

    st.markdown("---")
    st.markdown("### Platform Info")
    st.info(f"**Database:** CUSTOMER_ANALYTICS")
    st.info(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ============= MAIN CONTENT =============

if page == "Segment Explorer":
    from tabs import segment_explorer
    segment_explorer.render(execute_query)

elif page == "Customer 360":
    from tabs import customer_360
    customer_360.render(execute_query, get_snowflake_connection())

elif page == "AI Assistant":
    from tabs import ai_assistant
    ai_assistant.render(execute_query, get_snowflake_connection())

elif page == "Campaign Performance":
    from tabs import campaign_simulator
    campaign_simulator.render(execute_query, get_snowflake_connection())
