"""
Pytest configuration and shared fixtures for integration tests.
"""

import os
import pytest
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


@pytest.fixture(scope="session")
def snowflake_connection():
    """
    Create a Snowflake connection for integration tests.

    Uses credentials from environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_ROLE
    - SNOWFLAKE_WAREHOUSE
    - SNOWFLAKE_DATABASE
    - SNOWFLAKE_SCHEMA
    """
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'CUSTOMER_ANALYTICS'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'GOLD'),
    )

    yield conn

    conn.close()


@pytest.fixture(scope="session")
def dbt_project_dir():
    """
    Return the path to the dbt project directory.
    """
    return Path(__file__).parent.parent / "dbt_customer_analytics"


@pytest.fixture(scope="session")
def streamlit_dir():
    """
    Return the path to the streamlit directory.
    """
    return Path(__file__).parent.parent / "streamlit"
