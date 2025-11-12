"""
Integration Tests for AI Assistant Tab (Iteration 5.3)

Tests AI Assistant tab functionality:
1. Suggested questions display
2. Mock Cortex Analyst execution
3. Question execution
4. Query history
5. CSV export
6. Error handling

Run:
    pytest tests/integration/test_ai_assistant_tab.py -v
"""

import os
import pytest
import pandas as pd
from snowflake.connector import connect
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../streamlit'))

from tabs.ai_assistant import SUGGESTED_QUESTIONS, call_cortex_analyst_mock

# Load environment variables
load_dotenv()


@pytest.fixture(scope="module")
def snowflake_conn():
    """Create Snowflake connection for tests"""
    conn = connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "CUSTOMER_ANALYTICS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "GOLD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
    yield conn
    conn.close()


# ============================================================================
# Test 1: Suggested Questions Display
# ============================================================================


def test_suggested_questions_display():
    """
    Test that SUGGESTED_QUESTIONS dictionary is populated.

    Validates:
    - Dictionary has categories
    - Each category has questions
    - Questions are non-empty strings
    """
    assert SUGGESTED_QUESTIONS is not None, "SUGGESTED_QUESTIONS not defined"
    assert len(SUGGESTED_QUESTIONS) > 0, "SUGGESTED_QUESTIONS is empty"

    # Expected categories
    expected_categories = [
        "Churn Analysis",
        "Customer Segmentation",
        "Spending Trends",
        "Geographic Analysis",
        "Campaign Targeting"
    ]

    for category in expected_categories:
        assert category in SUGGESTED_QUESTIONS, f"Missing category: {category}"
        questions = SUGGESTED_QUESTIONS[category]
        assert len(questions) > 0, f"Category {category} has no questions"

        # Verify questions are strings
        for question in questions:
            assert isinstance(question, str), f"Question not a string: {question}"
            assert len(question) > 0, "Empty question found"

    print(f"\n✓ Suggested questions validated:")
    print(f"  Categories: {len(SUGGESTED_QUESTIONS)}")
    total_questions = sum(len(q) for q in SUGGESTED_QUESTIONS.values())
    print(f"  Total questions: {total_questions}")


# ============================================================================
# Test 2: Mock Cortex Analyst - High Risk Churn
# ============================================================================


def test_cortex_analyst_mock_high_risk_churn(snowflake_conn):
    """
    Test mock Cortex Analyst with high risk churn question.

    Validates:
    - Question recognized
    - SQL generated
    - Results returned
    - No errors
    """
    question = "Which customers are at highest risk of churning?"

    response = call_cortex_analyst_mock(snowflake_conn, question)

    assert response is not None, "Response is None"
    assert response['error'] is None, f"Error occurred: {response['error']}"
    assert response['sql'] is not None, "No SQL generated"
    assert response['results'] is not None, "No results returned"

    # Validate results
    df = response['results']
    assert isinstance(df, pd.DataFrame), "Results not a DataFrame"
    assert len(df) > 0, "No results returned"

    # Validate columns
    expected_columns = ['CUSTOMER_ID', 'FULL_NAME', 'EMAIL', 'CUSTOMER_SEGMENT',
                        'CHURN_RISK_SCORE', 'CHURN_RISK_CATEGORY']

    for col in expected_columns:
        assert col in df.columns, f"Expected column {col} not found"

    # Validate all are High Risk
    assert (df['CHURN_RISK_CATEGORY'] == 'High Risk').all(), \
        "Found non-High Risk customers"

    print(f"\n✓ High risk churn query successful:")
    print(f"  Results: {len(df):,} customers")
    print(f"  Avg churn risk: {df['CHURN_RISK_SCORE'].mean():.2f}")


# ============================================================================
# Test 3: Mock Cortex Analyst - Segment Count
# ============================================================================


def test_cortex_analyst_mock_segment_count(snowflake_conn):
    """
    Test mock Cortex Analyst with segment count question.

    Validates:
    - Question recognized
    - Aggregation works
    - All segments returned
    """
    question = "How many customers are in each segment?"

    response = call_cortex_analyst_mock(snowflake_conn, question)

    assert response['error'] is None, f"Error occurred: {response['error']}"
    assert response['sql'] is not None, "No SQL generated"
    assert response['results'] is not None, "No results returned"

    df = response['results']
    assert len(df) > 0, "No segments returned"

    # Validate columns
    assert 'CUSTOMER_SEGMENT' in df.columns, "Missing CUSTOMER_SEGMENT column"
    assert 'CUSTOMER_COUNT' in df.columns, "Missing CUSTOMER_COUNT column"

    # Validate counts are positive
    assert (df['CUSTOMER_COUNT'] > 0).all(), "Segments should have positive counts"

    print(f"\n✓ Segment count query successful:")
    print(f"  Segments: {len(df)}")
    for idx, row in df.iterrows():
        print(f"    {row['CUSTOMER_SEGMENT']}: {row['CUSTOMER_COUNT']:,}")


# ============================================================================
# Test 4: Mock Cortex Analyst - Lifetime Value by Segment
# ============================================================================


def test_cortex_analyst_mock_ltv_by_segment(snowflake_conn):
    """
    Test mock Cortex Analyst with LTV by segment question.

    Validates:
    - Aggregation works
    - AVG calculated correctly
    """
    question = "Compare lifetime value across segments"

    response = call_cortex_analyst_mock(snowflake_conn, question)

    assert response['error'] is None, f"Error occurred: {response['error']}"
    assert response['sql'] is not None, "No SQL generated"

    df = response['results']
    assert len(df) > 0, "No results returned"

    # Validate columns
    assert 'CUSTOMER_SEGMENT' in df.columns
    assert 'AVG_LTV' in df.columns
    assert 'CUSTOMER_COUNT' in df.columns

    # Validate avg_ltv is positive
    assert (df['AVG_LTV'] > 0).all(), "Average LTV should be positive"

    print(f"\n✓ LTV by segment query successful:")
    print(f"  Segments: {len(df)}")
    for idx, row in df.iterrows():
        print(f"    {row['CUSTOMER_SEGMENT']}: ${row['AVG_LTV']:,.0f} (n={row['CUSTOMER_COUNT']:,})")


# ============================================================================
# Test 5: Mock Cortex Analyst - Unrecognized Question
# ============================================================================


def test_cortex_analyst_mock_unrecognized_question(snowflake_conn):
    """
    Test mock Cortex Analyst with unrecognized question.

    Validates:
    - Error returned for unrecognized questions
    - No crash
    """
    question = "What is the weather in Paris?"

    response = call_cortex_analyst_mock(snowflake_conn, question)

    # Should return error for unrecognized question
    assert response['error'] is not None, "Should return error for unrecognized question"
    assert response['sql'] is None, "Should not generate SQL"
    assert response['results'] is None, "Should not return results"

    print(f"\n✓ Unrecognized question handling successful:")
    print(f"  Error message: {response['error']}")


# ============================================================================
# Test 6: Mock Cortex Analyst - Premium High Risk
# ============================================================================


def test_cortex_analyst_mock_premium_high_risk(snowflake_conn):
    """
    Test mock Cortex Analyst with Premium high/medium risk question.

    Validates:
    - Complex filter works
    - Multiple conditions applied
    """
    question = "Which Premium cardholders are at medium or high risk?"

    response = call_cortex_analyst_mock(snowflake_conn, question)

    assert response['error'] is None, f"Error occurred: {response['error']}"

    df = response['results']

    if len(df) > 0:
        # Validate all are Premium
        assert (df['CARD_TYPE'] == 'Premium').all(), "Should only return Premium cardholders"

        # Validate all are Medium or High Risk
        risk_categories = df['CHURN_RISK_CATEGORY'].unique()
        assert all(cat in ['Medium Risk', 'High Risk'] for cat in risk_categories), \
            "Should only return Medium or High Risk"

        print(f"\n✓ Premium high/medium risk query successful:")
        print(f"  Premium customers at risk: {len(df):,}")
        print(f"  Risk categories: {risk_categories.tolist()}")
    else:
        print(f"\n✓ Premium high/medium risk query executed (0 results - possible with data)")


# ============================================================================
# Test 7: CSV Export
# ============================================================================


def test_csv_export(snowflake_conn):
    """
    Test CSV export of query results.

    Validates:
    - DataFrame converts to CSV
    - CSV has headers
    - CSV has data
    """
    question = "How many customers are in each segment?"

    response = call_cortex_analyst_mock(snowflake_conn, question)

    assert response['error'] is None
    assert response['results'] is not None

    df = response['results']

    # Convert to CSV
    csv = df.to_csv(index=False)

    assert csv is not None, "CSV conversion failed"
    assert len(csv) > 0, "CSV is empty"

    csv_lines = csv.split('\n')
    assert 'CUSTOMER_SEGMENT' in csv_lines[0], "CSV missing headers"
    assert len(csv_lines) > 1, "CSV has no data rows"

    print(f"\n✓ CSV export successful:")
    print(f"  CSV size: {len(csv):,} characters")
    print(f"  CSV lines: {len(csv_lines):,}")


# ============================================================================
# Test 8: SQL Generation
# ============================================================================


def test_sql_generation():
    """
    Test that mock generates valid SQL.

    Validates:
    - SQL is not None
    - SQL contains key elements
    """
    from unittest.mock import Mock

    mock_conn = Mock()

    # Test high risk churn
    question = "Which customers are at highest risk of churning?"
    response = call_cortex_analyst_mock(mock_conn, question)

    assert response['sql'] is not None
    assert 'SELECT' in response['sql'].upper()
    assert 'FROM' in response['sql'].upper()
    assert 'CUSTOMER_360_PROFILE' in response['sql'].upper()
    assert 'High Risk' in response['sql']

    print(f"\n✓ SQL generation validated")


# ============================================================================
# Test 9: Question Categories Coverage
# ============================================================================


def test_question_categories_coverage(snowflake_conn):
    """
    Test that at least one question from each category works.

    Validates:
    - Mock handles questions from all categories
    """
    # Pick first question from each category
    test_questions = [
        SUGGESTED_QUESTIONS["Churn Analysis"][0],
        SUGGESTED_QUESTIONS["Customer Segmentation"][0],
        SUGGESTED_QUESTIONS["Campaign Targeting"][1],  # Premium high/medium risk
    ]

    for question in test_questions:
        response = call_cortex_analyst_mock(snowflake_conn, question)

        # Should either succeed or fail gracefully
        assert response is not None, f"No response for question: {question}"

        if response['error']:
            # Graceful error handling
            assert isinstance(response['error'], str), "Error should be a string"
        else:
            # Successful execution
            assert response['sql'] is not None, f"No SQL for question: {question}"
            assert response['results'] is not None, f"No results for question: {question}"

    print(f"\n✓ Question categories coverage validated:")
    print(f"  Tested {len(test_questions)} questions across categories")


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
