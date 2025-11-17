import streamlit as st
import pandas as pd
from datetime import datetime
import json
import _snowflake
from snowflake.snowpark.context import get_active_session


# Suggested questions organized by use case
SUGGESTED_QUESTIONS = {
    "Churn Analysis": [
        "Which customers are at highest risk of churning?",
        "What is the average churn risk score by segment?",
        "Show me High-Value Travelers with high churn risk",
        "Which states have the highest churn risk?",
    ],
    "Customer Segmentation": [
        "How many customers are in each segment?",
        "Compare lifetime value across segments",
        "Which segments have Premium cards?",
        "Show me Declining segment customers in California",
    ],
    "Spending Trends": [
        "What is the total spending in the last 90 days?",
        "Show spending trends in travel over last 6 months",
        "Which customers increased spending the most?",
        "What is the average transaction value by card type?",
    ],
    "Geographic Analysis": [
        "What is the average lifetime value by state?",
        "Which states have the most Premium cardholders?",
        "Show me customer distribution across states",
        "Compare spending between California and Texas",
    ],
    "Campaign Targeting": [
        "Show me customers eligible for retention campaigns",
        "Which Premium cardholders are at medium or high risk?",
        "Find customers with declining spend in the last 90 days",
        "Show high-value customers with low recent activity",
    ]
}


def call_cortex_analyst_mock(conn, question: str) -> dict:
    """
    Mock Cortex Analyst for testing when Cortex Analyst not available.
    Maps common questions to pre-written SQL.

    Args:
        conn: Snowflake connection
        question: Natural language question

    Returns:
        dict with keys: sql, results, error
    """
    question_lower = question.lower()

    # Map questions to SQL
    if 'highest risk' in question_lower and 'churn' in question_lower:
        sql = """
            SELECT customer_id, full_name, email, customer_segment,
                   churn_risk_score, churn_risk_category
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE churn_risk_category = 'High Risk'
            ORDER BY churn_risk_score DESC
            LIMIT 100
        """

    elif 'customers in each segment' in question_lower:
        sql = """
            SELECT customer_segment, COUNT(*) AS customer_count
            FROM GOLD.CUSTOMER_360_PROFILE
            GROUP BY customer_segment
            ORDER BY customer_count DESC
        """

    elif 'lifetime value' in question_lower and 'segment' in question_lower:
        sql = """
            SELECT customer_segment,
                   AVG(lifetime_value) AS avg_ltv,
                   COUNT(*) AS customer_count
            FROM GOLD.CUSTOMER_360_PROFILE
            GROUP BY customer_segment
            ORDER BY avg_ltv DESC
        """

    elif 'total spending' in question_lower and '90 days' in question_lower:
        sql = """
            SELECT SUM(spend_last_90_days) AS total_spend_90d,
                   COUNT(*) AS customer_count
            FROM GOLD.CUSTOMER_360_PROFILE
        """

    elif 'premium' in question_lower and ('medium' in question_lower or 'high' in question_lower) and 'risk' in question_lower:
        sql = """
            SELECT customer_id, full_name, customer_segment,
                   card_type, churn_risk_category, lifetime_value
            FROM GOLD.CUSTOMER_360_PROFILE
            WHERE card_type = 'Premium'
              AND churn_risk_category IN ('Medium Risk', 'High Risk')
            ORDER BY churn_risk_score DESC
            LIMIT 100
        """

    else:
        return {
            'sql': None,
            'results': None,
            'error': 'Question not recognized by mock. Try a suggested question or wait for Cortex Analyst integration.'
        }

    # Execute SQL
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        cursor.close()

        return {
            'sql': sql,
            'results': df,
            'error': None
        }
    except Exception as e:
        return {
            'sql': sql,
            'results': None,
            'error': str(e)
        }


def call_cortex_analyst(conn, question: str, conversation_history: list = None) -> dict:
    """
    Call Snowflake Cortex Analyst REST API to answer natural language question.
    Uses the native Streamlit in Snowflake API for authentication.

    Args:
        conn: Snowflake connection
        question: Natural language question
        conversation_history: Optional list of previous Q&A pairs for context

    Returns:
        dict with keys: sql, results, interpretation, error
    """
    try:
        # Build conversation history for context (multi-turn conversations)
        messages = []
        if conversation_history:
            for item in conversation_history[-3:]:  # Last 3 exchanges for context
                messages.append({
                    "role": "user",
                    "content": [{"type": "text", "text": item.get('question', '')}]
                })

        # Add current question
        messages.append({
            "role": "user",
            "content": [{"type": "text", "text": question}]
        })

        # Request payload
        request_body = {
            "messages": messages,
            "semantic_model_file": "@SEMANTIC_MODELS.DEFINITIONS.SEMANTIC_STAGE/customer_analytics.yaml"
        }

        # Use Snowflake's native API request function for Streamlit in Snowflake
        # This handles authentication automatically
        resp = _snowflake.send_snow_api_request(
            "POST",  # method
            "/api/v2/cortex/analyst/message",  # path
            {},  # headers
            {},  # params
            request_body,  # body
            None,  # request_guid
            50000,  # timeout in milliseconds
        )

        # Parse response content (it's a JSON string)
        parsed_content = json.loads(resp["content"])

        # Check if the response is successful
        if resp["status"] >= 400:
            error_msg = f"Cortex Analyst API error (status {resp['status']}): {parsed_content.get('message', 'Unknown error')}"
            st.warning(f"⚠️ {error_msg}. Using mock implementation.")
            return call_cortex_analyst_mock(conn, question)

        # Extract message content
        message = parsed_content.get('message', {})

        # Extract SQL and interpretation
        generated_sql = None
        interpretation = None

        # Try to find SQL in content blocks
        content = message.get('content', [])
        for item in content:
            if item.get('type') == 'sql':
                generated_sql = item.get('statement')
            elif item.get('type') == 'text':
                interpretation = item.get('text')

        if not generated_sql:
            return {
                'sql': None,
                'results': None,
                'interpretation': interpretation,
                'error': 'Cortex Analyst did not generate SQL for this question'
            }

        # Execute the generated SQL
        cursor = conn.cursor()
        cursor.execute(generated_sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        cursor.close()

        return {
            'sql': generated_sql,
            'results': df,
            'interpretation': interpretation,
            'error': None
        }

    except Exception as e:
        error_msg = str(e)
        st.warning(f"⚠️ Cortex Analyst error: {error_msg}. Using mock implementation.")
        return call_cortex_analyst_mock(conn, question)


def render(execute_query, conn):
    """
    Render AI Assistant tab with Cortex Analyst integration.

    Features:
    - Natural language question input
    - Suggested questions by category
    - Generated SQL display
    - Results table
    - Query history
    """
    st.title("🤖 AI Assistant")
    st.markdown("Ask questions about your customers in plain English")

    st.success("✨ **Powered by Snowflake Cortex Analyst** - Natural language to SQL with AI")

    # ========== SUGGESTED QUESTIONS ==========

    st.subheader("💡 Suggested Questions")

    # Category selector
    selected_category = st.selectbox(
        "Browse by category:",
        list(SUGGESTED_QUESTIONS.keys())
    )

    # Display suggested questions as clickable buttons
    st.markdown(f"**{selected_category}:**")

    cols = st.columns(2)
    for idx, question in enumerate(SUGGESTED_QUESTIONS[selected_category]):
        with cols[idx % 2]:
            if st.button(question, key=f"suggested_{selected_category}_{idx}"):
                st.session_state['current_question'] = question

    st.markdown("---")

    # ========== QUESTION INPUT ==========

    st.subheader("❓ Ask Your Question")

    # Text input for custom question
    default_question = st.session_state.get('current_question', '')

    question = st.text_area(
        "Enter your question:",
        value=default_question,
        height=100,
        placeholder="e.g., Which customers spent more than $10,000 in the last 90 days?"
    )

    col1, col2, col3 = st.columns([1, 1, 4])

    with col1:
        ask_button = st.button("🚀 Ask", type="primary")

    with col2:
        clear_button = st.button("🔄 Clear")

    if clear_button:
        st.session_state['current_question'] = ''
        st.session_state.pop('last_response', None)
        st.rerun()

    # ========== QUERY EXECUTION ==========

    if ask_button and question:
        with st.spinner("🤔 Thinking..."):
            # Get conversation history for context
            conversation_history = st.session_state.get('query_history', [])

            # Call Cortex Analyst with conversation context
            response = call_cortex_analyst(conn, question, conversation_history)

            st.session_state['last_response'] = response
            st.session_state['last_question'] = question

            # Add to history
            if 'query_history' not in st.session_state:
                st.session_state['query_history'] = []

            st.session_state['query_history'].append({
                'timestamp': datetime.now(),
                'question': question,
                'response': response
            })

    # ========== DISPLAY RESULTS ==========

    if 'last_response' in st.session_state:
        response = st.session_state['last_response']
        question = st.session_state.get('last_question', '')

        st.markdown("---")
        st.subheader("📊 Results")

        if response['error']:
            st.error(f"❌ Error: {response['error']}")

            st.info("""
            **Troubleshooting Tips:**
            - Rephrase your question to be more specific
            - Use terms from the semantic model (segment, state, churn risk, etc.)
            - Try one of the suggested questions above
            - Ensure Cortex Analyst is enabled in your Snowflake account
            """)

        else:
            # Display question
            st.markdown(f"**Question:** {question}")

            # Display AI interpretation if available
            if response.get('interpretation'):
                st.info(f"**AI Interpretation:** {response['interpretation']}")

            # Display generated SQL
            with st.expander("🔍 View Generated SQL", expanded=False):
                st.code(response['sql'], language='sql')

            # Display results
            df = response['results']

            if df is not None and not df.empty:
                st.success(f"✅ Found {len(df)} results")

                # Summary metrics (if applicable)
                if len(df) < 20 and len(df.columns) <= 5:
                    # Display as cards for small result sets
                    cols = st.columns(min(len(df.columns), 4))

                    for idx, col_name in enumerate(df.columns[:4]):
                        with cols[idx]:
                            if pd.api.types.is_numeric_dtype(df[col_name]):
                                value = df[col_name].iloc[0] if len(df) == 1 else df[col_name].sum()
                                if col_name.lower() in ['lifetime_value', 'total_spend', 'amount', 'avg_ltv', 'total_spend_90d']:
                                    st.metric(col_name, f"${value:,.0f}")
                                else:
                                    st.metric(col_name, f"{value:,.0f}")

                # Results table
                st.dataframe(df, use_container_width=True, height=400)

                # Export
                st.download_button(
                    label="📥 Download Results (CSV)",
                    data=df.to_csv(index=False),
                    file_name=f"cortex_analyst_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

            else:
                st.warning("No results found")

    # ========== QUERY HISTORY ==========

    if st.session_state.get('query_history'):
        st.markdown("---")
        st.subheader("📜 Query History")

        history = st.session_state['query_history']

        # Display last 5 queries
        for idx, item in enumerate(reversed(history[-5:])):
            with st.expander(f"{item['timestamp'].strftime('%H:%M:%S')} - {item['question'][:50]}..."):
                st.markdown(f"**Question:** {item['question']}")

                if item['response']['error']:
                    st.error(f"Error: {item['response']['error']}")
                else:
                    st.code(item['response']['sql'], language='sql')

                    if item['response']['results'] is not None:
                        st.dataframe(item['response']['results'], use_container_width=True)

    # ========== HELP SECTION ==========

    st.markdown("---")

    with st.expander("ℹ️ How to Use AI Assistant"):
        st.markdown("""
        **Tips for asking questions:**

        1. **Be specific:** Instead of "Show customers", try "Show customers in California with high churn risk"

        2. **Use domain terms:** The AI understands:
           - Customer segments: High-Value Travelers, Declining, New & Growing, Budget-Conscious, Stable Mid-Spenders
           - Churn risk: High Risk, Medium Risk, Low Risk
           - Card types: Standard, Premium
           - Metrics: lifetime value, churn risk score, spend last 90 days

        3. **Time periods:** Specify timeframes like "last 30 days", "last 90 days", "last 6 months"

        4. **Comparisons:** Ask to "compare" segments, states, or time periods

        5. **Filters:** Combine multiple criteria: "Premium cardholders in Texas with declining spend"

        **Powered by Snowflake Cortex Analyst** - Real-time natural language to SQL using AI

        *Note: If Cortex Analyst is not available in your Snowflake account, the system will automatically fallback to a mock implementation with pre-defined queries.*
        """)
