"""
Utility functions for Streamlit application
"""

import pandas as pd


# Mapping of common abbreviations and acronyms to their full forms
ACRONYM_MAPPING = {
    'id': 'Id',
    'ltv': 'Lifetime Value',
    'clv': 'Customer Lifetime Value',
    'roi': 'Return On Investment',
    'roas': 'Return On Ad Spend',
    'aov': 'Average Order Value',
    'avg': 'Average',
    'min': 'Minimum',
    'max': 'Maximum',
    'std': 'Standard Deviation',
    'pct': 'Percent',
    'cvr': 'Conversion Rate',
    'ctr': 'Click Through Rate',
    'num': 'Number',
    'qty': 'Quantity',
}


def format_column_name(col_name: str) -> str:
    """
    Convert database column names to human-readable format.

    Examples:
        customer_id -> Customer Id
        CUSTOMER_ID -> Customer Id
        transaction_date -> Transaction Date
        days_since_last_transaction -> Days Since Last Transaction
        avg_transaction_value -> Average Transaction Value
        ltv -> Lifetime Value
        roi -> Return On Investment

    Args:
        col_name: Database column name (snake_case or SCREAMING_SNAKE_CASE)

    Returns:
        Human-readable column name in Title Case with acronyms spelled out
    """
    # Convert to lowercase first
    col_name = col_name.lower()

    # Replace underscores with spaces
    col_name = col_name.replace('_', ' ')

    # Split into words
    words = col_name.split()
    formatted_words = []

    for word in words:
        # Check if the word is a known acronym/abbreviation
        if word in ACRONYM_MAPPING:
            formatted_words.append(ACRONYM_MAPPING[word])
        else:
            # Title case for regular words
            formatted_words.append(word.capitalize())

    return ' '.join(formatted_words)


def format_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply human-readable column names to a dataframe.

    Args:
        df: DataFrame with database column names

    Returns:
        DataFrame with human-readable column names
    """
    # Create a mapping of old names to new names
    column_mapping = {col: format_column_name(col) for col in df.columns}

    # Rename columns
    return df.rename(columns=column_mapping)
