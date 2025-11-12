"""
Configuration constants for customer and transaction data generation.

This module contains all configuration parameters for synthetic data generation
including customer segments, spending patterns, and demographic distributions.
"""

# Customer Segment Definitions
# Percentages must sum to 100%
SEGMENTS = {
    "High-Value Travelers": 0.15,      # 15% - Premium customers, frequent travelers
    "Stable Mid-Spenders": 0.40,       # 40% - Consistent spending, low churn risk
    "Budget-Conscious": 0.25,          # 25% - Low spend, high frequency
    "Declining": 0.10,                 # 10% - At-risk, decreasing engagement
    "New & Growing": 0.10,             # 10% - Recent customers, increasing spend
}

# Monthly spend ranges by segment (min, max) in dollars
SEGMENT_SPEND_RANGES = {
    "High-Value Travelers": (2000, 8000),
    "Stable Mid-Spenders": (800, 2500),
    "Budget-Conscious": (200, 800),
    "Declining": (500, 2000),          # Historical spend before decline
    "New & Growing": (300, 1200),
}

# Card Types
CARD_TYPES = ["Standard", "Premium"]

# Employment Statuses
EMPLOYMENT_STATUSES = ["Employed", "Self-Employed", "Retired", "Unemployed"]

# US States (50 states + DC)
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
]

# Credit Limit Ranges
MIN_CREDIT_LIMIT = 5000
MAX_CREDIT_LIMIT = 50000
CREDIT_LIMIT_STEP = 1000

# Age Ranges
MIN_AGE = 22
MAX_AGE = 75

# Account Open Date Range (years ago)
ACCOUNT_OPEN_MIN_YEARS_AGO = 5
ACCOUNT_OPEN_MAX_YEARS_AGO = 2

# Decline Types (for Declining segment only)
DECLINE_TYPES = ["gradual", "sudden"]
GRADUAL_DECLINE_PERCENTAGE = 0.70  # 70% of declining customers have gradual decline
