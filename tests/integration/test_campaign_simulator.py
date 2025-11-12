"""
Integration Tests for Campaign Simulator Tab (Iteration 5.4)

Tests Campaign Simulator tab functionality:
1. Target audience query building
2. ROI calculation
3. ROI calculation logic validation
4. Sensitivity analysis
5. Breakeven calculation
6. CSV export
7. Campaign recommendations

Run:
    pytest tests/integration/test_campaign_simulator.py -v
"""

import os
import pytest
import pandas as pd
from snowflake.connector import connect
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../streamlit'))

from tabs.campaign_simulator import calculate_campaign_roi

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
# Test 1: Target Audience Query
# ============================================================================


def test_target_audience_query(snowflake_conn):
    """
    Test building target audience query with filters.

    Validates:
    - Query executes successfully
    - Returns DataFrame
    - Filters applied correctly
    """
    # Build query with filters
    segment_options = ["Declining"]
    churn_risk_options = ["High Risk", "Medium Risk"]
    card_type_options = ["Standard", "Premium"]
    min_ltv = 5000
    min_churn_score = 40

    where_clauses = []

    if segment_options:
        segments_str = "', '".join(segment_options)
        where_clauses.append(f"customer_segment IN ('{segments_str}')")

    if churn_risk_options:
        risk_str = "', '".join(churn_risk_options)
        where_clauses.append(f"churn_risk_category IN ('{risk_str}')")

    if card_type_options:
        card_str = "', '".join(card_type_options)
        where_clauses.append(f"card_type IN ('{card_str}')")

    where_clauses.append(f"lifetime_value >= {min_ltv}")
    where_clauses.append(f"churn_risk_score >= {min_churn_score}")

    where_clause = " AND ".join(where_clauses)

    query = f"""
        SELECT
            customer_id,
            full_name,
            email,
            customer_segment,
            churn_risk_category,
            churn_risk_score,
            card_type,
            lifetime_value,
            avg_monthly_spend,
            spend_last_90_days,
            state
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE {where_clause}
        ORDER BY churn_risk_score DESC, lifetime_value DESC
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df = pd.DataFrame(results, columns=columns)

    assert df is not None, "Query returned None"
    assert isinstance(df, pd.DataFrame), "Query did not return DataFrame"

    if len(df) > 0:
        # Validate filters applied
        assert df['CUSTOMER_SEGMENT'].isin(segment_options).all(), \
            "Segment filter not applied correctly"
        assert df['CHURN_RISK_CATEGORY'].isin(churn_risk_options).all(), \
            "Churn risk filter not applied correctly"
        assert df['CARD_TYPE'].isin(card_type_options).all(), \
            "Card type filter not applied correctly"
        assert (df['LIFETIME_VALUE'] >= min_ltv).all(), \
            "LTV filter not applied correctly"
        assert (df['CHURN_RISK_SCORE'] >= min_churn_score).all(), \
            "Churn score filter not applied correctly"

        print(f"\n✓ Target audience query successful:")
        print(f"  Customers found: {len(df):,}")
        print(f"  Avg LTV: ${df['LIFETIME_VALUE'].mean():,.0f}")
        print(f"  Avg churn risk: {df['CHURN_RISK_SCORE'].mean():.2f}")
    else:
        print(f"\n✓ Target audience query executed (0 results - possible with strict filters)")


# ============================================================================
# Test 2: Calculate Campaign ROI
# ============================================================================


def test_calculate_campaign_roi(snowflake_conn):
    """
    Test ROI calculation function.

    Validates:
    - Function executes without error
    - Returns dict with expected keys
    - Calculations are reasonable
    """
    # Get sample target customers
    query = """
        SELECT
            customer_id,
            lifetime_value,
            avg_monthly_spend
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE churn_risk_category = 'High Risk'
        LIMIT 100
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df_targets = pd.DataFrame(results, columns=columns)

    if len(df_targets) == 0:
        pytest.skip("No high-risk customers found for testing")

    # Calculate ROI
    incentive = 50
    retention_rate = 30
    campaign_cost = 5

    roi_results = calculate_campaign_roi(
        df_targets,
        incentive,
        retention_rate,
        campaign_cost
    )

    # Validate structure
    assert roi_results is not None, "ROI results is None"
    assert isinstance(roi_results, dict), "ROI results not a dict"

    expected_keys = [
        'num_customers',
        'total_cost',
        'incentive_cost',
        'campaign_cost',
        'expected_retained_customers',
        'expected_retained_value',
        'net_benefit',
        'roi_pct',
        'cost_per_retained_customer'
    ]

    for key in expected_keys:
        assert key in roi_results, f"Missing key: {key}"

    # Validate values
    assert roi_results['num_customers'] == len(df_targets), "Customer count mismatch"
    assert roi_results['incentive_cost'] == len(df_targets) * incentive, \
        "Incentive cost calculation incorrect"
    assert roi_results['campaign_cost'] == len(df_targets) * campaign_cost, \
        "Campaign cost calculation incorrect"
    assert roi_results['total_cost'] == roi_results['incentive_cost'] + roi_results['campaign_cost'], \
        "Total cost calculation incorrect"

    print(f"\n✓ ROI calculation successful:")
    print(f"  Target customers: {roi_results['num_customers']:,}")
    print(f"  Total cost: ${roi_results['total_cost']:,.0f}")
    print(f"  Expected retained: {roi_results['expected_retained_customers']:,}")
    print(f"  Expected value: ${roi_results['expected_retained_value']:,.0f}")
    print(f"  Net benefit: ${roi_results['net_benefit']:,.0f}")
    print(f"  ROI: {roi_results['roi_pct']:.1f}%")


# ============================================================================
# Test 3: ROI Calculation Logic
# ============================================================================


def test_roi_calculation_logic():
    """
    Test ROI calculation formulas with synthetic data.

    Validates:
    - Retention rate percentage logic
    - Cost calculations
    - Value calculations
    - ROI percentage
    """
    # Create synthetic test data
    test_data = pd.DataFrame({
        'CUSTOMER_ID': range(1, 101),
        'LIFETIME_VALUE': [10000] * 100  # 100 customers, $10K LTV each
    })

    incentive = 100
    retention_rate = 40  # 40%
    campaign_cost = 10

    roi_results = calculate_campaign_roi(
        test_data,
        incentive,
        retention_rate,
        campaign_cost
    )

    # Expected calculations
    expected_num_customers = 100
    expected_incentive_cost = 100 * 100  # 100 customers * $100
    expected_campaign_cost = 100 * 10   # 100 customers * $10
    expected_total_cost = expected_incentive_cost + expected_campaign_cost  # $11,000
    expected_retained_customers = int(100 * 0.40)  # 40 customers
    expected_retained_value = 40 * 10000 * 0.20  # 40 * $10K * 20% = $80,000
    expected_net_benefit = expected_retained_value - expected_total_cost  # $80K - $11K = $69K
    expected_roi_pct = (expected_net_benefit / expected_total_cost) * 100  # ~627%

    # Validate
    assert roi_results['num_customers'] == expected_num_customers
    assert roi_results['incentive_cost'] == expected_incentive_cost
    assert roi_results['campaign_cost'] == expected_campaign_cost
    assert roi_results['total_cost'] == expected_total_cost
    assert roi_results['expected_retained_customers'] == expected_retained_customers
    assert roi_results['expected_retained_value'] == expected_retained_value
    assert roi_results['net_benefit'] == expected_net_benefit
    assert abs(roi_results['roi_pct'] - expected_roi_pct) < 0.01  # Allow small floating point error

    print(f"\n✓ ROI calculation logic validated:")
    print(f"  Total cost: ${roi_results['total_cost']:,.0f} (expected ${expected_total_cost:,.0f})")
    print(f"  Retained value: ${roi_results['expected_retained_value']:,.0f} (expected ${expected_retained_value:,.0f})")
    print(f"  Net benefit: ${roi_results['net_benefit']:,.0f} (expected ${expected_net_benefit:,.0f})")
    print(f"  ROI: {roi_results['roi_pct']:.1f}% (expected {expected_roi_pct:.1f}%)")


# ============================================================================
# Test 4: Sensitivity Analysis
# ============================================================================


def test_sensitivity_analysis(snowflake_conn):
    """
    Test sensitivity analysis across retention rates.

    Validates:
    - ROI calculated for range of retention rates
    - ROI increases with retention rate
    - Data suitable for visualization
    """
    # Get sample target customers
    query = """
        SELECT
            customer_id,
            lifetime_value,
            avg_monthly_spend
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE churn_risk_category = 'High Risk'
        LIMIT 50
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df_targets = pd.DataFrame(results, columns=columns)

    if len(df_targets) == 0:
        pytest.skip("No high-risk customers found for testing")

    incentive = 50
    campaign_cost = 5

    # Calculate ROI for range of retention rates
    retention_range = range(10, 81, 10)
    sensitivity_results = []

    for rate in retention_range:
        result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
        sensitivity_results.append({
            'Retention Rate (%)': rate,
            'ROI (%)': result['roi_pct'],
            'Net Benefit ($)': result['net_benefit']
        })

    df_sensitivity = pd.DataFrame(sensitivity_results)

    assert len(df_sensitivity) == len(retention_range), "Missing sensitivity data points"
    assert 'Retention Rate (%)' in df_sensitivity.columns
    assert 'ROI (%)' in df_sensitivity.columns
    assert 'Net Benefit ($)' in df_sensitivity.columns

    # Validate ROI increases with retention rate
    assert df_sensitivity['ROI (%)'].is_monotonic_increasing, \
        "ROI should increase with retention rate"

    print(f"\n✓ Sensitivity analysis validated:")
    print(f"  Retention rates tested: {list(retention_range)}")
    print(f"  ROI range: {df_sensitivity['ROI (%)'].min():.1f}% to {df_sensitivity['ROI (%)'].max():.1f}%")
    print(f"\n  Sample points:")
    for idx, row in df_sensitivity.head(3).iterrows():
        print(f"    {row['Retention Rate (%)']:.0f}%: ROI = {row['ROI (%)']:.1f}%, Net = ${row['Net Benefit ($)']:,.0f}")


# ============================================================================
# Test 5: Breakeven Calculation
# ============================================================================


def test_breakeven_calculation(snowflake_conn):
    """
    Test breakeven retention rate calculation.

    Validates:
    - Breakeven point found
    - ROI is near zero at breakeven
    """
    # Get sample target customers
    query = """
        SELECT
            customer_id,
            lifetime_value,
            avg_monthly_spend
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE churn_risk_category = 'High Risk'
        LIMIT 50
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df_targets = pd.DataFrame(results, columns=columns)

    if len(df_targets) == 0:
        pytest.skip("No high-risk customers found for testing")

    incentive = 50
    campaign_cost = 5

    # Find breakeven point
    breakeven_rate = None
    for rate in range(1, 101):
        result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
        if result['roi_pct'] >= 0:
            breakeven_rate = rate
            break

    if breakeven_rate:
        # Validate breakeven
        result = calculate_campaign_roi(df_targets, incentive, breakeven_rate, campaign_cost)

        assert result['roi_pct'] >= 0, "Breakeven ROI should be >= 0"

        # Validate previous rate was negative
        if breakeven_rate > 1:
            prev_result = calculate_campaign_roi(df_targets, incentive, breakeven_rate - 1, campaign_cost)
            assert prev_result['roi_pct'] < 0, "Previous rate should have negative ROI"

        print(f"\n✓ Breakeven calculation successful:")
        print(f"  Breakeven retention rate: {breakeven_rate}%")
        print(f"  ROI at breakeven: {result['roi_pct']:.2f}%")
        print(f"  Net benefit: ${result['net_benefit']:,.0f}")
    else:
        print(f"\n✓ Breakeven calculation complete (no breakeven found - campaign not viable)")


# ============================================================================
# Test 6: Export Target List
# ============================================================================


def test_export_target_list(snowflake_conn):
    """
    Test CSV export of target customer list.

    Validates:
    - DataFrame converts to CSV
    - CSV has headers
    - CSV has data
    """
    # Get sample target customers
    query = """
        SELECT
            customer_id,
            full_name,
            email,
            customer_segment,
            churn_risk_category,
            churn_risk_score,
            lifetime_value,
            avg_monthly_spend
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE churn_risk_category = 'High Risk'
        LIMIT 20
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df_targets = pd.DataFrame(results, columns=columns)

    if len(df_targets) == 0:
        pytest.skip("No high-risk customers found for testing")

    # Convert to CSV
    csv = df_targets.to_csv(index=False)

    assert csv is not None, "CSV conversion failed"
    assert len(csv) > 0, "CSV is empty"

    csv_lines = csv.split('\n')
    assert 'CUSTOMER_ID' in csv_lines[0], "CSV missing CUSTOMER_ID header"
    assert 'FULL_NAME' in csv_lines[0], "CSV missing FULL_NAME header"
    assert 'EMAIL' in csv_lines[0], "CSV missing EMAIL header"
    assert len(csv_lines) > 1, "CSV has no data rows"

    print(f"\n✓ CSV export successful:")
    print(f"  CSV size: {len(csv):,} characters")
    print(f"  CSV lines: {len(csv_lines):,}")
    print(f"  Customers: {len(df_targets):,}")


# ============================================================================
# Test 7: Campaign Recommendations
# ============================================================================


def test_campaign_recommendations(snowflake_conn):
    """
    Test campaign recommendations generation.

    Validates:
    - Recommendations include key metrics
    - Recommendations are actionable
    """
    # Get sample target customers
    query = """
        SELECT
            customer_id,
            lifetime_value,
            avg_monthly_spend,
            churn_risk_score
        FROM GOLD.CUSTOMER_360_PROFILE
        WHERE churn_risk_category IN ('High Risk', 'Medium Risk')
        LIMIT 100
    """

    cursor = snowflake_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()

    df_targets = pd.DataFrame(results, columns=columns)

    if len(df_targets) == 0:
        pytest.skip("No at-risk customers found for testing")

    incentive = 50
    retention_rate = 30
    campaign_cost = 5

    roi_results = calculate_campaign_roi(
        df_targets,
        incentive,
        retention_rate,
        campaign_cost
    )

    # Find breakeven
    breakeven_rate = None
    for rate in range(1, 101):
        result = calculate_campaign_roi(df_targets, incentive, rate, campaign_cost)
        if result['roi_pct'] >= 0:
            breakeven_rate = rate
            break

    # Generate recommendations text (simulating what would appear in UI)
    recommendations = f"""
    **Based on your target audience ({roi_results['num_customers']:,} customers):**

    ✅ **Recommended Actions:**
    - Current ROI: **{roi_results['roi_pct']:.1f}%**
    - Target retention breakeven: **{breakeven_rate}%**
    - Focus on customers with churn risk score > 60
    - Personalize incentives based on customer segment

    📧 **Campaign Messaging:**
    - Emphasize benefits of staying (rewards, benefits)
    - Highlight exclusive offers for loyal customers
    - Create urgency with limited-time offers

    ⏰ **Timing:**
    - Deploy within 7 days for high-risk customers
    - Follow up after 2 weeks
    - Monitor spend changes in next 30 days

    📊 **Success Metrics:**
    - Track retention rate (target: {retention_rate}%+)
    - Monitor spend increase among retained customers
    - Calculate actual ROI vs projected
    """

    # Validate recommendations contain key metrics
    assert str(roi_results['num_customers']) in recommendations, \
        "Recommendations missing customer count"
    assert f"{roi_results['roi_pct']:.1f}" in recommendations, \
        "Recommendations missing ROI percentage"

    if breakeven_rate:
        assert str(breakeven_rate) in recommendations, \
            "Recommendations missing breakeven rate"

    print(f"\n✓ Campaign recommendations validated:")
    print(f"  Target customers: {roi_results['num_customers']:,}")
    print(f"  Current ROI: {roi_results['roi_pct']:.1f}%")
    print(f"  Breakeven retention: {breakeven_rate}%" if breakeven_rate else "  No breakeven found")


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
