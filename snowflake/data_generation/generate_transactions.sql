-- ============================================================================
-- Generate Synthetic Transaction Data at Scale (13.5M rows)
-- ============================================================================
-- Purpose: Generate realistic transaction data for 50K customers over 18 months
-- Method: Use Snowflake GENERATOR() function for performance at scale
-- Target: ~13.5M transactions with segment-specific spending patterns
-- ============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH;  -- Consider using larger warehouse (MEDIUM/LARGE) for better performance
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA BRONZE;

-- ============================================================================
-- Part A: Create Date Spine (18 months, daily granularity)
-- ============================================================================

SELECT 'Part A: Creating date spine...' AS step;

CREATE OR REPLACE TEMP TABLE date_spine AS
SELECT
    DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE())) AS transaction_date,
    DATEDIFF('month', DATEADD('month', -18, CURRENT_DATE()),
             DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE()))) AS month_num
FROM TABLE(GENERATOR(ROWCOUNT => 540));  -- 18 months * 30 days

-- Verify date spine
SELECT
    'Date Spine Created' AS status,
    COUNT(*) AS total_days,
    MIN(transaction_date) AS start_date,
    MAX(transaction_date) AS end_date,
    DATEDIFF('month', MIN(transaction_date), MAX(transaction_date)) AS month_range
FROM date_spine;

-- ============================================================================
-- Part B: Determine Monthly Transaction Volume per Customer by Segment
-- ============================================================================

SELECT 'Part B: Calculating monthly transaction volumes...' AS step;

CREATE OR REPLACE TEMP TABLE customer_monthly_volume AS
SELECT
    c.customer_id,
    c.customer_segment,
    c.decline_type,
    d.transaction_date,
    d.month_num,
    -- Monthly transaction volume varies by segment
    CASE c.customer_segment
        WHEN 'High-Value Travelers' THEN UNIFORM(40, 80, RANDOM())
        WHEN 'Stable Mid-Spenders' THEN UNIFORM(20, 40, RANDOM())
        WHEN 'Budget-Conscious' THEN UNIFORM(15, 30, RANDOM())
        WHEN 'Declining' THEN UNIFORM(20, 40, RANDOM())
        WHEN 'New & Growing' THEN UNIFORM(25, 50, RANDOM())
    END AS monthly_transactions
FROM BRONZE.BRONZE_CUSTOMERS c
CROSS JOIN (
    SELECT DISTINCT transaction_date, month_num
    FROM date_spine
    WHERE DAY(transaction_date) = 1  -- One row per month
) d;

-- Verify customer monthly volume
SELECT
    'Customer Monthly Volume Created' AS status,
    COUNT(*) AS total_customer_months,
    SUM(monthly_transactions) AS estimated_total_transactions,
    ROUND(AVG(monthly_transactions), 2) AS avg_monthly_txns
FROM customer_monthly_volume;

-- ============================================================================
-- Part C: Expand to Individual Transactions
-- ============================================================================

SELECT 'Part C: Expanding to individual transactions...' AS step;

CREATE OR REPLACE TEMP TABLE transactions_expanded AS
SELECT
    cmv.customer_id,
    cmv.customer_segment,
    cmv.decline_type,
    cmv.month_num,
    -- Random day within the month
    DATEADD('day', UNIFORM(0, 28, RANDOM()), cmv.transaction_date) AS transaction_date,
    gen.SEQ4() AS txn_seq
FROM customer_monthly_volume cmv
CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 100)) gen  -- Max transactions per customer per month
WHERE gen.SEQ4() < cmv.monthly_transactions;  -- Filter to actual monthly volume

-- Verify transaction expansion
SELECT
    'Transactions Expanded' AS status,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(COUNT(*) / COUNT(DISTINCT customer_id), 2) AS avg_txns_per_customer
FROM transactions_expanded;

-- ============================================================================
-- Part D: Generate Transaction Details with Segment-Specific Patterns
-- ============================================================================

SELECT 'Part D: Generating transaction details...' AS step;

CREATE OR REPLACE TEMP TABLE transactions_with_details AS
SELECT
    -- Transaction ID: TXN + 10-digit zero-padded number
    'TXN' || LPAD(ROW_NUMBER() OVER (ORDER BY transaction_date, customer_id), 11, '0') AS transaction_id,

    customer_id,
    transaction_date,
    customer_segment,

    -- Transaction amount varies by segment and applies decline pattern
    CASE customer_segment
        WHEN 'High-Value Travelers' THEN
            ROUND(UNIFORM(50, 500, RANDOM()), 2)

        WHEN 'Stable Mid-Spenders' THEN
            ROUND(UNIFORM(30, 150, RANDOM()), 2)

        WHEN 'Budget-Conscious' THEN
            ROUND(UNIFORM(10, 80, RANDOM()), 2)

        WHEN 'Declining' THEN
            CASE decline_type
                WHEN 'gradual' THEN
                    -- Linear decline: 10% reduction per month after month 12
                    ROUND(
                        UNIFORM(30, 150, RANDOM()) *
                        GREATEST(0.4, 1 - ((month_num - 12) * 0.1)),
                        2
                    )
                WHEN 'sudden' THEN
                    -- Sudden drop: 60% reduction after month 16
                    ROUND(
                        UNIFORM(30, 150, RANDOM()) *
                        IFF(month_num < 16, 1.0, 0.4),
                        2
                    )
                ELSE
                    ROUND(UNIFORM(30, 150, RANDOM()), 2)
            END

        WHEN 'New & Growing' THEN
            -- 5% growth per month
            ROUND(
                UNIFORM(20, 100, RANDOM()) * (1 + month_num * 0.05),
                2
            )
    END AS transaction_amount,

    -- Merchant name (simplified)
    'Merchant_' || LPAD(UNIFORM(1, 1000, RANDOM())::STRING, 4, '0') AS merchant_name,

    -- Merchant category varies by segment
    CASE customer_segment
        WHEN 'High-Value Travelers' THEN
            ARRAY_CONSTRUCT('Travel', 'Dining', 'Hotels', 'Airlines')[UNIFORM(0, 3, RANDOM())]::STRING

        WHEN 'Budget-Conscious' THEN
            ARRAY_CONSTRUCT('Grocery', 'Gas', 'Utilities')[UNIFORM(0, 2, RANDOM())]::STRING

        ELSE
            ARRAY_CONSTRUCT(
                'Retail', 'Dining', 'Entertainment', 'Grocery',
                'Gas', 'Travel', 'Healthcare', 'Utilities'
            )[UNIFORM(0, 7, RANDOM())]::STRING
    END AS merchant_category,

    -- Transaction channel
    ARRAY_CONSTRUCT('Online', 'In-Store', 'Mobile')[UNIFORM(0, 2, RANDOM())]::STRING AS channel,

    -- Transaction status (97% approved, 3% declined)
    CASE
        WHEN UNIFORM(1, 100, RANDOM()) <= 97 THEN 'approved'
        ELSE 'declined'
    END AS status

FROM transactions_expanded
WHERE
    -- Filter out any negative amounts from decline logic
    CASE customer_segment
        WHEN 'High-Value Travelers' THEN UNIFORM(50, 500, RANDOM())
        WHEN 'Stable Mid-Spenders' THEN UNIFORM(30, 150, RANDOM())
        WHEN 'Budget-Conscious' THEN UNIFORM(10, 80, RANDOM())
        WHEN 'Declining' THEN
            CASE decline_type
                WHEN 'gradual' THEN UNIFORM(30, 150, RANDOM()) * GREATEST(0.4, 1 - ((month_num - 12) * 0.1))
                WHEN 'sudden' THEN UNIFORM(30, 150, RANDOM()) * IFF(month_num < 16, 1.0, 0.4)
                ELSE UNIFORM(30, 150, RANDOM())
            END
        WHEN 'New & Growing' THEN UNIFORM(20, 100, RANDOM()) * (1 + month_num * 0.05)
    END > 0;

-- Verify transaction details
SELECT
    'Transaction Details Generated' AS status,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT transaction_id) AS unique_transaction_ids,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(MIN(transaction_amount), 2) AS min_amount,
    ROUND(MAX(transaction_amount), 2) AS max_amount,
    MIN(transaction_date) AS earliest_date,
    MAX(transaction_date) AS latest_date
FROM transactions_with_details;

-- ============================================================================
-- Part E: Export to S3 for Bulk Load
-- ============================================================================

SELECT 'Part E: Exporting to S3...' AS step;

-- Export to S3 stage (compressed CSV)
COPY INTO @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical/transactions_historical.csv
FROM (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        transaction_amount,
        merchant_name,
        merchant_category,
        channel,
        status
    FROM transactions_with_details
    ORDER BY transaction_date, customer_id
)
FILE_FORMAT = (
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
HEADER = TRUE
OVERWRITE = TRUE
MAX_FILE_SIZE = 104857600;  -- 100MB files

-- ============================================================================
-- Summary Statistics
-- ============================================================================

SELECT '========== GENERATION SUMMARY ==========' AS summary;

-- Overall statistics
SELECT
    'Total Transactions' AS metric,
    COUNT(*) AS value
FROM transactions_with_details
UNION ALL
SELECT
    'Unique Customers',
    COUNT(DISTINCT customer_id)
FROM transactions_with_details
UNION ALL
SELECT
    'Unique Transaction IDs',
    COUNT(DISTINCT transaction_id)
FROM transactions_with_details
UNION ALL
SELECT
    'Average Amount',
    ROUND(AVG(transaction_amount), 2)
FROM transactions_with_details
UNION ALL
SELECT
    'Total Amount',
    ROUND(SUM(transaction_amount), 2)
FROM transactions_with_details
UNION ALL
SELECT
    'Date Range (Days)',
    DATEDIFF('day', MIN(transaction_date), MAX(transaction_date))
FROM transactions_with_details;

-- Segment breakdown
SELECT 'Segment Breakdown:' AS breakdown;

SELECT
    customer_segment,
    COUNT(*) AS transaction_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount,
    ROUND(SUM(transaction_amount), 2) AS total_amount
FROM transactions_with_details
GROUP BY customer_segment
ORDER BY transaction_count DESC;

-- Status breakdown
SELECT 'Status Breakdown:' AS breakdown;

SELECT
    status,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM transactions_with_details
GROUP BY status
ORDER BY transaction_count DESC;

-- Channel breakdown
SELECT 'Channel Breakdown:' AS breakdown;

SELECT
    channel,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM transactions_with_details
GROUP BY channel
ORDER BY transaction_count DESC;

-- Top merchant categories
SELECT 'Top Merchant Categories:' AS breakdown;

SELECT
    merchant_category,
    COUNT(*) AS transaction_count,
    ROUND(AVG(transaction_amount), 2) AS avg_amount
FROM transactions_with_details
GROUP BY merchant_category
ORDER BY transaction_count DESC
LIMIT 10;

-- ============================================================================
-- Verify Declining Segment Pattern
-- ============================================================================

SELECT 'Declining Segment Validation:' AS validation;

WITH declining_monthly AS (
    SELECT
        MONTH(transaction_date) AS month,
        AVG(transaction_amount) AS avg_amount
    FROM transactions_with_details
    WHERE customer_segment = 'Declining'
    GROUP BY MONTH(transaction_date)
    ORDER BY month
)
SELECT
    month,
    avg_amount,
    LAG(avg_amount) OVER (ORDER BY month) AS prev_month_amount,
    ROUND((avg_amount - LAG(avg_amount) OVER (ORDER BY month)) /
          NULLIF(LAG(avg_amount) OVER (ORDER BY month), 0) * 100, 2) AS pct_change
FROM declining_monthly;

-- ============================================================================
-- List Exported Files
-- ============================================================================

SELECT 'Exported Files in S3:' AS files;

LIST @CUSTOMER_ANALYTICS.BRONZE.transaction_stage_historical;

-- ============================================================================
-- Completion Message
-- ============================================================================

SELECT '✓ Transaction generation completed successfully' AS status;
SELECT 'Transactions exported to S3 stage: @transaction_stage_historical' AS next_step;
SELECT 'Next: Load transactions into Bronze layer (Iteration 2.5)' AS action;
