{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'date']
    )
}}

{#
============================================================================
Gold Layer: Date Dimension
============================================================================
Purpose: Date dimension for time-based analysis of transactions

Grain: One row per calendar day
Coverage: Transaction date range (~18 months) + buffer
Attributes: Year, quarter, month, week, day, weekend flag

Usage:
  -- Join fact table by date_key
  JOIN dim_date d ON f.date_key = d.date_key

  -- Filter by time period
  WHERE d.year = 2024 AND d.month = 6
============================================================================
#}

WITH date_spine AS (
    -- Generate daily dates for 18 months + 30 day buffer
    SELECT
        DATEADD('day', SEQ4(), DATEADD('month', -18, CURRENT_DATE())) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 580))  -- 18 months * 30 days + 30 buffer
),

date_attributes AS (
    SELECT
        date_day,

        -- Date key (YYYYMMDD format as integer)
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,

        -- Year attributes
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        CONCAT('Q', QUARTER(date_day), ' ', YEAR(date_day)) AS quarter_name,

        -- Month attributes
        MONTH(date_day) AS month,
        MONTHNAME(date_day) AS month_name,
        CONCAT(MONTHNAME(date_day), ' ', YEAR(date_day)) AS month_year,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,

        -- Week attributes
        WEEKOFYEAR(date_day) AS week_of_year,
        WEEKISO(date_day) AS week_iso,

        -- Day attributes
        DAY(date_day) AS day_of_month,
        DAYOFWEEK(date_day) AS day_of_week,  -- 0 = Sunday, 6 = Saturday
        DAYNAME(date_day) AS day_name,
        DAYOFYEAR(date_day) AS day_of_year,

        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,

        -- Relative date indicators
        CASE WHEN date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = CURRENT_DATE() - 1 THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN date_day = CURRENT_DATE() + 1 THEN TRUE ELSE FALSE END AS is_tomorrow,

        -- First/last day flags
        CASE WHEN DAY(date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
        CASE WHEN date_day = LAST_DAY(date_day) THEN TRUE ELSE FALSE END AS is_last_day_of_month,

        -- Fiscal attributes (assuming fiscal year = calendar year)
        YEAR(date_day) AS fiscal_year,
        QUARTER(date_day) AS fiscal_quarter

    FROM date_spine
)

SELECT * FROM date_attributes
ORDER BY date_key
