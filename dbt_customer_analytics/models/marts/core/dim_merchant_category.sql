{{
    config(
        materialized='table',
        schema='gold',
        tags=['gold', 'dimension', 'merchant']
    )
}}

{#
============================================================================
Gold Layer: Merchant Category Dimension
============================================================================
Purpose: Merchant category dimension for transaction analysis

Grain: One row per unique merchant category
Coverage: All categories from transactions
Attributes: Category name and grouping

Category Groups:
- Leisure: Travel, Dining, Hotels, Airlines, Entertainment
- Necessities: Grocery, Gas, Utilities, Healthcare
- Retail: Shopping and retail
- Other: Uncategorized or misc

Usage:
  -- Join fact table
  JOIN dim_merchant_category cat ON f.merchant_category_key = cat.category_key

  -- Filter by group
  WHERE cat.category_group = 'Leisure'
============================================================================
#}

WITH categories AS (
    -- Get distinct categories from staging
    SELECT DISTINCT merchant_category
    FROM {{ ref('stg_transactions') }}
),

category_mapping AS (
    SELECT
        merchant_category AS category_name,

        -- Group categories into higher-level buckets
        CASE merchant_category
            -- Leisure spending
            WHEN 'Travel' THEN 'Leisure'
            WHEN 'Dining' THEN 'Leisure'
            WHEN 'Hotels' THEN 'Leisure'
            WHEN 'Airlines' THEN 'Leisure'
            WHEN 'Entertainment' THEN 'Leisure'

            -- Necessities
            WHEN 'Grocery' THEN 'Necessities'
            WHEN 'Gas' THEN 'Necessities'
            WHEN 'Utilities' THEN 'Necessities'
            WHEN 'Healthcare' THEN 'Necessities'

            -- Retail
            WHEN 'Retail' THEN 'Retail'

            -- Other/Uncategorized
            ELSE 'Other'
        END AS category_group,

        -- Additional attributes
        CASE merchant_category
            WHEN 'Travel' THEN 'High discretionary spending'
            WHEN 'Dining' THEN 'Moderate discretionary spending'
            WHEN 'Hotels' THEN 'High discretionary spending'
            WHEN 'Airlines' THEN 'High discretionary spending'
            WHEN 'Entertainment' THEN 'Moderate discretionary spending'
            WHEN 'Grocery' THEN 'Essential spending'
            WHEN 'Gas' THEN 'Essential spending'
            WHEN 'Utilities' THEN 'Essential spending'
            WHEN 'Healthcare' THEN 'Essential spending'
            WHEN 'Retail' THEN 'Variable spending'
            ELSE 'Other spending'
        END AS spending_type,

        -- Discretionary vs Essential
        CASE merchant_category
            WHEN 'Travel' THEN 'Discretionary'
            WHEN 'Dining' THEN 'Discretionary'
            WHEN 'Hotels' THEN 'Discretionary'
            WHEN 'Airlines' THEN 'Discretionary'
            WHEN 'Entertainment' THEN 'Discretionary'
            WHEN 'Retail' THEN 'Discretionary'
            WHEN 'Grocery' THEN 'Essential'
            WHEN 'Gas' THEN 'Essential'
            WHEN 'Utilities' THEN 'Essential'
            WHEN 'Healthcare' THEN 'Essential'
            ELSE 'Other'
        END AS discretionary_flag

    FROM categories
)

SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY category_name) AS category_key,

    -- Attributes
    category_name,
    category_group,
    spending_type,
    discretionary_flag

FROM category_mapping
ORDER BY category_name
