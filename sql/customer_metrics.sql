-- Customer Metrics — Gold Layer
-- Purpose: top-level KPIs consumed by Analytics and Business teams
-- Engine: Delta Lake SQL / Databricks SQL / Spark SQL

-- ============================================================
-- 1. Customer Summary — last 30 days
-- ============================================================
SELECT
    c.customer_id,
    c.name,
    c.country,
    c.segment,
    c.activity_tier,
    c.total_transactions,
    ROUND(c.total_spend, 2)                         AS total_spend,
    ROUND(c.avg_transaction_value, 2)               AS avg_transaction_value,
    ROUND(c.p95_transaction_value, 2)               AS p95_transaction_value,
    c.active_days,
    c.recency_days,
    c.rfm_score,
    c.last_transaction_at,
    c.process_date
FROM gold.customer_analytics c
WHERE c.process_date = CURRENT_DATE()
ORDER BY c.total_spend DESC;

-- ============================================================
-- 2. Segment Distribution
-- ============================================================
SELECT
    segment,
    activity_tier,
    COUNT(customer_id)              AS customer_count,
    ROUND(SUM(total_spend), 2)      AS segment_total_spend,
    ROUND(AVG(avg_transaction_value), 2) AS segment_avg_tx_value,
    ROUND(AVG(rfm_score), 2)        AS avg_rfm_score
FROM gold.customer_analytics
WHERE process_date = CURRENT_DATE()
GROUP BY segment, activity_tier
ORDER BY segment, customer_count DESC;

-- ============================================================
-- 3. Top 10 Customers by Spend (last 30 days)
-- ============================================================
SELECT
    customer_id,
    name,
    country,
    segment,
    ROUND(total_spend, 2)           AS total_spend,
    total_transactions,
    ROUND(avg_transaction_value, 2) AS avg_tx_value,
    activity_tier
FROM gold.customer_analytics
WHERE process_date = CURRENT_DATE()
ORDER BY total_spend DESC
LIMIT 10;

-- ============================================================
-- 4. Churn Risk Report — At-Risk and Churned customers
-- ============================================================
SELECT
    customer_id,
    name,
    country,
    segment,
    recency_days,
    frequency_score,
    monetary_score,
    rfm_score,
    activity_tier,
    last_transaction_at
FROM gold.customer_analytics
WHERE process_date = CURRENT_DATE()
  AND activity_tier IN ('at_risk', 'churned')
ORDER BY rfm_score ASC, recency_days DESC;

-- ============================================================
-- 5. Customer Activity Trend — 30-day rolling window
-- ============================================================
SELECT
    process_date,
    activity_tier,
    COUNT(customer_id)              AS customer_count,
    ROUND(SUM(total_spend), 2)      AS total_spend
FROM gold.customer_analytics
WHERE process_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY process_date, activity_tier
ORDER BY process_date, activity_tier;

-- ============================================================
-- 6. New vs Returning Customers (based on first_transaction_at)
-- ============================================================
SELECT
    process_date,
    SUM(CASE WHEN first_transaction_at >= DATE_SUB(process_date, 30)
             THEN 1 ELSE 0 END)     AS new_customers,
    SUM(CASE WHEN first_transaction_at < DATE_SUB(process_date, 30)
             THEN 1 ELSE 0 END)     AS returning_customers
FROM gold.customer_analytics
WHERE process_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY process_date
ORDER BY process_date;
