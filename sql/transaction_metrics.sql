-- Transaction Metrics — Gold Layer
-- Purpose: transaction KPIs for Analytics, Risk, and Business teams
-- Engine: Delta Lake SQL / Databricks SQL / Spark SQL

-- ============================================================
-- 1. Daily Transaction Summary
-- ============================================================
SELECT
    event_date,
    transaction_type,
    currency,
    tx_count,
    ROUND(total_volume, 2)          AS total_volume,
    ROUND(avg_amount, 2)            AS avg_amount,
    ROUND(p50_amount, 2)            AS median_amount,
    ROUND(p95_amount, 2)            AS p95_amount,
    ROUND(p99_amount, 2)            AS p99_amount,
    ROUND(success_rate * 100, 2)    AS success_rate_pct,
    failed_count,
    late_arrival_count
FROM gold.transaction_analytics
WHERE process_date = CURRENT_DATE()
  AND granularity = 'daily_by_type'
ORDER BY event_date DESC, total_volume DESC;

-- ============================================================
-- 2. 30-Day Volume Trend by Transaction Type
-- ============================================================
SELECT
    event_date,
    transaction_type,
    SUM(tx_count)                   AS tx_count,
    ROUND(SUM(total_volume), 2)     AS total_volume,
    ROUND(AVG(success_rate) * 100, 2) AS avg_success_rate_pct
FROM gold.transaction_analytics
WHERE process_date >= DATE_SUB(CURRENT_DATE(), 30)
  AND granularity = 'daily_by_type'
GROUP BY event_date, transaction_type
ORDER BY event_date ASC, transaction_type;

-- ============================================================
-- 3. Currency Breakdown — Today
-- ============================================================
SELECT
    currency,
    SUM(tx_count)                   AS total_transactions,
    ROUND(SUM(total_volume), 2)     AS total_volume,
    ROUND(SUM(total_volume) / SUM(SUM(total_volume)) OVER () * 100, 2) AS volume_share_pct
FROM gold.transaction_analytics
WHERE process_date = CURRENT_DATE()
  AND granularity = 'daily_by_type'
GROUP BY currency
ORDER BY total_volume DESC;

-- ============================================================
-- 4. Failure Rate Monitor — Alert if > 5% failures
-- ============================================================
SELECT
    event_date,
    transaction_type,
    tx_count,
    failed_count,
    ROUND(failed_count * 100.0 / NULLIF(tx_count, 0), 2) AS failure_rate_pct,
    CASE
        WHEN failed_count * 100.0 / NULLIF(tx_count, 0) > 5 THEN 'ALERT'
        WHEN failed_count * 100.0 / NULLIF(tx_count, 0) > 2 THEN 'WARNING'
        ELSE 'OK'
    END AS status
FROM gold.transaction_analytics
WHERE process_date = CURRENT_DATE()
  AND granularity = 'daily_by_type'
ORDER BY failure_rate_pct DESC;

-- ============================================================
-- 5. Peak Hours Analysis (last 7 days)
-- ============================================================
SELECT
    HOUR(hour)                      AS hour_of_day,
    transaction_type,
    SUM(tx_count)                   AS total_tx_count,
    ROUND(SUM(total_volume), 2)     AS total_volume,
    ROUND(AVG(tx_count), 1)         AS avg_tx_per_day
FROM gold.transaction_analytics
WHERE process_date >= DATE_SUB(CURRENT_DATE(), 7)
  AND granularity = 'hourly'
GROUP BY HOUR(hour), transaction_type
ORDER BY hour_of_day, total_tx_count DESC;

-- ============================================================
-- 6. Late Arrival Impact Report
-- ============================================================
SELECT
    event_date,
    SUM(tx_count)                   AS total_tx,
    SUM(late_arrival_count)         AS late_arrivals,
    ROUND(SUM(late_arrival_count) * 100.0 / NULLIF(SUM(tx_count), 0), 2) AS late_rate_pct
FROM gold.transaction_analytics
WHERE process_date >= DATE_SUB(CURRENT_DATE(), 7)
  AND granularity = 'daily_by_type'
GROUP BY event_date
ORDER BY event_date DESC;

-- ============================================================
-- 7. Data Quality Dashboard — DQ metrics over time
-- ============================================================
SELECT
    process_date,
    check_name,
    status,
    ROUND(pass_rate * 100, 4)       AS pass_rate_pct,
    ROUND(threshold * 100, 2)       AS threshold_pct,
    failed_count,
    total_count,
    checked_at
FROM silver.dq_metrics
WHERE process_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY process_date DESC, status, check_name;
