-- Business View 1: Risk tier summary
-- Groups loans by risk rating and calculates portfolio-level exposure metrics
CREATE VIEW IF NOT EXISTS v_risk_tier_summary AS
SELECT
    risk_rating,
    COUNT(*)                            AS loan_count,
    ROUND(SUM(loan_amount), 2)          AS total_exposure,
    ROUND(AVG(pd_score), 4)             AS avg_pd,
    ROUND(AVG(lgd_score), 4)            AS avg_lgd,
    ROUND(AVG(pd_score * lgd_score * exposure_at_default), 2) AS avg_expected_loss
FROM raw_loans
WHERE status = 'ACTIVE'
GROUP BY risk_rating
ORDER BY avg_pd DESC;

-- Business View 2: Sector exposure bucketed
-- Breaks exposure into buckets per sector for concentration risk reporting
CREATE VIEW IF NOT EXISTS v_sector_exposure AS
SELECT
    sector,
    region,
    COUNT(*)                            AS loan_count,
    ROUND(SUM(loan_amount), 2)          AS total_exposure,
    ROUND(AVG(pd_score), 4)             AS avg_pd,
    CASE
        WHEN SUM(loan_amount) > 5000000  THEN 'HIGH'
        WHEN SUM(loan_amount) > 1000000  THEN 'MEDIUM'
        ELSE 'LOW'
    END                                 AS concentration_tier
FROM raw_loans
WHERE status = 'ACTIVE'
GROUP BY sector, region;

-- Business View 3: Daily portfolio snapshot
-- Summarises portfolio metrics per origination date for trend analysis
CREATE VIEW IF NOT EXISTS v_daily_portfolio AS
SELECT
    DATE(origination_date)              AS snapshot_date,
    COUNT(*)                            AS new_loans,
    ROUND(SUM(loan_amount), 2)          AS new_volume,
    ROUND(AVG(pd_score), 4)             AS avg_pd,
    SUM(CASE WHEN status='DEFAULTED' THEN 1 ELSE 0 END) AS defaults
FROM raw_loans
GROUP BY DATE(origination_date)
ORDER BY snapshot_date;

-- Business View 4: DQ health summary (latest run)
-- Aggregates the most recent DQ run results for the dashboard
CREATE VIEW IF NOT EXISTS v_dq_health AS
SELECT
    check_name,
    table_name,
    column_name,
    status,
    records_checked,
    records_failed,
    ROUND(failure_rate * 100, 2)        AS failure_pct,
    details,
    checked_at
FROM dq_results
WHERE run_id = (SELECT MAX(run_id) FROM dq_results);
