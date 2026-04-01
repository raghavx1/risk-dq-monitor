-- Raw loan/credit risk data table
CREATE TABLE IF NOT EXISTS raw_loans (
    loan_id         TEXT,
    customer_id     TEXT,
    loan_amount     REAL,
    loan_type       TEXT,
    risk_rating     TEXT,
    pd_score        REAL,
    lgd_score       REAL,
    exposure_at_default REAL,
    region          TEXT,
    origination_date TEXT,
    maturity_date   TEXT,
    status          TEXT,
    sector          TEXT,
    currency        TEXT,
    loaded_at       TEXT
);

-- Data quality results log (one row per check per run)
CREATE TABLE IF NOT EXISTS dq_results (
    run_id          TEXT,
    check_name      TEXT,
    table_name      TEXT,
    column_name     TEXT,
    status          TEXT,   -- PASS / FAIL / WARN
    records_checked INTEGER,
    records_failed  INTEGER,
    failure_rate    REAL,
    details         TEXT,
    checked_at      TEXT
);
