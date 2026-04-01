# Risk Data Quality Monitor

An end-to-end data quality pipeline for loan/credit risk data, built to mirror the data wrangling, validation, and reporting workflows used in bank risk functions.

Ingests raw loan records, applies SQL business-view transformations, runs a suite of automated DQ checks, and produces a governance-ready HTML report alongside Tableau-ready CSVs.

---

## Project Structure

```
project/
├── main.py                   # Pipeline entry point
├── requirements.txt
├── src/
│   ├── ingest.py             # Generate & load synthetic loan data into SQLite
│   ├── transform.py          # Build SQL business views
│   ├── dq_checks.py          # Run 8 data quality rule categories
│   ├── report.py             # Generate HTML governance report
│   └── dashboard_export.py   # Export 5 Tableau-ready CSVs
├── sql/
│   ├── create_tables.sql     # DDL for raw_loans and dq_results tables
│   └── business_views.sql    # 4 risk-reporting SQL views
└── outputs/                  # Auto-created on first run
    ├── dq_report_*.html
    └── tableau/
        ├── dq_summary.csv
        ├── risk_tier_summary.csv
        ├── sector_exposure.csv
        ├── daily_portfolio.csv
        └── dq_trend.csv
```

---

## Quickstart

```bash
# 1. Clone the repo
git clone https://github.com/your-username/risk-dq-monitor.git
cd risk-dq-monitor

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python main.py
```

The `outputs/` folder and `risk_dq.db` are created automatically on first run.

To generate a larger dataset:
```bash
python main.py --records 5000
```

---

## DQ Checks

| Check | Rule |
|---|---|
| Null completeness | 9 critical fields must not be null |
| Negative values | `loan_amount` and `exposure_at_default` must be positive |
| Range validation | `pd_score` and `lgd_score` must be in \[0, 1\] |
| Duplicate IDs | `loan_id` must be unique |
| Referential integrity | `risk_rating` must be a known enum value |
| Date logic | `maturity_date` must be after `origination_date` |
| Cross-field validation | Defaulted loans should have `pd_score` > 0.05 |
| Currency whitelist | `currency` must be from an approved CCY list |

Each check logs structured results to `dq_results` — enabling quality trending over time, in line with BCBS 239 data quality standards.

---

## Tableau Setup

1. Open Tableau Public → Connect → Text File
2. Load each CSV from `outputs/tableau/` as a separate data source
3. Recommended sheets:
   - `dq_summary.csv` → Horizontal bar chart (failure % by check)
   - `risk_tier_summary.csv` → Bar chart (exposure by risk rating)
   - `sector_exposure.csv` → Treemap (exposure, coloured by concentration tier)
   - `daily_portfolio.csv` → Dual-axis line (volume + avg PD over time)
   - `dq_trend.csv` → Line chart (fail/warn/pass count across runs)

---

## Tech Stack

- **Python** — pipeline orchestration
- **SQLite** — lightweight data store (swap for PostgreSQL in production)
- **Pandas / NumPy** — data wrangling and DQ logic
- **SQL** — business views and transformations
- **Tableau** — dashboard visualisation (via CSV export)
