"""
dashboard_export.py
-------------------
Exports all business views as clean CSVs ready to be loaded into
Tableau Public (or Tableau Desktop).

Each CSV maps to one sheet in the recommended Tableau workbook:
  1. dq_summary.csv          → DQ Health scorecard sheet
  2. risk_tier_summary.csv   → Risk distribution bar/heatmap
  3. sector_exposure.csv     → Sector concentration treemap
  4. daily_portfolio.csv     → Time-series trend line
  5. dq_trend.csv            → DQ failure rate over multiple runs

Key outputs
-----------
- outputs/tableau/*.csv
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs", "tableau")


def _safe_read(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    try:
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"  [WARN] Query failed: {e}")
        return pd.DataFrame()


def export_dq_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    DQ results from the latest run — one row per check.
    Tableau sheet: horizontal bar chart of failure_pct by check_name,
    coloured by status.
    """
    df = _safe_read(conn, """
        SELECT
            check_name,
            column_name,
            status,
            records_checked,
            records_failed,
            ROUND(failure_rate * 100, 2) AS failure_pct,
            details,
            checked_at
        FROM dq_results
        WHERE run_id = (SELECT MAX(run_id) FROM dq_results)
        ORDER BY failure_pct DESC
    """)
    # Add a numeric severity for Tableau colour encoding
    severity_map = {"FAIL": 3, "WARN": 2, "PASS": 1}
    if not df.empty:
        df["severity"] = df["status"].map(severity_map)
    return df


def export_risk_tier(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Risk tier aggregates — for portfolio distribution visuals.
    Tableau sheet: stacked bar or heatmap of exposure by risk rating.
    """
    return _safe_read(conn, "SELECT * FROM v_risk_tier_summary")


def export_sector_exposure(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Sector × region exposure — for concentration risk treemap.
    Tableau sheet: treemap sized by total_exposure, coloured by concentration_tier.
    """
    return _safe_read(conn, "SELECT * FROM v_sector_exposure")


def export_daily_portfolio(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Daily portfolio time series — for trend analysis.
    Tableau sheet: dual-axis line chart (volume + avg PD over time).
    """
    return _safe_read(conn, "SELECT * FROM v_daily_portfolio ORDER BY snapshot_date")


def export_dq_trend(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Aggregated DQ metrics per run — for historical quality trending.
    Tableau sheet: line chart showing FAIL/WARN/PASS counts improving over time.
    """
    return _safe_read(conn, """
        SELECT
            run_id,
            checked_at,
            SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END)  AS pass_count,
            SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END)  AS warn_count,
            SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END)  AS fail_count,
            COUNT(*)                                          AS total_checks,
            ROUND(AVG(failure_rate) * 100, 4)                AS avg_failure_pct
        FROM dq_results
        GROUP BY run_id, DATE(checked_at)
        ORDER BY checked_at
    """)


EXPORTS = {
    "dq_summary.csv":        export_dq_summary,
    "risk_tier_summary.csv": export_risk_tier,
    "sector_exposure.csv":   export_sector_exposure,
    "daily_portfolio.csv":   export_daily_portfolio,
    "dq_trend.csv":          export_dq_trend,
}


def run(conn: sqlite3.Connection) -> list[str]:
    """
    Entry point called by main.py.
    Exports all CSVs and returns list of file paths.
    """
    print("\n[EXPORT] Writing Tableau-ready CSVs...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    paths = []
    for filename, export_fn in EXPORTS.items():
        df   = export_fn(conn)
        path = os.path.join(OUTPUT_DIR, filename)
        if not df.empty:
            df.to_csv(path, index=False)
            print(f"  ✓ {filename} ({len(df)} rows)")
            paths.append(path)
        else:
            print(f"  ⚠ {filename} — no data, skipped")

    print(f"[EXPORT] {len(paths)} CSVs written to outputs/tableau/ ✓")
    print("\n  📌 Tableau setup instructions:")
    print("     1. Open Tableau Public → Connect → Text File")
    print("     2. Load each CSV as a separate data source")
    print("     3. Recommended sheets:")
    print("        • dq_summary.csv       → Horizontal bar (failure_pct by check_name)")
    print("        • risk_tier_summary.csv → Bar chart (exposure by risk_rating)")
    print("        • sector_exposure.csv  → Treemap (total_exposure, colour=concentration_tier)")
    print("        • daily_portfolio.csv  → Dual-axis line (new_volume + avg_pd)")
    print("        • dq_trend.csv         → Line chart (fail/warn/pass count over time)")
    return paths


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from src.ingest    import run as ingest_run
    from src.transform import run as transform_run
    from src.dq_checks import run as dq_run
    conn = ingest_run()
    transform_run(conn)
    dq_run(conn)
    run(conn)
