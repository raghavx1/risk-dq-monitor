"""
report.py
---------
Generates a professional HTML summary report of the DQ run and portfolio
state.  This is what you'd present to senior stakeholders or attach to a
BCBS 239 governance pack.

Key outputs
-----------
- outputs/dq_report_<timestamp>.html
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")


def _status_badge(status: str) -> str:
    colors = {"PASS": "#27ae60", "WARN": "#f39c12", "FAIL": "#e74c3c"}
    color  = colors.get(status, "#95a5a6")
    return (f'<span style="background:{color};color:white;padding:2px 8px;'
            f'border-radius:4px;font-size:12px;font-weight:bold;">{status}</span>')


def _df_to_html(df: pd.DataFrame, status_col: str = None) -> str:
    """Renders a DataFrame as a styled HTML table."""
    rows = ""
    for _, row in df.iterrows():
        cells = ""
        for col in df.columns:
            val = row[col]
            if col == status_col:
                val = _status_badge(str(val))
            cells += f"<td>{val}</td>"
        rows += f"<tr>{cells}</tr>"

    headers = "".join(f"<th>{c}</th>" for c in df.columns)
    return f"""
    <table>
      <thead><tr>{headers}</tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_report(conn: sqlite3.Connection, dq_df: pd.DataFrame) -> str:
    """Assembles the full HTML report and writes it to disk."""

    # Load business views
    def safe_read(sql):
        try:
            return pd.read_sql_query(sql, conn)
        except Exception:
            return pd.DataFrame()

    risk_summary  = safe_read("SELECT * FROM v_risk_tier_summary")
    sector_exp    = safe_read("SELECT * FROM v_sector_exposure")
    daily_port    = safe_read(
        "SELECT * FROM v_daily_portfolio ORDER BY snapshot_date DESC LIMIT 10"
    )

    # Aggregate DQ stats
    pass_ct  = int((dq_df["status"] == "PASS").sum())
    warn_ct  = int((dq_df["status"] == "WARN").sum())
    fail_ct  = int((dq_df["status"] == "FAIL").sum())
    total_ct = len(dq_df)
    overall  = "FAIL" if fail_ct > 0 else ("WARN" if warn_ct > 0 else "PASS")
    ov_color = {"PASS": "#27ae60", "WARN": "#f39c12", "FAIL": "#e74c3c"}[overall]

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Risk Data Quality Report — {ts}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background:#f5f6fa;
          color:#2c3e50; padding:32px; }}
  h1   {{ font-size:26px; margin-bottom:4px; }}
  h2   {{ font-size:18px; margin:28px 0 10px; border-bottom:2px solid #3498db;
          padding-bottom:6px; color:#2980b9; }}
  p    {{ font-size:13px; color:#7f8c8d; margin-bottom:20px; }}
  .meta {{ font-size:12px; color:#95a5a6; }}
  .kpi-row  {{ display:flex; gap:16px; margin-bottom:24px; flex-wrap:wrap; }}
  .kpi      {{ background:white; border-radius:8px; padding:18px 24px;
               flex:1; min-width:140px; box-shadow:0 1px 4px rgba(0,0,0,.08); }}
  .kpi .val {{ font-size:32px; font-weight:700; }}
  .kpi .lbl {{ font-size:12px; color:#95a5a6; margin-top:4px; }}
  .overall  {{ background:{ov_color}; color:white; border-radius:8px;
               padding:14px 24px; display:inline-block; font-size:18px;
               font-weight:bold; margin-bottom:24px; }}
  table     {{ width:100%; border-collapse:collapse; background:white;
               border-radius:8px; overflow:hidden;
               box-shadow:0 1px 4px rgba(0,0,0,.08); margin-bottom:24px; }}
  th        {{ background:#2980b9; color:white; padding:10px 12px;
               text-align:left; font-size:13px; }}
  td        {{ padding:9px 12px; font-size:13px; border-bottom:1px solid #ecf0f1; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover td {{ background:#f8f9fa; }}
  footer    {{ text-align:center; font-size:11px; color:#bdc3c7; margin-top:32px; }}
</style>
</head>
<body>

<h1>📊 Risk Data Quality Monitor</h1>
<p>Data Hub — Automated DQ Report</p>
<div class="meta">Generated: {ts} &nbsp;|&nbsp; Table: raw_loans</div>

<h2>Overall DQ Status</h2>
<div class="overall">Overall: {overall}</div>

<div class="kpi-row">
  <div class="kpi"><div class="val" style="color:#27ae60">{pass_ct}</div>
    <div class="lbl">Checks Passed</div></div>
  <div class="kpi"><div class="val" style="color:#f39c12">{warn_ct}</div>
    <div class="lbl">Warnings</div></div>
  <div class="kpi"><div class="val" style="color:#e74c3c">{fail_ct}</div>
    <div class="lbl">Failures</div></div>
  <div class="kpi"><div class="val">{total_ct}</div>
    <div class="lbl">Total Checks</div></div>
</div>

<h2>DQ Check Results</h2>
{_df_to_html(
    dq_df[["check_name","column_name","status","records_checked",
           "records_failed","failure_rate","details"]],
    status_col="status"
)}

<h2>Portfolio — Risk Tier Summary</h2>
{_df_to_html(risk_summary) if not risk_summary.empty else "<p>No data.</p>"}

<h2>Portfolio — Sector Concentration</h2>
{_df_to_html(sector_exp) if not sector_exp.empty else "<p>No data.</p>"}

<h2>Recent Daily Portfolio Snapshot (latest 10 days)</h2>
{_df_to_html(daily_port) if not daily_port.empty else "<p>No data.</p>"}

<footer>Risk Data Quality Monitor &copy; {datetime.now().year} — For internal use only</footer>
</body>
</html>"""

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename  = f"dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    filepath  = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    return filepath


def run(conn: sqlite3.Connection, dq_df: pd.DataFrame) -> str:
    """Entry point called by main.py."""
    print("\n[REPORT] Generating HTML report...")
    path = build_report(conn, dq_df)
    print(f"[REPORT] Saved → {path} ✓")
    return path


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from src.ingest    import run as ingest_run
    from src.transform import run as transform_run
    from src.dq_checks import run as dq_run
    conn   = ingest_run()
    transform_run(conn)
    dq_df  = dq_run(conn)
    run(conn, dq_df)
