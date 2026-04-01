"""
transform.py
------------
Applies SQL transformations to the raw ingested data to produce
business-ready views.  This is the "data wrangling" step that mirrors
what Risk Data Hub does in production — mapping raw source fields into
cleaner, semantically meaningful structures for downstream analytics.

Key outputs
-----------
- Drops and recreates all views defined in sql/business_views.sql
- Returns a dict of DataFrames (one per view) for use by other modules
"""

import sqlite3
import os
import pandas as pd

VIEWS_PATH = os.path.join(os.path.dirname(__file__), "..", "sql", "business_views.sql")

VIEW_NAMES = [
    "v_risk_tier_summary",
    "v_sector_exposure",
    "v_daily_portfolio",
    "v_dq_health",
]


def _drop_views(conn: sqlite3.Connection) -> None:
    """Drops existing views so the SQL file can recreate them cleanly."""
    for view in VIEW_NAMES:
        conn.execute(f"DROP VIEW IF EXISTS {view}")
    conn.commit()


def create_views(conn: sqlite3.Connection) -> None:
    """Executes the business_views.sql file against the database."""
    _drop_views(conn)
    with open(VIEWS_PATH, "r") as f:
        sql = f.read()
    # Split on semicolons and execute each statement individually
    for statement in sql.split(";"):
        stmt = statement.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


def load_views(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    """
    Reads each view into a Pandas DataFrame.
    Returns a dict keyed by view name.
    Note: v_dq_health will be empty until DQ checks have run.
    """
    results = {}
    for view in VIEW_NAMES:
        try:
            results[view] = pd.read_sql_query(f"SELECT * FROM {view}", conn)
        except Exception as e:
            print(f"  [WARN] Could not read {view}: {e}")
            results[view] = pd.DataFrame()
    return results


def print_summary(views: dict[str, pd.DataFrame]) -> None:
    for name, df in views.items():
        if not df.empty:
            print(f"\n  ── {name} ({len(df)} rows) ──")
            print(df.to_string(index=False, max_rows=5))


def run(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    """Entry point called by main.py."""
    print("\n[TRANSFORM] Building business views...")
    create_views(conn)
    views = load_views(conn)
    # Skip v_dq_health — it's empty before DQ checks run
    available = {k: v for k, v in views.items() if not v.empty and k != "v_dq_health"}
    print(f"[TRANSFORM] Created {len(VIEW_NAMES)} views, "
          f"{len(available)} have data ✓")
    print_summary(available)
    return views


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from src.ingest import run as ingest_run
    conn = ingest_run()
    run(conn)
