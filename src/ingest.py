"""
ingest.py
---------
Generates a realistic synthetic loan/credit risk dataset and loads it into
the SQLite database.  In a real bank setting this module would instead pull
from upstream source systems (data lake, Oracle, etc.).

Key outputs
-----------
- Populates the `raw_loans` table in risk_dq.db
- Intentionally seeds ~5-8 % data quality issues so the DQ checks have
  something to flag (mirrors what you'd see in production risk data).
"""

import sqlite3
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "risk_dq.db")
DDL_PATH = os.path.join(os.path.dirname(__file__), "..", "sql", "create_tables.sql")

LOAN_TYPES    = ["TERM_LOAN", "REVOLVING", "TRADE_FINANCE", "MORTGAGE", "OVERDRAFT"]
RISK_RATINGS  = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D"]
SECTORS       = ["FINANCIAL", "ENERGY", "TECHNOLOGY", "REAL_ESTATE", "HEALTHCARE",
                 "MANUFACTURING", "RETAIL", "TELECOM"]
REGIONS       = ["APAC", "EMEA", "AMERICAS", "SOUTH_ASIA"]
CURRENCIES    = ["USD", "GBP", "EUR", "SGD", "INR"]
STATUSES      = ["ACTIVE", "DEFAULTED", "REPAID", "WATCHLIST"]


def _random_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    delta = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=delta)).strftime("%Y-%m-%d")


def generate_loans(n: int = 2000, seed: int = 42) -> pd.DataFrame:
    """
    Generates n synthetic loan records with realistic risk attributes.
    About 6 % of records have deliberate quality issues injected.
    """
    random.seed(seed)
    np.random.seed(seed)

    records = []
    for _ in range(n):
        rating      = random.choice(RISK_RATINGS)
        # PD increases as rating deteriorates
        rating_idx  = RISK_RATINGS.index(rating)
        pd_base     = 0.001 * (2 ** rating_idx)          # AAA→0.001, D→0.128
        pd_score    = min(round(np.random.normal(pd_base, pd_base * 0.2), 6), 1.0)
        lgd_score   = round(np.random.uniform(0.2, 0.8), 4)
        loan_amt    = round(np.random.lognormal(mean=12, sigma=1.5), 2)  # log-normal $
        ead         = round(loan_amt * np.random.uniform(0.8, 1.2), 2)

        orig_date   = _random_date(730, 30)
        mat_date    = (datetime.strptime(orig_date, "%Y-%m-%d")
                       + timedelta(days=random.randint(365, 3650))).strftime("%Y-%m-%d")

        records.append({
            "loan_id":             str(uuid.uuid4())[:12].upper(),
            "customer_id":         f"CUST{random.randint(1000, 9999)}",
            "loan_amount":         loan_amt,
            "loan_type":           random.choice(LOAN_TYPES),
            "risk_rating":         rating,
            "pd_score":            pd_score,
            "lgd_score":           lgd_score,
            "exposure_at_default": ead,
            "region":              random.choice(REGIONS),
            "origination_date":    orig_date,
            "maturity_date":       mat_date,
            "status":              random.choice(STATUSES),
            "sector":              random.choice(SECTORS),
            "currency":            random.choice(CURRENCIES),
            "loaded_at":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(records)

    # ── Inject data quality issues ─────────────────────────────────────────
    issue_idx = df.sample(frac=0.06, random_state=seed).index

    # Nulls in critical fields
    null_split = np.array_split(issue_idx, 3)
    df.loc[null_split[0], "pd_score"]    = None
    df.loc[null_split[1], "risk_rating"] = None
    df.loc[null_split[2], "loan_amount"] = None

    # Negative loan amounts (invalid)
    neg_idx = df.sample(frac=0.02, random_state=seed + 1).index
    df.loc[neg_idx, "loan_amount"] = df.loc[neg_idx, "loan_amount"] * -1

    # Duplicate loan IDs (simulate source system bug)
    dup_idx = df.sample(n=30, random_state=seed + 2).index
    df.loc[dup_idx, "loan_id"] = df["loan_id"].iloc[0]

    # PD scores > 1 (out-of-range)
    oor_idx = df.sample(frac=0.01, random_state=seed + 3).index
    df.loc[oor_idx, "pd_score"] = np.random.uniform(1.1, 2.0, size=len(oor_idx))

    return df


def init_db(conn: sqlite3.Connection) -> None:
    """Creates tables and views from SQL files if they don't already exist."""
    with open(DDL_PATH, "r") as f:
        conn.executescript(f.read())
    conn.commit()


def load_to_db(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """Truncates raw_loans and reloads with fresh data. Returns row count."""
    conn.execute("DELETE FROM raw_loans")
    df.to_sql("raw_loans", conn, if_exists="append", index=False)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM raw_loans").fetchone()[0]
    return count


def run(n_records: int = 2000) -> sqlite3.Connection:
    """
    Entry point called by main.py.
    Returns an open DB connection for subsequent pipeline steps.
    """
    print(f"[INGEST] Generating {n_records} synthetic loan records...")
    df   = generate_loans(n=n_records)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    rows = load_to_db(df, conn)
    print(f"[INGEST] Loaded {rows} rows into raw_loans ✓")
    return conn


if __name__ == "__main__":
    run()
