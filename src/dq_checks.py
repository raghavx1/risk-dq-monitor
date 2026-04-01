"""
dq_checks.py
------------
The heart of the pipeline.  Runs a suite of data quality checks against
raw_loans and writes detailed results to the dq_results table.

Checks implemented
------------------
1.  Null completeness   — critical fields must not be null
2.  Negative values     — loan_amount and ead must be positive
3.  Range validation    — pd_score ∈ [0,1], lgd_score ∈ [0,1]
4.  Duplicate IDs       — loan_id must be unique
5.  Referential values  — risk_rating must be a known enum value
6.  Date logic          — maturity_date must be after origination_date
7.  Cross-field check   — status='DEFAULTED' should imply pd_score > 0.05
8.  Currency whitelist  — currency must be an approved CCY code

Each check produces a structured result that is persisted to dq_results
so you can track quality over time (essential for BCBS 239 compliance).
"""

import sqlite3
import uuid
import pandas as pd
from datetime import datetime
from typing import Callable


# ── Constants ──────────────────────────────────────────────────────────────

VALID_RISK_RATINGS = {"AAA", "AA", "A", "BBB", "BB", "B", "CCC", "D"}
VALID_CURRENCIES   = {"USD", "GBP", "EUR", "SGD", "INR", "AUD", "JPY", "HKD"}
CRITICAL_FIELDS    = ["loan_id", "customer_id", "loan_amount", "risk_rating",
                      "pd_score", "lgd_score", "exposure_at_default",
                      "origination_date", "status"]

FAIL_THRESHOLD = 0.05   # >5 % failure rate → FAIL
WARN_THRESHOLD = 0.01   # 1–5 % → WARN


def _status_from_rate(rate: float) -> str:
    if rate == 0:
        return "PASS"
    elif rate < WARN_THRESHOLD:
        return "PASS"
    elif rate < FAIL_THRESHOLD:
        return "WARN"
    return "FAIL"


def _make_result(run_id, check_name, column, total, failed, details="") -> dict:
    rate = failed / total if total > 0 else 0
    return {
        "run_id":           run_id,
        "check_name":       check_name,
        "table_name":       "raw_loans",
        "column_name":      column,
        "status":           _status_from_rate(rate),
        "records_checked":  total,
        "records_failed":   failed,
        "failure_rate":     round(rate, 6),
        "details":          details,
        "checked_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ── Individual Checks ──────────────────────────────────────────────────────

def check_nulls(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 1: Critical fields must not contain nulls."""
    results = []
    for col in CRITICAL_FIELDS:
        if col not in df.columns:
            continue
        n_null = int(df[col].isna().sum())
        results.append(_make_result(
            run_id, "NULL_COMPLETENESS", col, len(df), n_null,
            details=f"{n_null} null values found" if n_null else "No nulls detected"
        ))
    return results


def check_negative_values(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 2: Monetary fields must be positive."""
    results = []
    for col in ["loan_amount", "exposure_at_default"]:
        if col not in df.columns:
            continue
        mask   = df[col].notna() & (df[col] < 0)
        failed = int(mask.sum())
        results.append(_make_result(
            run_id, "NEGATIVE_VALUE", col, len(df), failed,
            details=f"{failed} negative amounts detected" if failed else "All values positive"
        ))
    return results


def check_score_ranges(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 3: PD and LGD scores must lie in [0, 1]."""
    results = []
    for col in ["pd_score", "lgd_score"]:
        if col not in df.columns:
            continue
        mask   = df[col].notna() & ((df[col] < 0) | (df[col] > 1))
        failed = int(mask.sum())
        results.append(_make_result(
            run_id, "RANGE_VALIDATION", col, len(df), failed,
            details=f"{failed} out-of-range scores [0,1]" if failed else "All scores in range"
        ))
    return results


def check_duplicate_ids(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 4: loan_id must be unique."""
    total   = len(df)
    dupes   = int(df["loan_id"].duplicated().sum())
    return [_make_result(
        run_id, "DUPLICATE_ID", "loan_id", total, dupes,
        details=f"{dupes} duplicate loan_ids" if dupes else "All IDs unique"
    )]


def check_referential_values(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 5: risk_rating must be a known enum value."""
    mask   = df["risk_rating"].notna() & ~df["risk_rating"].isin(VALID_RISK_RATINGS)
    failed = int(mask.sum())
    return [_make_result(
        run_id, "REFERENTIAL_INTEGRITY", "risk_rating", len(df), failed,
        details=(f"{failed} unknown risk ratings: "
                 f"{df.loc[mask,'risk_rating'].unique().tolist()}")
                if failed else "All risk ratings valid"
    )]


def check_date_logic(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 6: maturity_date must be strictly after origination_date."""
    orig = pd.to_datetime(df["origination_date"], errors="coerce")
    mat  = pd.to_datetime(df["maturity_date"],    errors="coerce")
    mask = (orig.notna() & mat.notna()) & (mat <= orig)
    failed = int(mask.sum())
    return [_make_result(
        run_id, "DATE_LOGIC", "maturity_date", len(df), failed,
        details=f"{failed} records where maturity <= origination" if failed
                else "All date pairs valid"
    )]


def check_cross_field_default(df: pd.DataFrame, run_id: str) -> list[dict]:
    """
    Check 7: Defaulted loans should have pd_score > 0.05.
    Defaulted loans with very low PD suggest stale/incorrect scoring.
    """
    defaulted = df[df["status"] == "DEFAULTED"]
    if defaulted.empty:
        return []
    mask   = defaulted["pd_score"].notna() & (defaulted["pd_score"] < 0.05)
    failed = int(mask.sum())
    return [_make_result(
        run_id, "CROSS_FIELD_DEFAULT_PD", "pd_score",
        len(defaulted), failed,
        details=(f"{failed} defaulted loans with PD < 0.05 (possible stale scores)")
                if failed else "Default/PD cross-field logic passes"
    )]


def check_currency_whitelist(df: pd.DataFrame, run_id: str) -> list[dict]:
    """Check 8: currency must be from the approved CCY list."""
    mask   = df["currency"].notna() & ~df["currency"].isin(VALID_CURRENCIES)
    failed = int(mask.sum())
    return [_make_result(
        run_id, "CURRENCY_WHITELIST", "currency", len(df), failed,
        details=(f"{failed} unapproved currencies: "
                 f"{df.loc[mask,'currency'].unique().tolist()}")
                if failed else "All currencies approved"
    )]


# ── Orchestrator ───────────────────────────────────────────────────────────

ALL_CHECKS: list[Callable] = [
    check_nulls,
    check_negative_values,
    check_score_ranges,
    check_duplicate_ids,
    check_referential_values,
    check_date_logic,
    check_cross_field_default,
    check_currency_whitelist,
]


def run(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Entry point called by main.py.
    Runs all checks, persists results to dq_results, returns summary DataFrame.
    """
    print("\n[DQ CHECKS] Running data quality suite...")
    run_id = str(uuid.uuid4())[:8].upper()

    df_raw   = pd.read_sql_query("SELECT * FROM raw_loans", conn)
    all_results: list[dict] = []

    for check_fn in ALL_CHECKS:
        results = check_fn(df_raw, run_id)
        all_results.extend(results)

    df_results = pd.DataFrame(all_results)
    df_results.to_sql("dq_results", conn, if_exists="append", index=False)
    conn.commit()

    # Print a compact summary
    summary = (df_results.groupby("status")["check_name"]
               .count()
               .rename("count")
               .reset_index())
    print(f"[DQ CHECKS] Run ID: {run_id} | "
          f"{len(all_results)} checks across {len(ALL_CHECKS)} rules")
    for _, row in summary.iterrows():
        icon = "✓" if row["status"] == "PASS" else ("⚠" if row["status"] == "WARN" else "✗")
        print(f"  {icon} {row['status']}: {row['count']}")

    # Highlight failures
    failures = df_results[df_results["status"].isin(["FAIL", "WARN"])]
    if not failures.empty:
        print("\n  Issues detected:")
        for _, row in failures.iterrows():
            print(f"    [{row['status']}] {row['check_name']} on {row['column_name']}"
                  f" — {row['details']}")

    return df_results


if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from src.ingest import run as ingest_run
    conn = ingest_run()
    run(conn)
