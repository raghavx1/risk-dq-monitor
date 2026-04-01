"""
main.py
-------
Orchestrates the full Risk Data Quality Monitor pipeline:

    ingest → transform → dq_checks → report → dashboard_export

Run with:
    python main.py
or with a custom record count:
    python main.py --records 5000
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

from src.ingest           import run as ingest
from src.transform        import run as transform
from src.dq_checks        import run as dq_checks
from src.report           import run as report
from src.dashboard_export import run as export


def main(n_records: int = 2000) -> None:
    start = time.time()

    print("=" * 60)
    print("  RISK DATA QUALITY MONITOR")
    print("  Standard Chartered — Data Hub Pipeline")
    print("=" * 60)

    conn        = ingest(n_records=n_records)
    views       = transform(conn)
    dq_df       = dq_checks(conn)
    transform(conn)   # re-run so v_dq_health populates
    report_path = report(conn, dq_df)
    csv_paths   = export(conn)
    conn.close()

    elapsed = round(time.time() - start, 2)
    print("\n" + "=" * 60)
    print(f"  Pipeline complete in {elapsed}s")
    print(f"  HTML report → {report_path}")
    print(f"  Tableau CSVs → outputs/tableau/")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Risk DQ Monitor Pipeline")
    parser.add_argument("--records", type=int, default=2000)
    args = parser.parse_args()
    main(n_records=args.records)
