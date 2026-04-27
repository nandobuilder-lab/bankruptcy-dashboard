"""
ETL orchestrator: parse downloaded XLSX files and load them into DuckDB.

Reads the manifest to know which files are available, parses each one,
and upserts into the database. Idempotent — safe to re-run.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from parser import parse_file
from database import connect, upsert, table_counts, DB_PATH

# pipeline/src/ -> pipeline/
ROOT_DIR = Path(__file__).parent.parent
MANIFEST_PATH = ROOT_DIR / "manifest.json"

_UPSERT_KEYS = {
    "f_summary":  ["release_id"],
    "f2_filings": ["release_id", "period_months"],
    "f5a_county": ["release_id"],
}


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"manifest.json not found at {MANIFEST_PATH}. Run scraper.py first."
        )
    return json.loads(MANIFEST_PATH.read_text())


def _period_end_from_release(release: dict) -> date:
    quarter_end = {"Q1": (3, 31), "Q2": (6, 30), "Q3": (9, 30), "Q4": (12, 31)}
    m, d = quarter_end[release["quarter"]]
    return date(release["year"], m, d)


def run(
    release_ids: Optional[List[str]] = None,
    force: bool = False,
    db_path: Path = DB_PATH,
) -> dict:
    """
    Parse and load XLSX files into DuckDB.

    Args:
        release_ids: Process only these releases (e.g. ['2024_Q1']). None = all.
        force:       Re-process releases already marked as loaded in the manifest.
        db_path:     Path to the DuckDB database file.

    Returns:
        Summary dict with counts of releases processed and rows inserted per table.
    """
    manifest = load_manifest()
    con = connect(db_path)

    summary: Dict = {"releases_processed": 0, "rows_inserted": {}}

    releases = manifest.get("releases", {})
    to_process = {
        rid: rel for rid, rel in releases.items()
        if (release_ids is None or rid in release_ids)
        and rel.get("files")
    }

    if not to_process:
        print("No releases with downloaded files found.")
        return summary

    print(f"Loading {len(to_process)} release(s) into DuckDB...\n")

    for rid, release in tqdm(to_process.items(), desc="Releases", unit="release"):
        period_end = _period_end_from_release(release)

        rel_df = pd.DataFrame([{
            "release_id": rid,
            "year":       release["year"],
            "quarter":    release["quarter"],
            "month":      release["month"],
            "period_end": period_end,
        }])
        upsert(con, "releases", rel_df, ["release_id"])

        for file_info in tqdm(release["files"], desc=f"  {rid}", leave=False, unit="file"):
            local_path = ROOT_DIR / file_info["local_path"]
            if not local_path.exists():
                tqdm.write(f"  [{rid}] Missing file: {local_path} — skipping")
                continue

            try:
                table_name, df = parse_file(local_path, rid)
            except Exception as exc:
                tqdm.write(f"  [{rid}] Parse error on {local_path.name}: {exc}")
                continue

            if df.empty:
                tqdm.write(f"  [{rid}] {local_path.name}: parsed 0 rows — skipping")
                continue

            key_cols = _UPSERT_KEYS.get(table_name, ["release_id"])
            if table_name == "f2_filings" and "period_months" in df.columns:
                key_cols = ["release_id", "period_months"]

            n = upsert(con, table_name, df, key_cols)
            summary["rows_inserted"][table_name] = (
                summary["rows_inserted"].get(table_name, 0) + n
            )
            tqdm.write(f"  [{rid}] {local_path.name} → {table_name}: {n} rows")

        summary["releases_processed"] += 1

    con.close()

    print(f"\nDone. {summary['releases_processed']} release(s) loaded.")
    print("Rows inserted per table:")
    for t, n in summary["rows_inserted"].items():
        print(f"  {t}: {n:,}")

    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Parse downloaded XLSX files and load into DuckDB."
    )
    parser.add_argument(
        "--releases", nargs="+",
        help="Process only these release IDs, e.g. --releases 2024_Q1 2024_Q4"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-process releases even if already loaded"
    )
    args = parser.parse_args()

    run(release_ids=args.releases, force=args.force)
