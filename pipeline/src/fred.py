"""
FRED integration: fetch macroeconomic series and store in DuckDB.

Reads FRED_API_KEY from the environment (or a .env file).
Fetches series metadata + observations, aggregates monthly series to quarterly,
and upserts into fred_series and fred_observations tables.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv

from database import connect, DB_PATH

# pipeline/src/ -> pipeline/  (look for .env alongside requirements.txt)
ROOT_DIR = Path(__file__).parent.parent
load_dotenv(ROOT_DIR / ".env")

DEFAULT_SERIES: Dict[str, str] = {
    "UNRATE":        "Unemployment Rate",
    "FEDFUNDS":      "Federal Funds Effective Rate",
    "DGS10":         "10-Year Treasury Constant Maturity Rate",
    "CPIAUCSL":      "Consumer Price Index (All Urban Consumers)",
    "DSPIC96":       "Real Disposable Personal Income",
    "TDSP":          "Household Debt Service Ratio",
    "DRCCLACBS":     "Credit Card Delinquency Rate (Banks)",
    "DRSFRMACBS":    "Residential Mortgage Delinquency Rate",
    "BAMLH0A0HYM2":  "ICE BofA US High Yield Index OAS",
    "UMCSENT":       "Univ. of Michigan Consumer Sentiment",
    "GDP":           "Gross Domestic Product",
    "PSAVERT":       "Personal Saving Rate",
}


def _get_api_key() -> str:
    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        raise EnvironmentError(
            "FRED_API_KEY not set. Add it to your environment or to a .env file "
            "in the pipeline directory:\n  FRED_API_KEY=your_key_here"
        )
    return key


def _to_quarterly(series: pd.Series) -> pd.Series:
    """Resample a time series to quarter-end frequency by averaging."""
    return series.resample("QE").mean().dropna()


def fetch_series(
    series_ids: Optional[List[str]] = None,
    start_date: str = "2000-01-01",
    db_path: Path = DB_PATH,
) -> dict:
    """
    Fetch FRED series and load into DuckDB.

    Args:
        series_ids:  List of FRED series IDs. None = use DEFAULT_SERIES.
        start_date:  Earliest observation date to fetch (YYYY-MM-DD).
        db_path:     Path to the DuckDB database.

    Returns:
        Summary dict with series fetched and observation counts.
    """
    try:
        from fredapi import Fred
    except ImportError:
        raise ImportError("fredapi is not installed. Run: pip install fredapi")

    fred = Fred(api_key=_get_api_key())
    con = connect(db_path)
    summary = {"fetched": 0, "observations": 0, "errors": []}

    ids_to_fetch = series_ids or list(DEFAULT_SERIES.keys())
    print(f"Fetching {len(ids_to_fetch)} FRED series (start: {start_date})...\n")

    for sid in ids_to_fetch:
        try:
            info = fred.get_series_info(sid)
            meta_df = pd.DataFrame([{
                "series_id": sid,
                "title":     info.get("title", DEFAULT_SERIES.get(sid, sid)),
                "frequency": info.get("frequency_short", ""),
                "units":     info.get("units_short", ""),
                "notes":     str(info.get("notes", ""))[:500],
            }])
            con.execute("DELETE FROM fred_series WHERE series_id = ?", [sid])
            con.register("_meta", meta_df)
            con.execute("INSERT INTO fred_series SELECT * FROM _meta")
            con.unregister("_meta")

            raw: pd.Series = fred.get_series(sid, observation_start=start_date)
            raw.index = pd.to_datetime(raw.index)
            raw = raw.replace(".", float("nan")).astype(float).dropna()

            freq = info.get("frequency_short", "Q")
            if freq in ("D", "W", "M", "BW"):
                obs = _to_quarterly(raw)
            else:
                obs = raw.copy()
                obs.index = obs.index.to_period("Q").to_timestamp("Q")

            obs_df = pd.DataFrame({
                "series_id": sid,
                "date":      obs.index.date,
                "value":     obs.values,
            })

            con.execute("DELETE FROM fred_observations WHERE series_id = ?", [sid])
            con.register("_obs", obs_df)
            con.execute("INSERT INTO fred_observations SELECT * FROM _obs")
            con.unregister("_obs")

            summary["fetched"] += 1
            summary["observations"] += len(obs_df)
            print(f"  {sid:20s} {len(obs_df):4d} quarters  [{info.get('frequency_short','?')} → Q]  {info.get('title','')[:50]}")

        except Exception as exc:
            summary["errors"].append({"series_id": sid, "error": str(exc)})
            print(f"  {sid:20s} ERROR: {exc}")

    con.close()
    print(f"\nDone. {summary['fetched']} series, {summary['observations']:,} observations.")
    if summary["errors"]:
        print(f"Errors ({len(summary['errors'])}):")
        for e in summary["errors"]:
            print(f"  {e['series_id']}: {e['error']}")
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch FRED macroeconomic series into DuckDB."
    )
    parser.add_argument(
        "--series", nargs="+",
        help="FRED series IDs to fetch. Defaults to the project's standard set."
    )
    parser.add_argument(
        "--start", default="2000-01-01",
        help="Start date for observations (default: 2000-01-01)"
    )
    args = parser.parse_args()

    fetch_series(series_ids=args.series, start_date=args.start)
