"""
DuckDB schema setup and data loading for the bankruptcy pipeline.

Tables:
  releases      — one row per downloaded quarter
  f_summary     — Table F: filed/terminated/pending by district
  f2_filings    — Table F-2/F-2.1/F-2.3: filings by chapter and nature of debt
  f5a_county    — Table F-5A: filings by county
  fred_series   — FRED series metadata
  fred_observations — FRED time-series data
"""

from pathlib import Path

import duckdb
import pandas as pd

# Points from pipeline/src/ up to evidence-app/, then into sources/bankruptcy/
DB_PATH = Path(__file__).parent.parent.parent / "sources" / "bankruptcy" / "bankruptcy.duckdb"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
    release_id  VARCHAR PRIMARY KEY,
    year        INTEGER,
    quarter     VARCHAR,
    month       VARCHAR,
    period_end  DATE
);

CREATE TABLE IF NOT EXISTS f_summary (
    release_id          VARCHAR,
    period_end          DATE,
    row_type            VARCHAR,   -- 'national', 'circuit', 'district'
    label               VARCHAR,
    prior_year          INTEGER,
    current_year        INTEGER,
    filed_prior         INTEGER,
    filed_current       INTEGER,
    terminated_prior    INTEGER,
    terminated_current  INTEGER,
    pending_prior       INTEGER,
    pending_current     INTEGER,
    PRIMARY KEY (release_id, row_type, label)
);

CREATE TABLE IF NOT EXISTS f2_filings (
    release_id      VARCHAR,
    period_end      DATE,
    period_months   INTEGER,   -- 1, 3, or 12
    row_type        VARCHAR,   -- 'national', 'circuit', 'district'
    label           VARCHAR,
    total_all       INTEGER,
    total_ch7       INTEGER,
    total_ch11      INTEGER,
    total_ch13      INTEGER,
    total_other     INTEGER,
    biz_all         INTEGER,
    biz_ch7         INTEGER,
    biz_ch11        INTEGER,
    biz_ch13        INTEGER,
    biz_other       INTEGER,
    nonbiz_all      INTEGER,
    nonbiz_ch7      INTEGER,
    nonbiz_ch11     INTEGER,
    nonbiz_ch13     INTEGER,
    PRIMARY KEY (release_id, period_end, period_months, row_type, label)
);

CREATE TABLE IF NOT EXISTS f5a_county (
    release_id          VARCHAR,
    period_end          DATE,
    row_type            VARCHAR,   -- 'national', 'circuit', 'district', 'county'
    circuit             VARCHAR,
    district            VARCHAR,
    label               VARCHAR,
    county_name         VARCHAR,
    county_fips         VARCHAR,
    is_out_of_district  BOOLEAN,
    total_all           INTEGER,
    total_ch7           INTEGER,
    total_ch11          INTEGER,
    total_ch13          INTEGER,
    total_other         INTEGER,
    biz_all             INTEGER,
    biz_ch7             INTEGER,
    biz_ch11            INTEGER,
    biz_ch13            INTEGER,
    biz_other           INTEGER,
    nonbiz_all          INTEGER,
    nonbiz_ch7          INTEGER,
    nonbiz_ch11         INTEGER,
    nonbiz_ch13         INTEGER
    -- No PRIMARY KEY: circuit/district/county_fips can be NULL.
    -- Deduplication is handled by delete-then-insert in upsert().
);

CREATE TABLE IF NOT EXISTS fred_series (
    series_id   VARCHAR PRIMARY KEY,
    title       VARCHAR,
    frequency   VARCHAR,
    units       VARCHAR,
    notes       VARCHAR
);

CREATE TABLE IF NOT EXISTS fred_observations (
    series_id   VARCHAR,
    date        DATE,
    value       DOUBLE,
    PRIMARY KEY (series_id, date)
);
"""


def connect(path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open (or create) the DuckDB database and ensure all tables exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(_SCHEMA)
    return con


def upsert(
    con: duckdb.DuckDBPyConnection,
    table: str,
    df: "pd.DataFrame",
    key_cols: list,
) -> int:
    """
    Delete existing rows matching the key columns, then insert the new DataFrame.

    This is a clean replace strategy: safe for idempotent ETL runs.
    Returns the number of rows inserted.
    """
    if df.empty:
        return 0

    key_vals = {col: df[col].iloc[0] for col in key_cols if col in df.columns}
    where_parts = [f"{col} = {_quote(val)}" for col, val in key_vals.items()]
    if where_parts:
        con.execute(f"DELETE FROM {table} WHERE {' AND '.join(where_parts)}")

    con.register("_df", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM _df")
    con.unregister("_df")
    return len(df)


def _quote(val) -> str:
    """Produce a SQL-safe literal for a scalar value."""
    if val is None:
        return "NULL"
    if isinstance(val, str):
        return f"'{val.replace(chr(39), chr(39)*2)}'"
    return str(val)


def table_counts(con: duckdb.DuckDBPyConnection) -> dict:
    """Return row counts for all pipeline tables."""
    tables = ["releases", "f_summary", "f2_filings", "f5a_county",
              "fred_series", "fred_observations"]
    return {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
