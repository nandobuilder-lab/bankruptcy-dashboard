"""
Scraper for US Courts quarterly bankruptcy filings statistics.

Strategy:
  1. Scrape the statistics index page to discover all available quarters.
  2. For each quarter, fetch the release page to find PDF download links.
  3. For each PDF, derive the XLSX counterpart (same path, swap extension)
     and HEAD-check it. All tables have XLSX versions alongside PDFs.
  4. Download XLSX files into raw/[year]/[quarter]/.
  5. Record everything in manifest.json for incremental runs.

Index page (release discovery):
  https://www.uscourts.gov/data-news/reports/statistical-reports/bankruptcy-filings-statistics

Release pages (one per quarter, linked from index):
  https://www.uscourts.gov/data-news/reports/statistical-reports/bankruptcy-filing-statistics/[month]-[year]-quarterly-bankruptcy-filings

Files downloaded (XLSX, one per table per quarter):
  bf_f_MMDD.YYYY.xlsx      Table F  — Filed/Terminated/Pending (12-month)
  bf_f2_MMDD.YYYY.xlsx     Table F-2 — Filings by Chapter (12-month)
  bf_f2.1_MMDD.YYYY.xlsx   Table F-2.1 — Filings by Chapter (1-month)
  bf_f2.3_MMDD.YYYY.xlsx   Table F-2.3 — Filings by Chapter (3-month)
  bf_f5a_MMDD.YYYY.xlsx    Table F-5A — Filings by Chapter, District, and County
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = "https://www.uscourts.gov"
INDEX_URL = (
    f"{BASE_URL}/data-news/reports/statistical-reports/bankruptcy-filings-statistics"
)

# pipeline/src/ -> pipeline/
ROOT_DIR = Path(__file__).parent.parent
RAW_DIR = ROOT_DIR / "raw"
MANIFEST_PATH = ROOT_DIR / "manifest.json"

MONTH_TO_QUARTER = {
    "march": "Q1",
    "june": "Q2",
    "september": "Q3",
    "december": "Q4",
}

RELEASE_PATTERN = re.compile(
    r"/(january|february|march|april|may|june|july|august"
    r"|september|october|november|december)"
    r"-(\d{4})-quarterly-bankruptcy-filings$"
)

TABLE_LABELS = {
    "bf_f_":    "Table F — Cases Filed, Terminated, and Pending (12-month)",
    "bf_f2_":   "Table F-2 — Business/Nonbusiness by Chapter (12-month)",
    "bf_f2.1_": "Table F-2.1 — Business/Nonbusiness by Chapter (1-month)",
    "bf_f2.3_": "Table F-2.3 — Business/Nonbusiness by Chapter (3-month)",
    "bf_f5a_":  "Table F-5A — Filings by Chapter, District, and County",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {"releases": {}}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "BankruptcyResearchPipeline/1.0 (academic research)"
    return s


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def fetch_release_index(session: requests.Session) -> List[dict]:
    """Return a sorted list of quarterly release metadata from the index page."""
    resp = session.get(INDEX_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    seen, releases = set(), []
    for a in soup.find_all("a", href=True):
        match = RELEASE_PATTERN.search(a["href"])
        if not match:
            continue
        month, year = match.group(1), match.group(2)
        quarter = MONTH_TO_QUARTER.get(month)
        if not quarter:
            continue
        release_id = f"{year}_{quarter}"
        if release_id in seen:
            continue
        seen.add(release_id)
        releases.append({
            "release_id":  release_id,
            "year":        int(year),
            "quarter":     quarter,
            "month":       month,
            "release_url": urljoin(BASE_URL, a["href"]),
        })

    return sorted(releases, key=lambda r: (r["year"], r["quarter"]))


# ---------------------------------------------------------------------------
# XLSX link discovery (per release)
# ---------------------------------------------------------------------------

def fetch_xlsx_links(session: requests.Session, release_url: str) -> List[dict]:
    """
    Fetch a quarterly release page, find all PDF download links, and return
    a list of available XLSX counterparts (verified with a HEAD request).
    """
    resp = session.get(release_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    results = []
    seen_filenames: set = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".pdf"):
            continue

        xlsx_href = href[:-4] + ".xlsx"
        xlsx_url = urljoin(BASE_URL, xlsx_href)
        filename = xlsx_url.split("/")[-1]

        if filename in seen_filenames:
            continue
        seen_filenames.add(filename)

        try:
            r = session.head(xlsx_url, timeout=10)
        except requests.RequestException:
            continue

        if r.status_code != 200:
            continue

        label = next(
            (desc for prefix, desc in TABLE_LABELS.items() if filename.startswith(prefix)),
            filename,
        )
        results.append({"filename": filename, "url": xlsx_url, "label": label})

    return results


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(session: requests.Session, url: str, dest: Path) -> bool:
    """Download url to dest. Returns True if newly downloaded, False if already exists."""
    if dest.exists():
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = session.get(url, timeout=120, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=16_384):
            f.write(chunk)
    return True


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(
    years: Optional[List[int]] = None,
    force: bool = False,
    delay: float = 0.5,
) -> dict:
    """
    Download new quarterly XLSX releases from the US Courts website.

    Args:
        years:  Only process releases for these years. None = all years.
        force:  Re-download releases already recorded in the manifest.
        delay:  Seconds between HTTP requests (polite crawling).

    Returns:
        Summary dict with counts of new/skipped releases and files.
    """
    manifest = load_manifest()
    session = _session()
    summary = {
        "new_releases":    0,
        "skipped_releases": 0,
        "new_files":       0,
        "missing_xlsx":    0,
    }

    print("Fetching release index...")
    all_releases = fetch_release_index(session)
    print(f"Found {len(all_releases)} quarterly releases on index page.")

    to_process = [
        r for r in all_releases
        if (years is None or r["year"] in years)
        and (force or r["release_id"] not in manifest["releases"])
    ]
    summary["skipped_releases"] = len(all_releases) - len(to_process)

    if not to_process:
        print("No new releases to download.")
        return summary

    print(f"Processing {len(to_process)} release(s) ({summary['skipped_releases']} already done).\n")

    for release in tqdm(to_process, desc="Releases", unit="release"):
        rid = release["release_id"]
        tqdm.write(f"  [{rid}] Fetching release page...")
        time.sleep(delay)

        xlsx_links = fetch_xlsx_links(session, release["release_url"])

        if not xlsx_links:
            tqdm.write(f"  [{rid}] No XLSX files found — skipping.")
            summary["missing_xlsx"] += 1
            continue

        dest_dir = RAW_DIR / str(release["year"]) / release["quarter"]
        downloaded_files = []

        for link in tqdm(xlsx_links, desc=f"  {rid}", leave=False, unit="file"):
            dest = dest_dir / link["filename"]
            is_new = download_file(session, link["url"], dest)
            if is_new:
                summary["new_files"] += 1
                tqdm.write(f"  [{rid}] + {link['filename']}  ({link['label']})")
            else:
                tqdm.write(f"  [{rid}] = {link['filename']}  (already exists)")
            downloaded_files.append({
                **link,
                "local_path": str(dest.relative_to(ROOT_DIR)),
            })
            time.sleep(delay)

        manifest["releases"][rid] = {**release, "files": downloaded_files}
        save_manifest(manifest)
        summary["new_releases"] += 1

    print(
        f"\nDone. {summary['new_releases']} new release(s), "
        f"{summary['new_files']} new file(s)."
    )
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download US Courts quarterly bankruptcy filing XLSX files."
    )
    parser.add_argument(
        "--years", nargs="+", type=int,
        help="Limit to specific years, e.g. --years 2023 2024 2025"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download even if already recorded in manifest"
    )
    parser.add_argument(
        "--delay", type=float, default=0.5,
        help="Seconds between HTTP requests (default: 0.5)"
    )
    args = parser.parse_args()

    run(years=args.years, force=args.force, delay=args.delay)
