# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a743e7da-a128-4060-a81f-4d6844a04abd",
# META       "default_lakehouse_name": "pick_a_part",
# META       "default_lakehouse_workspace_id": "0cd3d9a9-0414-44eb-91ad-5516f97df911",
# META       "known_lakehouses": [
# META         {
# META           "id": "a743e7da-a128-4060-a81f-4d6844a04abd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import polars as pl
import io

# Create Polars DataFrame
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
})


file_path = "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/polars_sample.csv"

# Save into Lakehouse
notebookutils.fs.put(file_path, buffer.getvalue(), overwrite=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# --- Fabric / OneLake unified JSON per vehicle (snake_case attributes) ---
import pandas as pd
import requests, re, json
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ========= CONFIG =========
vehicle_url = "https://www.pickapart.co.nz/eziparts/Display_Vehicle.asp?PriceListID=0&VehicleID=214439&$Location=112105099107097112097114116099111110122&LocationID=6&VehicleDesc=Nissan%20E51%2006/02-05/10"

# Lakehouse folder for vehicle JSONs
BASE = "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/vehicle"
# ==========================

def get_vehicle_id(url: str) -> str:
    q = parse_qs(urlparse(url).query)
    return str(q.get("VehicleID", [""])[0])

def pick_details_table(tables):
    for t in tables:
        if t.shape[1] == 2 and 2 <= len(t) <= 50:
            left = t.iloc[:, 0].astype(str).str.len().median()
            right = t.iloc[:, 1].astype(str).str.len().median()
            if left < right:
                return t
    return None

def extract_sold_parts(soup: BeautifulSoup, tables):
    sold = []
    heading = soup.find(lambda tag: tag.name in ["h1","h2","h3","h4","h5","h6","strong","b"]
                        and re.search(r"\bsold\b", tag.get_text(strip=True), re.I))
    if heading:
        next_tbl = heading.find_next("table")
        if next_tbl:
            try:
                df = pd.read_html(str(next_tbl))[0]
                for col in df.columns:
                    parts = df[col].dropna().astype(str)
                    parts = parts[~parts.str.match(r"^\s*(Part|Qty|Price|Status|Sold)\s*$", case=False)]
                    sold.extend(parts.tolist())
            except Exception:
                pass
        for ul in heading.find_all_next(["ul","ol"], limit=2):
            for li in ul.find_all("li"):
                txt = li.get_text(" ", strip=True)
                if txt:
                    sold.append(txt)

    for t in tables:
        df = t.copy()
        df.columns = [str(c) for c in df.columns]
        likely_cols = [c for c in df.columns if re.search(r"\bpart\b", c, re.I)]
        if likely_cols:
            for col in likely_cols:
                parts = df[col].dropna().astype(str)
                parts = parts[parts.str.len() >= 2]
                sold.extend(parts.tolist())
        elif df.shape[1] == 1:
            parts = df.iloc[:, 0].dropna().astype(str)
            parts = parts[parts.str.len() >= 2]
            sold.extend(parts.tolist())

    seen, deduped = set(), []
    for p in [s.strip() for s in sold if s.strip()]:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped

def extract_vehicle_details_fallback(text: str) -> dict:
    """
    Extract vehicle details when no neat details table is available.
    Normalises attribute names to snake_case.
    """
    raw = {}

    def grab(*patterns, flags=re.I):
        for pat in patterns:
            m = re.search(pat, text, flags)
            if m:
                for g in m.groups()[::-1]:
                    if g:
                        return g.strip()
        return None

    raw["vehicle_ref"]   = grab(r"Vehicle\s*Ref[:\s]*([^\n\r]+)")
    raw["acq"]           = grab(r"\bACQ[:\s]*([A-Z0-9]+)\b", r"\bACQ\s*([0-9]+)\b", r"\b(ACQ[0-9]+)\b")
    raw["yard_location"] = grab(r"Yard\s*location[:\s]*([^\n\r]+)")
    raw["make_model"]    = grab(r"Make/Model[:\s]*([^\n\r]+)")
    raw["engine"]        = grab(r"Engine[:\s]*([^\n\r]+)", r"\bEngine\s*([0-9.]+[A-Za-z]*)")
    raw["transmission"]  = grab(r"Transmission[:\s]*([^\n\r]+)", r"\bTransmission\s*([A-Za-z/]+)")
    raw["year"]          = grab(r"Year[:\s]*([0-9]{4})", r"\bYear\s*([0-9]{4})")
    raw["odometer"]      = grab(r"Odometer[:\s]*([0-9,]+)")
    raw["date_in_stock"] = grab(r"Date\s*In\s*Stock[:\s]*([^\n\r]+)")
    raw["description"]   = grab(r"Description[:\s]*([^\n\r]*)")

    return {k: v for k, v in raw.items() if v}

def normalise_keys(d: dict) -> dict:
    """
    Convert any arbitrary keys (from HTML tables) to snake_case consistently.
    """
    out = {}
    for k, v in d.items():
        key = re.sub(r"[^a-z0-9]+", "_", k.strip().lower()).strip("_")
        out[key] = v
    return out

# --------- Fetch & Parse ----------
resp = requests.get(vehicle_url, timeout=30)
resp.raise_for_status()
html = resp.text
soup = BeautifulSoup(html, "lxml")
page_text = soup.get_text(separator="\n", strip=True)

try:
    tables = pd.read_html(html)
except ValueError:
    tables = []

details = {}
dtbl = pick_details_table(tables)
if dtbl is not None:
    dtbl.columns = ["field", "value"]
    details = dict(zip(dtbl["field"].astype(str).str.strip(),
                       dtbl["value"].astype(str).str.strip()))
    details = normalise_keys(details)

# Merge fallback
fallback = extract_vehicle_details_fallback(page_text)
details.update(fallback)

# Always include source_url
details["source_url"] = vehicle_url

# Sold parts
sold_parts = extract_sold_parts(soup, tables)

# --------- Prepare unified JSON ----------
vehicle_id = get_vehicle_id(vehicle_url)
ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

vehicle_filename = f"vehicle_{vehicle_id}_{ts}.json"
vehicle_path = f"{BASE}/{vehicle_filename}"

output = {
    "vehicle_id": vehicle_id,
    "details": details,
    "sold_parts": sold_parts,
    "scraped_at": ts
}

vehicle_json = json.dumps(output, ensure_ascii=False, indent=2)

# --------- Write to OneLake ----------
notebookutils.fs.put(vehicle_path, vehicle_json, overwrite=True)

print("Wrote:", vehicle_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Crawl Pick-a-Part -> scrape vehicles -> write unified JSONs (snake_case) to OneLake ---
# Each run writes into a distinct subfolder: run_YYYY_MM_DD_HH_MM_SS
import pandas as pd
import requests, re, json, time, random
from urllib.parse import urlparse, parse_qs, urljoin
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from typing import Dict, List, Optional

# ========= CONFIG =========
HOMEPAGE = "https://www.pickapart.co.nz/"
DEFAULT_SITES: Dict[str, str] = {
    "Avondale": "https://www.pickapart.co.nz/Avondale-Stock",
    "Christchurch": "https://www.pickapart.co.nz/Current-Stock-Christchurch",
    "Mangere": "https://www.pickapart.co.nz/Mangere-Stock",
    "New Plymouth": "https://www.pickapart.co.nz/New-Plymouth-Stock",
    "Takanini": "https://www.pickapart.co.nz/Takanini-Stock",
    "Tauranga": "https://www.pickapart.co.nz/Tauranga-Stock",
    "Wellington": "https://www.pickapart.co.nz/Current-Stock-Wellington",
}

# Lakehouse base folder for per-vehicle JSONs
VEHICLE_OUT_BASE = (
    "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/"
    "a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/vehicle"
)

# Generate a unique run folder (UTC)
RUN_ID = datetime.now(timezone.utc).strftime("run_%Y_%m_%d_%H_%M_%S")
VEHICLE_OUT_BASE_RUN = f"{VEHICLE_OUT_BASE}/{RUN_ID}"

# Crawl controls
RUN_DRY = False                 # Set to False to actually write to OneLake
SITE_LIMIT: Optional[int] = None              # e.g. 2 to limit sites during testing
SERIES_LIMIT_PER_SITE: Optional[int] = None   # e.g. 5 to limit series pages per site
VEHICLE_LIMIT_PER_SERIES: Optional[int] = None  # e.g. 3 to limit vehicles per series

REQ_TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN = (0.1, 0.3)     # polite small delay between requests
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; pickapart-crawler/1.0)"}
# ==========================


# ---------- Utilities ----------
def sleep_a_bit() -> None:
    """Sleep for a small random interval to avoid hammering the site."""
    lo, hi = SLEEP_BETWEEN
    time.sleep(random.uniform(lo, hi))


def fetch(url: str, retries: int = RETRIES) -> requests.Response:
    """GET a URL with basic retry logic and a polite delay between attempts."""
    for i in range(retries):
        try:
            resp = requests.get(url, timeout=REQ_TIMEOUT, headers=HEADERS)
            resp.raise_for_status()
            return resp
        except Exception:
            if i == retries - 1:
                raise
            sleep_a_bit()


def snake(s: str) -> str:
    """Convert a string to lowercase snake_case, stripping non-alphanumerics."""
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")


def get_vehicle_id(url: str) -> str:
    """Extract VehicleID query param from a Display_Vehicle.asp URL."""
    q = parse_qs(urlparse(url).query)
    return str(q.get("VehicleID", [""])[0])


# ---------- Vehicle page parsing ----------
def pick_details_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Heuristic to find a 2-column details table where the left column looks like labels.
    Returns the table or None.
    """
    for t in tables:
        if t.shape[1] == 2 and 2 <= len(t) <= 50:
            left = t.iloc[:, 0].astype(str).str.len().median()
            right = t.iloc[:, 1].astype(str).str.len().median()
            if left < right:
                return t
    return None


def normalise_keys(d: Dict[str, str]) -> Dict[str, str]:
    """Normalise dictionary keys to snake_case while leaving values untouched."""
    return {snake(k): v for k, v in d.items()}


def extract_vehicle_details_fallback(text: str) -> Dict[str, str]:
    """
    Extract vehicle details from the raw page text when no neat details table is available.
    Normalises attribute names to snake_case.
    """
    raw: Dict[str, Optional[str]] = {}

    def grab(*patterns, flags=re.I) -> Optional[str]:
        for pat in patterns:
            m = re.search(pat, text, flags)
            if m:
                for g in m.groups()[::-1]:
                    if g:
                        return g.strip()
        return None

    raw["vehicle_ref"]   = grab(r"Vehicle\s*Ref[:\s]*([^\n\r]+)")
    raw["acq"]           = grab(r"\bACQ[:\s]*([A-Z0-9]+)\b", r"\bACQ\s*([0-9]+)\b", r"\b(ACQ[0-9]+)\b")
    raw["yard_location"] = grab(r"Yard\s*location[:\s]*([^\n\r]+)")
    raw["make_model"]    = grab(r"Make/Model[:\s]*([^\n\r]+)")
    raw["engine"]        = grab(r"Engine[:\s]*([^\n\r]+)", r"\bEngine\s*([0-9.]+[A-Za-z]*)")
    raw["transmission"]  = grab(r"Transmission[:\s]*([^\n\r]+)", r"\bTransmission\s*([A-Za-z/]+)")
    raw["year"]          = grab(r"Year[:\s]*([0-9]{4})", r"\bYear\s*([0-9]{4})")
    raw["odometer"]      = grab(r"Odometer[:\s]*([0-9,]+)")
    raw["date_in_stock"] = grab(r"Date\s*In\s*Stock[:\s]*([^\n\r]+)")
    raw["description"]   = grab(r"Description[:\s]*([^\n\r]*)")

    return {k: v for k, v in raw.items() if v}


def extract_sold_parts(soup: BeautifulSoup, tables: List[pd.DataFrame]) -> List[str]:
    """
    Extract a deduplicated list of already sold parts.
    Looks for a heading containing 'sold', then scans nearby tables and lists.
    Also heuristically scans all tables for a 'part' column or single-column lists.
    """
    sold: List[str] = []

    # Try a heading with "sold"
    heading = soup.find(
        lambda tag: tag.name in ["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b"]
        and re.search(r"\bsold\b", tag.get_text(strip=True), re.I)
    )
    if heading:
        next_tbl = heading.find_next("table")
        if next_tbl:
            try:
                df = pd.read_html(str(next_tbl))[0]
                for col in df.columns:
                    parts = df[col].dropna().astype(str)
                    parts = parts[~parts.str.match(r"^\s*(Part|Qty|Price|Status|Sold)\s*$", case=False)]
                    sold.extend(parts.tolist())
            except Exception:
                pass
        for ul in heading.find_all_next(["ul", "ol"], limit=2):
            for li in ul.find_all("li"):
                txt = li.get_text(" ", strip=True)
                if txt:
                    sold.append(txt)

    # Heuristic scan of all tables
    for t in tables:
        df = t.copy()
        df.columns = [str(c) for c in df.columns]
        likely_cols = [c for c in df.columns if re.search(r"\bpart\b", c, re.I)]
        if likely_cols:
            for col in likely_cols:
                parts = df[col].dropna().astype(str)
                parts = parts[parts.str.len() >= 2]
                sold.extend(parts.tolist())
        elif df.shape[1] == 1:
            parts = df.iloc[:, 0].dropna().astype(str)
            parts = parts[parts.str.len() >= 2]
            sold.extend(parts.tolist())

    # De-duplicate while preserving order
    seen, deduped = set(), []
    for p in [s.strip() for s in sold if s.strip()]:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped


def scrape_vehicle(vehicle_url: str, site_name: Optional[str] = None, series_url: Optional[str] = None) -> Dict:
    """
    Scrape a single Display_Vehicle.asp page.
    Returns a dict with:
      - vehicle_id: str
      - site: str (optional)
      - series_url: str (optional)
      - details: dict of snake_case attributes including source_url
      - sold_parts: list[str]
      - scraped_at: UTC timestamp string YYYYMMDDTHHMMSSZ
    """
    resp = fetch(vehicle_url)
    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(separator="\n", strip=True)

    try:
        tables = pd.read_html(html)
    except ValueError:
        tables = []

    details: Dict[str, str] = {}
    dtbl = pick_details_table(tables)
    if dtbl is not None:
        dtbl.columns = ["field", "value"]
        details = dict(
            zip(dtbl["field"].astype(str).str.strip(), dtbl["value"].astype(str).str.strip())
        )
        details = normalise_keys(details)

    fallback = extract_vehicle_details_fallback(page_text)
    details.update(fallback)
    details["source_url"] = vehicle_url

    sold_parts = extract_sold_parts(soup, tables)

    vehicle_id = get_vehicle_id(vehicle_url)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output = {
        "vehicle_id": vehicle_id,
        "site": site_name if site_name else None,
        "series_url": series_url if series_url else None,
        "details": details,
        "sold_parts": sold_parts,
        "scraped_at": ts,
        "run_id": RUN_ID,
    }
    # Drop Nones for cleanliness
    return {k: v for k, v in output.items() if v is not None}


# ---------- Site discovery & traversal ----------
def discover_site_stock_links() -> Dict[str, str]:
    """
    Discover site stock URLs from the homepage.
    Falls back to DEFAULT_SITES for completeness.
    Returns a dict: {site_name: site_stock_url}
    """
    try:
        resp = fetch(HOMEPAGE)
        soup = BeautifulSoup(resp.text, "lxml")
        links: Dict[str, str] = {}
        for a in soup.find_all("a", href=True):
            txt = a.get_text(" ", strip=True)
            href = urljoin(HOMEPAGE, a["href"])
            if re.search(r"(Stock|Current-Stock)", href, re.I):
                name = txt or urlparse(href).path.split("/")[-1].replace("-Stock", "").replace("Current-Stock-", "")
                name = name.replace("-", " ").strip()
                if any(n.lower() in name.lower() for n in DEFAULT_SITES.keys()):
                    canonical = next((n for n in DEFAULT_SITES if n.lower() in name.lower()), name.title())
                    links[canonical] = href
        merged = DEFAULT_SITES.copy()
        merged.update(links)
        return merged
    except Exception:
        return DEFAULT_SITES.copy()


def extract_series_links(site_stock_url: str) -> List[str]:
    """
    From a site stock page (e.g., Takanini-Stock), collect all links to Display_Vehicles.asp.
    Returns a sorted list of absolute URLs.
    """
    resp = fetch(site_stock_url)
    soup = BeautifulSoup(resp.text, "lxml")
    series_urls: set = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(site_stock_url, a["href"])
        if "Display_Vehicles.asp" in href:
            series_urls.add(href)
    return sorted(series_urls)


def extract_vehicle_links(series_url: str) -> List[str]:
    """
    From a Display_Vehicles.asp page, collect all links to Display_Vehicle.asp.
    Returns a sorted list of absolute URLs.
    """
    resp = fetch(series_url)
    soup = BeautifulSoup(resp.text, "lxml")
    vehicle_urls: set = set()

    # Anchor tags
    for a in soup.find_all("a", href=True):
        href = urljoin(series_url, a["href"])
        if "Display_Vehicle.asp" in href:
            vehicle_urls.add(href)

    # Tables that may embed links in HTML
    try:
        dfs = pd.read_html(resp.text)
        for df in dfs:
            for col in df.columns:
                for val in df[col].astype(str).tolist():
                    for m in re.finditer(r'href=["\']([^"\']+Display_Vehicle\.asp[^"\']*)', val, re.I):
                        vehicle_urls.add(urljoin(series_url, m.group(1)))
    except ValueError:
        pass

    return sorted(vehicle_urls)


# ---------- Writer ----------
def write_vehicle_json_to_onelake(data: Dict) -> None:
    """
    Write one vehicle JSON into the current run folder.
    File name: vehicle_<VehicleID>_<UTC>.json
    """
    vehicle_id = data.get("vehicle_id", "unknown")
    ts = data.get("scraped_at", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    vehicle_filename = f"vehicle_{vehicle_id}_{ts}.json"
    vehicle_path = f"{VEHICLE_OUT_BASE_RUN}/{vehicle_filename}"
    payload = json.dumps(data, ensure_ascii=False, indent=2)

    if RUN_DRY:
        print("[dry-run] would write:", vehicle_path)
    else:
        # notebookutils.fs.put creates intermediate folders if needed
        notebookutils.fs.put(vehicle_path, payload, overwrite=True)
        print("wrote:", vehicle_path)


# ---------- Driver ----------
def crawl_all_pickapart() -> None:
    """
    Crawl all Pick-a-Part sites:
      1) Discover site stock pages
      2) For each site, extract all series pages (Display_Vehicles.asp)
      3) For each series, extract all vehicle pages (Display_Vehicle.asp)
      4) Scrape vehicle pages and write unified JSON into run_<timestamp> folder
    Respects RUN_DRY, SITE_LIMIT, SERIES_LIMIT_PER_SITE, VEHICLE_LIMIT_PER_SERIES.
    """
    print(f"Run folder: {VEHICLE_OUT_BASE_RUN}")
    site_links = discover_site_stock_links()
    print(f"Discovered {len(site_links)} site stock pages.")

    for i, (site_name, site_url) in enumerate(site_links.items(), start=1):
        if SITE_LIMIT and i > SITE_LIMIT:
            print(f"[limit] stopping after {SITE_LIMIT} sites")
            break
        print(f"\n=== Site [{i}/{len(site_links)}]: {site_name} -> {site_url}")
        sleep_a_bit()

        series_urls = extract_series_links(site_url)
        print(f"  found series pages: {len(series_urls)}")
        if SERIES_LIMIT_PER_SITE:
            series_urls = series_urls[:SERIES_LIMIT_PER_SITE]
            print(f"  applying series limit -> {len(series_urls)}")

        seen_vehicle_urls: set = set()
        for j, series_url in enumerate(series_urls, start=1):
            print(f"  - Series [{j}/{len(series_urls)}]: {series_url}")
            sleep_a_bit()

            v_urls = extract_vehicle_links(series_url)
            print(f"      vehicles found: {len(v_urls)}")
            if VEHICLE_LIMIT_PER_SERIES:
                v_urls = v_urls[:VEHICLE_LIMIT_PER_SERIES]
                print(f"      applying vehicle limit -> {len(v_urls)}")

            for k, vurl in enumerate(v_urls, start=1):
                if vurl in seen_vehicle_urls:
                    continue
                seen_vehicle_urls.add(vurl)
                try:
                    sleep_a_bit()
                    data = scrape_vehicle(vurl, site_name=site_name, series_url=series_url)
                    write_vehicle_json_to_onelake(data)
                except Exception as e:
                    print(f"      ! error scraping vehicle [{k}/{len(v_urls)}]: {vurl}\n        {e}")


# ---------- Main ----------
if __name__ == "__main__":
    """
    Entry point when running this cell as a script.
    Adjust CONFIG above, then run.
    """
    crawl_all_pickapart()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Pick-a-Part crawler: auto-discover sites, parallel scrape, JSON per vehicle into run folder ---
# Output path:
#   .../pickapart/raw/vehicle/run_YYYY_MM_DD_HH_MM_SS/vehicle_<VehicleID>_<UTC>.json
import pandas as pd
import requests, re, json, time, random, threading, concurrent.futures
from urllib.parse import urlparse, parse_qs, urljoin
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple

# ========= CONFIG =========
HOMEPAGE = "https://www.pickapart.co.nz/"

# Lakehouse base folder for per-vehicle JSONs
VEHICLE_OUT_BASE = (
    "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/"
    "a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/vehicle"
)

# Generate a unique run folder (UTC)
RUN_ID = datetime.now(timezone.utc).strftime("run_%Y_%m_%d_%H_%M_%S")
VEHICLE_OUT_BASE_RUN = f"{VEHICLE_OUT_BASE}/{RUN_ID}"

# Crawl controls
RUN_DRY = False                                 # Set to False to actually write to OneLake
SITE_LIMIT: Optional[int] = None               # e.g. 2 for testing
SERIES_LIMIT_PER_SITE: Optional[int] = None    # e.g. 5 for testing
VEHICLE_LIMIT_PER_SERIES: Optional[int] = None # e.g. 5 for testing

# Parallelism + HTTP politeness
MAX_WORKERS = 4                 # thread pool size for parallel tasks
MAX_FETCH_CONCURRENCY = 8        # global cap on concurrent HTTP requests
REQ_TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN = (0.2, 0.6)       # small jitter between requests
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; pickapart-crawler/1.1)"}
# ==========================

# ---------- Global concurrency primitives ----------
_CONCURRENCY_SEM = threading.BoundedSemaphore(MAX_FETCH_CONCURRENCY)
_WRITE_LOCK = threading.Lock()
_thread_local = threading.local()

# ---------- Utilities ----------
def sleep_a_bit() -> None:
    """Sleep for a small random interval to avoid hammering the site."""
    lo, hi = SLEEP_BETWEEN
    time.sleep(random.uniform(lo, hi))

def _get_session() -> requests.Session:
    """Return a thread-local Session with connection pooling."""
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS
        )
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        _thread_local.session = sess
    return sess

def fetch(url: str, retries: int = RETRIES) -> requests.Response:
    """
    GET a URL with basic retry, jitter, and a global concurrency cap.
    Uses a thread-local requests.Session for connection pooling.
    """
    backoff = 0.5
    for attempt in range(retries):
        with _CONCURRENCY_SEM:
            try:
                sleep_a_bit()  # tiny delay before each request
                resp = _get_session().get(url, timeout=REQ_TIMEOUT, headers=HEADERS)
                resp.raise_for_status()
                return resp
            except Exception as e:
                if attempt == retries - 1:
                    raise
        # exponential backoff with jitter
        time.sleep(backoff + random.uniform(0, 0.3))
        backoff *= 2

def snake(s: str) -> str:
    """Convert a string to lowercase snake_case, stripping non-alphanumerics."""
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")

def get_vehicle_id(url: str) -> str:
    """Extract VehicleID query param from a Display_Vehicle.asp URL."""
    q = parse_qs(urlparse(url).query)
    return str(q.get("VehicleID", [""])[0])

# ---------- Vehicle page parsing ----------
def pick_details_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Heuristic to find a 2-column details table where the left column looks like labels.
    Returns the table or None.
    """
    for t in tables:
        if t.shape[1] == 2 and 2 <= len(t) <= 50:
            left = t.iloc[:, 0].astype(str).str.len().median()
            right = t.iloc[:, 1].astype(str).str.len().median()
            if left < right:
                return t
    return None

def normalise_keys(d: Dict[str, str]) -> Dict[str, str]:
    """Normalise dictionary keys to snake_case while leaving values untouched."""
    return {snake(k): v for k, v in d.items()}

def extract_vehicle_details_fallback(text: str) -> Dict[str, str]:
    """
    Extract vehicle details from raw page text when no neat details table is available.
    Normalises attribute names to snake_case.
    """
    raw: Dict[str, Optional[str]] = {}

    def grab(*patterns, flags=re.I) -> Optional[str]:
        for pat in patterns:
            m = re.search(pat, text, flags)
            if m:
                for g in m.groups()[::-1]:
                    if g:
                        return g.strip()
        return None

    raw["vehicle_ref"]   = grab(r"Vehicle\s*Ref[:\s]*([^\n\r]+)")
    raw["acq"]           = grab(r"\bACQ[:\s]*([A-Z0-9]+)\b", r"\bACQ\s*([0-9]+)\b", r"\b(ACQ[0-9]+)\b")
    raw["yard_location"] = grab(r"Yard\s*location[:\s]*([^\n\r]+)")
    raw["make_model"]    = grab(r"Make/Model[:\s]*([^\n\r]+)")
    raw["engine"]        = grab(r"Engine[:\s]*([^\n\r]+)", r"\bEngine\s*([0-9.]+[A-Za-z]*)")
    raw["transmission"]  = grab(r"Transmission[:\s]*([^\n\r]+)", r"\bTransmission\s*([A-Za-z/]+)")
    raw["year"]          = grab(r"Year[:\s]*([0-9]{4})", r"\bYear\s*([0-9]{4})")
    raw["odometer"]      = grab(r"Odometer[:\s]*([0-9,]+)")
    raw["date_in_stock"] = grab(r"Date\s*In\s*Stock[:\s]*([^\n\r]+)")
    raw["description"]   = grab(r"Description[:\s]*([^\n\r]*)")

    return {k: v for k, v in raw.items() if v}

def extract_sold_parts(soup: BeautifulSoup, tables: List[pd.DataFrame]) -> List[str]:
    """
    Extract a deduplicated list of already sold parts.
    Looks for a heading containing 'sold', then scans nearby tables and lists.
    Also heuristically scans all tables for a 'part' column or single-column lists.
    """
    sold: List[str] = []

    heading = soup.find(
        lambda tag: tag.name in ["h1", "h2", "h3", "h4", "h5", "h6", "strong", "b"]
        and re.search(r"\bsold\b", tag.get_text(strip=True), re.I)
    )
    if heading:
        next_tbl = heading.find_next("table")
        if next_tbl:
            try:
                df = pd.read_html(str(next_tbl))[0]
                for col in df.columns:
                    parts = df[col].dropna().astype(str)
                    parts = parts[~parts.str.match(r"^\s*(Part|Qty|Price|Status|Sold)\s*$", case=False)]
                    sold.extend(parts.tolist())
            except Exception:
                pass
        for ul in heading.find_all_next(["ul", "ol"], limit=2):
            for li in ul.find_all("li"):
                txt = li.get_text(" ", strip=True)
                if txt:
                    sold.append(txt)

    for t in tables:
        df = t.copy()
        df.columns = [str(c) for c in df.columns]
        likely_cols = [c for c in df.columns if re.search(r"\bpart\b", c, re.I)]
        if likely_cols:
            for col in likely_cols:
                parts = df[col].dropna().astype(str)
                parts = parts[parts.str.len() >= 2]
                sold.extend(parts.tolist())
        elif df.shape[1] == 1:
            parts = df.iloc[:, 0].dropna().astype(str)
            parts = parts[parts.str.len() >= 2]
            sold.extend(parts.tolist())

    seen, deduped = set(), []
    for p in [s.strip() for s in sold if s.strip()]:
        if p not in seen:
            seen.add(p)
            deduped.append(p)
    return deduped

def scrape_vehicle(vehicle_url: str, site_name: Optional[str] = None, series_url: Optional[str] = None) -> Dict:
    """
    Scrape a single Display_Vehicle.asp page.
    Returns a dict with:
      - vehicle_id: str
      - site: str (optional)
      - series_url: str (optional)
      - details: dict of snake_case attributes incl. source_url
      - sold_parts: list[str]
      - scraped_at: UTC timestamp string YYYYMMDDTHHMMSSZ
      - run_id: run_YYYY_MM_DD_HH_MM_SS
    """
    resp = fetch(vehicle_url)
    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(separator="\n", strip=True)

    try:
        tables = pd.read_html(html)
    except ValueError:
        tables = []

    details: Dict[str, str] = {}
    dtbl = pick_details_table(tables)
    if dtbl is not None:
        dtbl.columns = ["field", "value"]
        details = dict(zip(dtbl["field"].astype(str).str.strip(),
                           dtbl["value"].astype(str).str.strip()))
        details = normalise_keys(details)

    fallback = extract_vehicle_details_fallback(page_text)
    details.update(fallback)
    details["source_url"] = vehicle_url

    sold_parts = extract_sold_parts(soup, tables)

    vehicle_id = get_vehicle_id(vehicle_url)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output = {
        "vehicle_id": vehicle_id,
        "site": site_name if site_name else None,
        "series_url": series_url if series_url else None,
        "details": details,
        "sold_parts": sold_parts,
        "scraped_at": ts,
        "run_id": RUN_ID,
    }
    return {k: v for k, v in output.items() if v is not None}

# ---------- Site discovery & traversal ----------
SITE_URL_PATTERNS = (
    re.compile(r"/([A-Za-z-]+)-Stock/?$", re.I),           # /Avondale-Stock
    re.compile(r"/Current-Stock-([A-Za-z-]+)/?$", re.I),   # /Current-Stock-Wellington
)

def _extract_site_name_from_match(match: re.Match) -> str:
    """Turn regex match group into a nicely cased site name: e.g., 'new-plymouth' -> 'New Plymouth'."""
    raw = match.group(1)
    raw = raw.replace("-", " ").strip()
    return " ".join(part.capitalize() for part in raw.split())

def discover_site_stock_links() -> Dict[str, str]:
    """
    Auto-discover site stock URLs from the homepage by matching:
      - '/<Place>-Stock'
      - '/Current-Stock-<Place>'
    Validates each candidate by checking it contains 'Display_Vehicles.asp'.
    Returns: {site_name: absolute_url}
    """
    resp = fetch(HOMEPAGE)
    soup = BeautifulSoup(resp.text, "lxml")

    candidates: Dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href_abs = urljoin(HOMEPAGE, a["href"])
        path = urlparse(href_abs).path or "/"
        site_name_from_text = a.get_text(" ", strip=True)

        matched = None
        for pat in SITE_URL_PATTERNS:
            m = pat.search(path)
            if m:
                matched = m
                break
        if not matched:
            continue

        site_name = _extract_site_name_from_match(matched) or site_name_from_text.title()
        if site_name:
            # Prefer https if multiple seen
            if site_name not in candidates or href_abs.startswith("https://"):
                candidates[site_name] = href_abs

    # Validate in parallel (small set)
    validated: Dict[str, str] = {}
    def _validate(site_item: Tuple[str, str]) -> Optional[Tuple[str, str]]:
        name, url = site_item
        try:
            r = fetch(url)
            if "Display_Vehicles.asp" in r.text:
                return name, url
        except Exception:
            return None
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as ex:
        for res in ex.map(_validate, candidates.items()):
            if res:
                validated[res[0]] = res[1]

    return validated or candidates

def extract_series_links(site_stock_url: str) -> List[str]:
    """
    From a site stock page (e.g., Takanini-Stock), collect all links to Display_Vehicles.asp.
    Returns a sorted list of absolute URLs.
    """
    resp = fetch(site_stock_url)
    soup = BeautifulSoup(resp.text, "lxml")
    series_urls: set = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(site_stock_url, a["href"])
        if "Display_Vehicles.asp" in href:
            series_urls.add(href)
    return sorted(series_urls)

def extract_vehicle_links(series_url: str) -> List[str]:
    """
    From a Display_Vehicles.asp page, collect all links to Display_Vehicle.asp.
    Returns a sorted list of absolute URLs.
    """
    resp = fetch(series_url)
    soup = BeautifulSoup(resp.text, "lxml")
    vehicle_urls: set = set()

    for a in soup.find_all("a", href=True):
        href = urljoin(series_url, a["href"])
        if "Display_Vehicle.asp" in href:
            vehicle_urls.add(href)

    # Fallback: scan any HTML tables for embedded hrefs
    try:
        dfs = pd.read_html(resp.text)
        for df in dfs:
            for col in df.columns:
                for val in df[col].astype(str).tolist():
                    for m in re.finditer(r'href=["\']([^"\']+Display_Vehicle\.asp[^"\']*)', val, re.I):
                        vehicle_urls.add(urljoin(series_url, m.group(1)))
    except ValueError:
        pass

    return sorted(vehicle_urls)

# ---------- Writer ----------
def write_vehicle_json_to_onelake(data: Dict) -> None:
    """
    Write one vehicle JSON into the current run folder.
    File name: vehicle_<VehicleID>_<UTC>.json
    """
    vehicle_id = data.get("vehicle_id", "unknown")
    ts = data.get("scraped_at", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    vehicle_filename = f"vehicle_{vehicle_id}_{ts}.json"
    vehicle_path = f"{VEHICLE_OUT_BASE_RUN}/{vehicle_filename}"
    payload = json.dumps(data, ensure_ascii=False, indent=2)

    if RUN_DRY:
        print("[dry-run] would write:", vehicle_path)
    else:
        # Serialise writes to be safe (some FS layers don't love concurrent writes)
        with _WRITE_LOCK:
            notebookutils.fs.put(vehicle_path, payload, overwrite=True)
        print("wrote:", vehicle_path)

# ---------- Parallel driver helpers ----------
def _series_for_site(site_item: Tuple[str, str]) -> Tuple[str, List[str]]:
    """Fetch all series links for a site (name, url) -> (site_name, [series_urls])."""
    site_name, site_url = site_item
    series = extract_series_links(site_url)
    if SERIES_LIMIT_PER_SITE:
        series = series[:SERIES_LIMIT_PER_SITE]
    return site_name, series

def _vehicles_for_series(input_item: Tuple[str, str]) -> Tuple[str, str, List[str]]:
    """Fetch all vehicle links for a given series -> (site_name, series_url, [vehicle_urls])."""
    site_name, series_url = input_item
    v_urls = extract_vehicle_links(series_url)
    if VEHICLE_LIMIT_PER_SERIES:
        v_urls = v_urls[:VEHICLE_LIMIT_PER_SERIES]
    return site_name, series_url, v_urls

def _scrape_and_write(item: Tuple[str, str, str]) -> Optional[str]:
    """Scrape vehicle and write JSON. Returns vehicle_id on success."""
    site_name, series_url, vehicle_url = item
    try:
        data = scrape_vehicle(vehicle_url, site_name=site_name, series_url=series_url)
        write_vehicle_json_to_onelake(data)
        return data.get("vehicle_id")
    except Exception as e:
        print(f"  ! error scraping: {vehicle_url}\n    {e}")
        return None

# ---------- Driver ----------
def crawl_all_pickapart() -> None:
    """
    Crawl all Pick-a-Part sites with parallelism:
      1) Auto-discover site stock pages (validated)
      2) In parallel, fetch series pages for each site
      3) In parallel, fetch vehicle links for each series
      4) In parallel, scrape vehicles and write unified JSON into run_<timestamp> folder

    Respects RUN_DRY, and the *_LIMIT settings for testing.
    """
    print(f"Run folder: {VEHICLE_OUT_BASE_RUN}")

    # Step 1: discover sites
    site_links = discover_site_stock_links()
    if not site_links:
        print("No sites discovered from homepage. Exiting.")
        return
    items = list(site_links.items())
    if SITE_LIMIT:
        items = items[:SITE_LIMIT]
    print(f"Sites: {len(items)}")

    # Step 2: series per site (parallel)
    print("Discovering series pages in parallel...")
    site_series: List[Tuple[str, List[str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as ex:
        for res in ex.map(_series_for_site, items):
            site_series.append(res)

    # Step 3: vehicles per series (parallel)
    print("Discovering vehicle pages in parallel...")
    site_series_pairs: List[Tuple[str, str]] = [
        (site_name, s_url) for site_name, series_list in site_series for s_url in series_list
    ]
    vehicle_triplets: List[Tuple[str, str, List[str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for res in ex.map(_vehicles_for_series, site_series_pairs):
            vehicle_triplets.append(res)

    # Flatten + de-duplicate vehicle URLs
    all_vehicle_tasks: List[Tuple[str, str, str]] = []
    seen_urls: set = set()
    for site_name, series_url, v_urls in vehicle_triplets:
        for vurl in v_urls:
            if vurl not in seen_urls:
                seen_urls.add(vurl)
                all_vehicle_tasks.append((site_name, series_url, vurl))

    print(f"Total unique vehicles to scrape: {len(all_vehicle_tasks)}")

    # Step 4: scrape vehicles in parallel
    scraped_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for vehicle_id in ex.map(_scrape_and_write, all_vehicle_tasks):
            if vehicle_id:
                scraped_count += 1

    print(f"Done. Scraped {scraped_count} vehicles into {VEHICLE_OUT_BASE_RUN}")

# ---------- Main ----------
if __name__ == "__main__":
    """
    Entry point when running this cell as a script/notebook.
    Adjust CONFIG above (limits, RUN_DRY, MAX_WORKERS), then run.
    """
    crawl_all_pickapart()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
