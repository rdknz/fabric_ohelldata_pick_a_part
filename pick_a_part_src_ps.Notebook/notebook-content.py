# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
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
# META     },
# META     "environment": {
# META       "environmentId": "e2a66915-fbf9-b5df-4d59-8200dd2a638a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# Fabric PySpark notebook version of the Pick-a-Part crawler
# Writes per-vehicle JSON files to OneLake using mssparkutils

import pandas as pd
import os
import requests, re, json, time, random, threading, concurrent.futures
from urllib.parse import urlparse, parse_qs, urljoin
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple

# Fabric utilities for OneLake access
from notebookutils import mssparkutils  # Available in Fabric/Synapse Spark runtimes

# ========= CONFIG =========
HOMEPAGE = "https://www.pickapart.co.nz/"

# Lakehouse base folder for per-vehicle JSONs (Files area)
VEHICLE_OUT_BASE = (
    "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/"
    "a743e7da-a128-4060-a81f-4d6844a04abd/Files/pickapart/raw/vehicle"
)

# Generate a unique run folder (UTC)
RUN_ID = datetime.now(timezone.utc).strftime("run_%Y_%m_%d_%H_%M_%S")
VEHICLE_OUT_BASE_RUN = f"{VEHICLE_OUT_BASE}/{RUN_ID}"

# Crawl controls
RUN_DRY = False                                 # Set True to test without writing
SITE_LIMIT: Optional[int] = None               # e.g. 2 for testing
SERIES_LIMIT_PER_SITE: Optional[int] = None    # e.g. 5 for testing
VEHICLE_LIMIT_PER_SERIES: Optional[int] = None # e.g. 5 for testing

#create cpu count or default to 4
cpu = os.cpu_count() or 4

# Parallelism + HTTP politeness
MAX_WORKERS = max(6, min(12, cpu))          # clamp to 6..12
MAX_FETCH_CONCURRENCY = MAX_WORKERS * 2
REQ_TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN = (0.2, 0.6)       # small jitter between requests
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; pickapart-crawler/1.2 Fabric)"}
# ==========================

# ---------- Global concurrency primitives ----------
_CONCURRENCY_SEM = threading.BoundedSemaphore(MAX_FETCH_CONCURRENCY)
_WRITE_LOCK = threading.Lock()
_thread_local = threading.local()

# ---------- Utilities ----------
def sleep_a_bit() -> None:
    lo, hi = SLEEP_BETWEEN
    time.sleep(random.uniform(lo, hi))

def _get_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max(MAX_WORKERS, 8),
            pool_maxsize=max(MAX_WORKERS, 8)
        )
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        _thread_local.session = sess
    return sess

def fetch(url: str, retries: int = RETRIES) -> requests.Response:
    backoff = 0.5
    for attempt in range(retries):
        with _CONCURRENCY_SEM:
            try:
                sleep_a_bit()
                resp = _get_session().get(url, timeout=REQ_TIMEOUT, headers=HEADERS)
                resp.raise_for_status()
                return resp
            except Exception:
                if attempt == retries - 1:
                    raise
        time.sleep(backoff + random.uniform(0, 0.3))
        backoff *= 2

def snake(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower()).strip("_")

def get_vehicle_id(url: str) -> str:
    q = parse_qs(urlparse(url).query)
    return str(q.get("VehicleID", [""])[0])

# ---------- Vehicle page parsing ----------
def pick_details_table(tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
    for t in tables:
        if t.shape[1] == 2 and 2 <= len(t) <= 50:
            left = t.iloc[:, 0].astype(str).str.len().median()
            right = t.iloc[:, 1].astype(str).str.len().median()
            if left < right:
                return t
    return None

def normalise_keys(d: Dict[str, str]) -> Dict[str, str]:
    return {snake(k): v for k, v in d.items()}

def extract_vehicle_details_fallback(text: str) -> Dict[str, str]:
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
    re.compile(r"/([A-Za-z-]+)-Stock/?$", re.I),
    re.compile(r"/Current-Stock-([A-Za-z-]+)/?$", re.I),
)

def _extract_site_name_from_match(match: re.Match) -> str:
    raw = match.group(1)
    raw = raw.replace("-", " ").strip()
    return " ".join(part.capitalize() for part in raw.split())

def discover_site_stock_links() -> Dict[str, str]:
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
            if site_name not in candidates or href_abs.startswith("https://"):
                candidates[site_name] = href_abs

    # validate
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
    resp = fetch(site_stock_url)
    soup = BeautifulSoup(resp.text, "lxml")
    series_urls: set = set()
    for a in soup.find_all("a", href=True):
        href = urljoin(site_stock_url, a["href"])
        if "Display_Vehicles.asp" in href:
            series_urls.add(href)
    return sorted(series_urls)

def extract_vehicle_links(series_url: str) -> List[str]:
    resp = fetch(series_url)
    soup = BeautifulSoup(resp.text, "lxml")
    vehicle_urls: set = set()

    for a in soup.find_all("a", href=True):
        href = urljoin(series_url, a["href"])
        if "Display_Vehicle.asp" in href:
            vehicle_urls.add(href)

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
def ensure_run_folder():
    try:
        mssparkutils.fs.mkdirs(VEHICLE_OUT_BASE_RUN)
    except Exception:
        # exists is fine
        pass

def write_vehicle_json_to_onelake(data: Dict) -> None:
    vehicle_id = data.get("vehicle_id", "unknown")
    ts = data.get("scraped_at", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    vehicle_filename = f"vehicle_{vehicle_id}_{ts}.json"
    vehicle_path = f"{VEHICLE_OUT_BASE_RUN}/{vehicle_filename}"
    payload = json.dumps(data, ensure_ascii=False, indent=2)

    if RUN_DRY:
        print("[dry-run] would write:", vehicle_path)
    else:
        with _WRITE_LOCK:
            mssparkutils.fs.put(vehicle_path, payload, True)
        print("wrote:", vehicle_path)

# ---------- Parallel driver helpers ----------
def _series_for_site(site_item: Tuple[str, str]) -> Tuple[str, List[str]]:
    site_name, site_url = site_item
    series = extract_series_links(site_url)
    if SERIES_LIMIT_PER_SITE:
        series = series[:SERIES_LIMIT_PER_SITE]
    return site_name, series

def _vehicles_for_series(input_item: Tuple[str, str]) -> Tuple[str, str, List[str]]:
    site_name, series_url = input_item
    v_urls = extract_vehicle_links(series_url)
    if VEHICLE_LIMIT_PER_SERIES:
        v_urls = v_urls[:VEHICLE_LIMIT_PER_SERIES]
    return site_name, series_url, v_urls

def _scrape_and_write(item: Tuple[str, str, str]) -> Optional[str]:
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
    print(f"Run folder: {VEHICLE_OUT_BASE_RUN}")
    ensure_run_folder()

    site_links = discover_site_stock_links()
    if not site_links:
        print("No sites discovered from homepage. Exiting.")
        return
    items = list(site_links.items())
    if SITE_LIMIT:
        items = items[:SITE_LIMIT]
    print(f"Sites: {len(items)}")

    print("Discovering series pages in parallel...")
    site_series: List[Tuple[str, List[str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as ex:
        for res in ex.map(_series_for_site, items):
            site_series.append(res)

    print("Discovering vehicle pages in parallel...")
    site_series_pairs: List[Tuple[str, str]] = [
        (site_name, s_url) for site_name, series_list in site_series for s_url in series_list
    ]
    vehicle_triplets: List[Tuple[str, str, List[str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for res in ex.map(_vehicles_for_series, site_series_pairs):
            vehicle_triplets.append(res)

    all_vehicle_tasks: List[Tuple[str, str, str]] = []
    seen_urls: set = set()
    for site_name, series_url, v_urls in vehicle_triplets:
        for vurl in v_urls:
            if vurl not in seen_urls:
                seen_urls.add(vurl)
                all_vehicle_tasks.append((site_name, series_url, vurl))

    print(f"Total unique vehicles to scrape: {len(all_vehicle_tasks)}")

    scraped_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for vehicle_id in ex.map(_scrape_and_write, all_vehicle_tasks):
            if vehicle_id:
                scraped_count += 1

    print(f"Done. Scraped {scraped_count} vehicles into {VEHICLE_OUT_BASE_RUN}")

# ---------- Main ----------
if __name__ == "__main__":
    crawl_all_pickapart()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import functions as F

table_name = "vehicles_with_parts"
target_path = "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/a743e7da-a128-4060-a81f-4d6844a04abd/Tables/vehicles_with_parts"

# 0) Defensive: ensure we actually have data to write
assert flat.limit(1).count() > 0, "No rows in 'flat'. Fix the upstream parse before writing the table."

# 1) Drop table if it exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# 2) Remove the existing path so we don't inherit a broken Delta log
try:
    mssparkutils.fs.rm(target_path, recurse=True)
    print("Removed existing folder:", target_path)
except Exception as e:
    print("Note: could not remove folder (may not exist):", e)

# 3) Write fresh Delta with schema and partition
(flat.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")       # ensure schema is persisted
 .partitionBy("scraped_at_date")
 .save(target_path))

# 4) Register table at that location
spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{target_path}'")

# 5) Try to set very long history retention (ignore if policy blocks it)
try:
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 999999 days',
        'delta.deletedFileRetentionDuration' = 'interval 999999 days',
        'delta.enableExpiredLogCleanup' = 'false'
      )
    """)
except Exception as e:
    print("Note: retention properties not applied:", e)

# 6) Peek a few rows
display(spark.table(table_name).limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
