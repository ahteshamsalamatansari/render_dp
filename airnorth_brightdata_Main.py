"""
Airnorth Fare Tracker - Async Brightdata Version
===============================================
Fast, reliable, non-blocking scraper with:

- Brightdata Web Unlocker API
- queue workers
- retries
- hard stop when Brightdata fails after 3 attempts
- JSONL crash-safe checkpointing
- resume support
- final CSV + Excel export

Install:
    pip install beautifulsoup4 pandas openpyxl python-dotenv

Run:
    python airnorth_fast_async.py --all --workers 4
    python airnorth_fast_async.py --all --workers 6 --delay-min 0.2 --delay-max 0.7
    python airnorth_fast_async.py --route BME-KNX --workers 3
    python airnorth_fast_async.py --resume-dir output/airnorth_20260506_143000 --workers 4
"""

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

import pandas as pd
from bs4 import BeautifulSoup

try:
    import aiohttp
except Exception:
    aiohttp = None


try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# ══════════════════════════════════════════════════════
ROUTES = [
    ("BME", "KNX"),
    ("BME", "DRW"),
    ("DRW", "KNX"),
    ("DRW", "BME"),
    ("KNX", "DRW"),
    ("KNX", "BME"),
]

# Full airport display names required by Airnorth's booking engine
AIRPORT_NAMES = {
    "BME": "Broome",
    "KNX": "Kununurra",
    "DRW": "Darwin",
}

AIRLINE = "Airnorth"
SOURCE = "airnorth.com.au"
BASE_URL = "https://secure.airnorth.com.au/ibe/availability"
DEFAULT_DAYS_OUT = 84

OUTPUT_ROOT = Path("output")
OUTPUT_ROOT.mkdir(exist_ok=True)

FINAL_COLUMNS = [
    "Date Checked",
    "Time Checked",
    "Airline",
    "Date of Departure",
    "Time of Departure",
    "Origin",
    "Destination",
    "Fare Price",
    "Fare Class",
    "Source",
]

BLOCKED_TEXT_MARKERS = [
    "just a moment",
    "verify you are human",
    "checking your browser",
    "attention required",
    "access denied",
    "captcha",
    "cf-challenge",
]

NO_FLIGHT_MARKERS = [
    "no flights were found",
    "no-flights",
]

BRIGHTDATA_API_URL = "https://api.brightdata.com/request"
BRIGHTDATA_DEFAULT_ZONE = "web_unlocker1"
BRIGHTDATA_CHECK_URL = "https://geo.brdtest.com/welcome.txt?product=unlocker&method=api"
BRIGHTDATA_RESPONSE_FORMAT = "raw"
BRIGHTDATA_MAX_ATTEMPTS = 3
BRIGHTDATA_WARNING = "Brightdata Not Working Please Fix it"
BRIGHTDATA_REQUEST_TIMEOUT_S = float(os.getenv("BRIGHTDATA_REQUEST_TIMEOUT_S", "90"))
BRIGHTDATA_RETRY_DELAY_BASE_S = float(os.getenv("BRIGHTDATA_RETRY_DELAY_BASE_S", "0.5"))
BRIGHTDATA_RETRY_DELAY_MAX_S = float(os.getenv("BRIGHTDATA_RETRY_DELAY_MAX_S", "2.0"))
WORKER_SAFETY_CAP = int(os.getenv("AIRNORTH_WORKER_SAFETY_CAP", "30"))
# How many distinct worker-level Brightdata failures before declaring a fatal outage.
# A value of 1 means any single timeout kills the run (old behaviour). 2+ is safer.
BRIGHTDATA_FATAL_THRESHOLD = int(os.getenv("BRIGHTDATA_FATAL_THRESHOLD", "2"))
# Retry passes after the main run. Each pass waits longer and uses fewer workers.
MAX_RETRY_PASSES = int(os.getenv("AIRNORTH_MAX_RETRY_PASSES", "3"))
RETRY_PASS_DELAY_S  = [30, 60, 120]   # seconds to wait before each retry pass
RETRY_PASS_WORKERS  = [4, 2, 1]       # workers per retry pass


# ══════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════

@dataclass(frozen=True)
class Config:
    days_out: int
    workers: int
    retries: int
    goto_timeout_ms: int
    selector_timeout_ms: int
    cloudflare_timeout_s: int
    delay_min: float
    delay_max: float
    block_assets: bool
    selected_routes: list[tuple[str, str]]
    run_dir: Path
    raw_jsonl: Path
    error_jsonl: Path
    final_csv: Path
    final_xlsx: Path
    progress_callback: object = None
    stop_requested: object = None
    fatal_stop: object = None


@dataclass(frozen=True)
class Job:
    origin: str
    destination: str
    departure_date: datetime

    @property
    def key(self) -> str:
        return f"{self.origin}-{self.destination}-{self.departure_date.strftime('%Y-%m-%d')}"


class BrightDataUnavailable(RuntimeError):
    """Raised when the Brightdata provider cannot serve requests."""


@dataclass(frozen=True)
class BrightDataClient:
    api_url: str
    token: str
    zone: str
    response_format: str
    check_url: str
    country: str = ""


def stop_requested(cfg: Config) -> bool:
    fatal_stop = getattr(cfg, "fatal_stop", None)
    if fatal_stop and fatal_stop.is_set():
        return True

    callback = cfg.stop_requested
    if not callback:
        return False
    try:
        return bool(callback())
    except Exception:
        return False


async def interruptible_async_sleep(seconds, stop_requested_fn=None):
    end = time.time() + seconds
    while time.time() < end:
        if stop_requested_fn and stop_requested_fn():
            return True
        await asyncio.sleep(min(1, end - time.time()))
    return False


def retry_delay_seconds(attempt: int, is_timeout: bool = False) -> float:
    if is_timeout:
        # Timeouts need a much longer back-off to let the server recover.
        # 10 s, 20 s, 30 s — so repeated timeouts don't hammer the proxy.
        return min(30.0, 10.0 * attempt)
    return min(BRIGHTDATA_RETRY_DELAY_MAX_S, BRIGHTDATA_RETRY_DELAY_BASE_S * attempt)


# ══════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════


def setup_logging(run_dir: Path) -> None:
    log_file = run_dir / "run.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
        force=True,
    )


# ══════════════════════════════════════════════════════
# Env / proxy helpers
# ══════════════════════════════════════════════════════

def make_run_id(now: Optional[datetime] = None) -> str:
    return (now or datetime.now()).strftime("%Y%m%d_%H%M%S")


def make_output_stamp(run_id: str) -> str:
    try:
        dt = datetime.strptime(run_id, "%Y%m%d_%H%M%S")
        return dt.strftime("%I-%M-%p_%d-%m-%Y")
    except ValueError:
        return run_id


def first_env_value(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def get_brightdata_client() -> BrightDataClient:
    token = first_env_value(
        "BRIGHTDATA_API_TOKEN",
        "BRIGHTDATA_TOKEN",
        "BRIGHTDATA_BEARER_TOKEN",
        "BRIGHT_API_TOKEN",
    )

    if not token:
        raise BrightDataUnavailable("Missing BRIGHTDATA_API_TOKEN in .env")

    return BrightDataClient(
        api_url=os.getenv("BRIGHTDATA_API_URL", BRIGHTDATA_API_URL).strip() or BRIGHTDATA_API_URL,
        token=token,
        zone=first_env_value("BRIGHTDATA_API_ZONE", "BRIGHTDATA_ZONE") or BRIGHTDATA_DEFAULT_ZONE,
        response_format=os.getenv("BRIGHTDATA_FORMAT", BRIGHTDATA_RESPONSE_FORMAT).strip()
        or BRIGHTDATA_RESPONSE_FORMAT,
        check_url=os.getenv("BRIGHTDATA_CHECK_URL", BRIGHTDATA_CHECK_URL).strip()
        or BRIGHTDATA_CHECK_URL,
        country=first_env_value("BRIGHTDATA_API_COUNTRY", "BRIGHTDATA_COUNTRY"),
    )


def brightdata_auth_header(token: str) -> str:
    if token.lower().startswith("bearer "):
        return token
    return f"Bearer {token}"


BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def brightdata_request_sync(client: BrightDataClient, url: str) -> str:
    payload = {
        "zone": client.zone,
        "url": url,
        "format": client.response_format,
        "method": "GET",
        "headers": BROWSER_HEADERS,
    }
    if client.country:
        payload["country"] = client.country

    body = json.dumps(payload).encode("utf-8")
    request = urllib_request.Request(
        client.api_url,
        data=body,
        headers={
            "Authorization": brightdata_auth_header(client.token),
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(request, timeout=BRIGHTDATA_REQUEST_TIMEOUT_S) as response:
            status = response.getcode()
            text = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as e:
        details = e.read().decode("utf-8", errors="replace")[:500]
        raise BrightDataUnavailable(f"HTTP {e.code}: {details}") from e
    except urllib_error.URLError as e:
        raise BrightDataUnavailable(str(e.reason)) from e
    except TimeoutError as e:
        raise BrightDataUnavailable("Request timed out") from e

    if status < 200 or status >= 300:
        raise BrightDataUnavailable(f"HTTP {status}: {text[:500]}")

    return text


async def brightdata_request_async(session, client: BrightDataClient, url: str) -> str:
    payload = {
        "zone": client.zone,
        "url": url,
        "format": client.response_format,
        "method": "GET",
        "headers": BROWSER_HEADERS,
    }
    if client.country:
        payload["country"] = client.country

    try:
        async with session.post(
            client.api_url,
            json=payload,
            headers={
                "Authorization": brightdata_auth_header(client.token),
                "Content-Type": "application/json",
            },
        ) as response:
            text = await response.text(errors="replace")
            status = response.status
    except asyncio.TimeoutError as e:
        raise BrightDataUnavailable("Request timed out") from e
    except Exception as e:
        raise BrightDataUnavailable(str(e)) from e

    if status < 200 or status >= 300:
        raise BrightDataUnavailable(f"HTTP {status}: {text[:500]}")

    return text


async def fetch_brightdata_url(client: BrightDataClient, url: str, session=None) -> str:
    if session:
        return await brightdata_request_async(session, client, url)
    return await asyncio.to_thread(brightdata_request_sync, client, url)


async def verify_brightdata(client: BrightDataClient, cfg: Config, session=None) -> None:
    last_error = None

    for attempt in range(1, BRIGHTDATA_MAX_ATTEMPTS + 1):
        if stop_requested(cfg):
            raise BrightDataUnavailable("Stop requested")

        try:
            await fetch_brightdata_url(client, client.check_url, session=session)
            logging.info("Brightdata check passed on attempt %s", attempt)
            return
        except Exception as e:
            last_error = e
            logging.warning(
                "Brightdata check attempt %s/%s failed: %s",
                attempt,
                BRIGHTDATA_MAX_ATTEMPTS,
                e,
            )

        if attempt < BRIGHTDATA_MAX_ATTEMPTS:
            await interruptible_async_sleep(
                retry_delay_seconds(attempt),
                lambda: stop_requested(cfg),
            )

    raise BrightDataUnavailable(str(last_error) if last_error else "Brightdata check failed")


# ══════════════════════════════════════════════════════
# Parsing
# ══════════════════════════════════════════════════════

def parse_flights(html: str) -> list[dict]:
    """
    Parse HTML and extract flight time + cheapest fare.
    """
    soup = BeautifulSoup(html, "html.parser")
    flights_found = []

    flight_rows = soup.select(".js-scheduled-flight")

    if not flight_rows:
        lower_html = html.lower()
        if any(marker in lower_html for marker in NO_FLIGHT_MARKERS):
            return [
                {
                    "price": None,
                    "time": "N/A",
                    "fare_class": "NO FLIGHTS",
                }
            ]
        return []

    for row in flight_rows:
        time_el = row.select_one(".desktop-route-block .time") or row.select_one(".time")
        dep_time = time_el.get_text(strip=True) if time_el else "N/A"

        fares = []
        price_elements = row.select(".bundle-budget")

        if not price_elements:
            fare_container = row.select_one(".fare-container")
            if fare_container:
                price_elements = fare_container.select(".bundle-budget")

        for el in price_elements:
            txt = (
                el.get_text(strip=True)
                .replace("$", "")
                .replace(",", "")
                .strip()
            )

            try:
                fares.append(float(txt))
            except ValueError:
                continue

        cheapest = min(fares) if fares else None

        flights_found.append(
            {
                "price": cheapest,
                "time": dep_time,
                "fare_class": "Economy" if cheapest is not None else "NO FLIGHTS",
            }
        )

    return flights_found


def looks_blocked(title: str, html: str) -> bool:
    text = f"{title}\n{html[:3000]}".lower()
    return any(marker in text for marker in BLOCKED_TEXT_MARKERS)


# ══════════════════════════════════════════════════════
# File/checkpoint helpers
# ══════════════════════════════════════════════════════

def read_completed_keys(raw_jsonl: Path) -> set[str]:
    """
    Reads successful jobs from previous JSONL output.
    Used for resume.
    """
    completed = set()

    if not raw_jsonl.exists():
        return completed

    with raw_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            if record.get("status") == "OK":
                completed.add(record["job_key"])

    return completed


def read_failed_jobs(error_jsonl: Path, completed_keys: set[str]) -> list[Job]:
    """
    Reads failed jobs from error JSONL and returns only jobs that are not
    already completed successfully in raw_jsonl.

    This powers the calm retry phase after the normal fast run completes.
    """
    failed_by_key: dict[str, Job] = {}

    if not error_jsonl.exists():
        return []

    with error_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            job_key = record.get("job_key")
            status = str(record.get("status") or "").upper()

            if not job_key or job_key in completed_keys:
                continue

            # Only retry real failures. Do not retry user-cancelled jobs.
            if status in {"OK", "CANCELLED"}:
                continue

            try:
                departure_date = datetime.strptime(record["departure_date"], "%Y-%m-%d")
                origin = str(record["origin"]).strip().upper()
                destination = str(record["destination"]).strip().upper()
            except Exception:
                continue

            failed_by_key[job_key] = Job(
                origin=origin,
                destination=destination,
                departure_date=departure_date,
            )

    return list(failed_by_key.values())


async def retry_failed_jobs(
    cfg: Config,
    client: BrightDataClient,
    all_jobs: list,
    session=None,
) -> None:
    """
    Multi-pass retry loop that guarantees every date is captured.

    After the main run, this compares the full job list against raw_jsonl
    (the source of truth for successes) and retries anything not yet captured —
    regardless of how it was labelled (FAILED, CANCELLED, or never recorded).

    Each pass uses fewer workers and a longer delay to ride out transient
    Brightdata issues. A fresh fatal_stop is created per pass so a previous
    outage does not block recovery.
    """
    for pass_num in range(MAX_RETRY_PASSES):
        completed = read_completed_keys(cfg.raw_jsonl)
        pending = [job for job in all_jobs if job.key not in completed]

        if not pending:
            logging.info("All %s jobs captured successfully.", len(all_jobs))
            return

        delay   = RETRY_PASS_DELAY_S[pass_num]  if pass_num < len(RETRY_PASS_DELAY_S)  else 120
        workers = RETRY_PASS_WORKERS[pass_num]   if pass_num < len(RETRY_PASS_WORKERS)  else 1

        logging.info(
            "Retry pass %s/%s: %s uncaptured job(s). Waiting %ss then retrying with %s worker(s).",
            pass_num + 1, MAX_RETRY_PASSES, len(pending), delay, workers,
        )
        await asyncio.sleep(delay)

        retry_cfg = replace(
            cfg,
            workers=workers,
            retries=3,
            delay_min=max(cfg.delay_min, 0.5),
            delay_max=max(cfg.delay_max, 2.0),
            fatal_stop=None,  # No fatal stop in retry passes — every job runs to completion
        )

        queue: asyncio.Queue = asyncio.Queue()
        for job in pending:
            await queue.put(job)

        write_lock    = asyncio.Lock()
        counters_lock = asyncio.Lock()
        counters = {
            "total": len(pending),
            "processed": 0,
            "success": 0,
            "failed": 0,
            "brightdata_errors": 0,
        }

        tasks = [
            asyncio.create_task(
                worker(
                    worker_id=i + 1,
                    queue=queue,
                    client=client,
                    cfg=retry_cfg,
                    write_lock=write_lock,
                    counters=counters,
                    counters_lock=counters_lock,
                    session=session,
                )
            )
            for i in range(workers)
        ]

        await queue.join()
        for _ in tasks:
            await queue.put(None)
        await asyncio.gather(*tasks)

        completed_after = read_completed_keys(cfg.raw_jsonl)
        recovered    = sum(1 for job in pending if job.key in completed_after)
        still_pending = len(pending) - recovered
        logging.info(
            "Retry pass %s/%s done: recovered %s, still uncaptured %s.",
            pass_num + 1, MAX_RETRY_PASSES, recovered, still_pending,
        )

        if still_pending == 0:
            logging.info("All jobs captured after %s retry pass(es).", pass_num + 1)
            return

    # Final report after all passes are exhausted.
    completed   = read_completed_keys(cfg.raw_jsonl)
    uncaptured  = [job for job in all_jobs if job.key not in completed]
    if uncaptured:
        logging.error(
            "Could not capture %s date(s) after %s retry pass(es): %s",
            len(uncaptured),
            MAX_RETRY_PASSES,
            ", ".join(job.key for job in uncaptured),
        )
    else:
        logging.info("All %s jobs captured successfully after retry passes.", len(all_jobs))


async def append_jsonl(path: Path, item: dict, lock: asyncio.Lock) -> None:
    """
    Atomic-ish append protected by asyncio lock.
    """
    async with lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def load_rows_from_jsonl(raw_jsonl: Path) -> list[dict]:
    rows = []

    if not raw_jsonl.exists():
        return rows

    with raw_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            for row in record.get("rows", []):
                rows.append(row)

    return rows


def write_final_files(cfg: Config) -> None:
    rows = load_rows_from_jsonl(cfg.raw_jsonl)

    if not rows:
        logging.warning("No successful rows found. Final files not created.")
        return

    df = pd.DataFrame(rows)

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[FINAL_COLUMNS].copy()

    df["Fare Price"] = pd.to_numeric(df["Fare Price"], errors="coerce")

    # De-duplicate after resume/retry.
    df.drop_duplicates(
        subset=[
            "Date Checked",
            "Date of Departure",
            "Origin",
            "Destination",
            "Time of Departure",
            "Fare Class",
        ],
        keep="last",
        inplace=True,
    )

    df.sort_values(
        by=["Origin", "Destination", "Date of Departure", "Time of Departure"],
        inplace=True,
    )

    df.to_csv(cfg.final_csv, index=False)

    with pd.ExcelWriter(cfg.final_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Fare Tracker")

        ok = df[df["Fare Price"].notna()].copy()

        if not ok.empty:
            ok["Route"] = ok["Origin"] + "→" + ok["Destination"]
            pivot = (
                ok.pivot_table(
                    index="Date of Departure",
                    columns="Route",
                    values="Fare Price",
                    aggfunc="min",
                )
                .round(2)
                .sort_index()
            )
            pivot.to_excel(writer, sheet_name="Cheapest By Route")

        summary = (
            df.groupby(["Origin", "Destination"])
            .size()
            .reset_index(name="Count")
        )
        summary.to_excel(writer, index=False, sheet_name="Summary")

    logging.info("Final CSV saved: %s", cfg.final_csv)
    logging.info("Final Excel saved: %s", cfg.final_xlsx)


# ══════════════════════════════════════════════════════
# Brightdata scraping
# ══════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════
# Request building
# ══════════════════════════════════════════════════════

def build_url(job: Job) -> str:
    origin_name = AIRPORT_NAMES.get(job.origin, job.origin)
    dest_name   = AIRPORT_NAMES.get(job.destination, job.destination)
    return (
        f"{BASE_URL}?tripType=ONE_WAY"
        f"&search_departing={origin_name}"
        f"&depPort={job.origin}"
        f"&search_arriving={dest_name}"
        f"&arrPort={job.destination}"
        f"&departureDate={job.departure_date.strftime('%d.%m.%Y')}"
        f"&adult=1&child=0&infant=0"
        f"&lang=en&promoCode=&assist=No"
    )


def build_rows(
    job: Job,
    flights: list[dict],
    date_checked: str,
    time_checked: str,
) -> list[dict]:
    rows = []

    for flight in flights:
        rows.append(
            {
                "Date Checked": date_checked,
                "Time Checked": time_checked,
                "Airline": AIRLINE,
                "Date of Departure": job.departure_date.strftime("%Y-%m-%d"),
                "Time of Departure": flight.get("time", "N/A"),
                "Origin": job.origin,
                "Destination": job.destination,
                "Fare Price": flight.get("price"),
                "Fare Class": flight.get("fare_class", "UNKNOWN"),
                "Source": SOURCE,
            }
        )

    return rows


async def scrape_job_with_brightdata(
    client: BrightDataClient,
    job: Job,
    cfg: Config,
    provider_name: str,
    date_checked: str,
    time_checked: str,
    session=None,
) -> dict:
    url = build_url(job)
    last_error = None

    if stop_requested(cfg):
        return {
            "ok": False,
            "status": "CANCELLED",
            "provider": provider_name,
            "attempt": 0,
            "rows": [],
            "error": "Stop requested",
        }

    attempt_limit = min(cfg.retries, BRIGHTDATA_MAX_ATTEMPTS)

    for attempt in range(1, attempt_limit + 1):
        if stop_requested(cfg):
            return {
                "ok": False,
                "status": "CANCELLED",
                "provider": provider_name,
                "attempt": attempt,
                "rows": [],
                "error": "Stop requested",
            }
        try:
            html = await fetch_brightdata_url(client, url, session=session)
            title = ""

            if not html.strip():
                # Server returned HTTP 200 but an empty body — not a Brightdata
                # outage, just a bad response for this specific request. Retry it.
                last_error = "Empty page returned — will retry"
                if await interruptible_async_sleep(retry_delay_seconds(attempt), lambda: stop_requested(cfg)):
                    break
                continue

            if looks_blocked(title, html):
                last_error = "Blocked/challenge page returned by Brightdata"
                if await interruptible_async_sleep(retry_delay_seconds(attempt), lambda: stop_requested(cfg)):
                    break
                continue

            flights = parse_flights(html)

            if flights:
                rows = build_rows(
                    job=job,
                    flights=flights,
                    date_checked=date_checked,
                    time_checked=time_checked,
                )

                return {
                    "ok": True,
                    "status": "OK",
                    "provider": provider_name,
                    "attempt": attempt,
                    "rows": rows,
                    "error": None,
                }

            last_error = "Parser returned empty despite Brightdata response"

            # Log a preview of the raw HTML so we can diagnose why the parser
            # found nothing — Cloudflare pages, error pages, or layout changes
            # all look like "empty" to the parser.
            logging.warning(
                "Parse empty for %s (attempt %s) — HTML preview: %s",
                job.key,
                attempt,
                html[:800].replace("\n", " ").strip(),
            )

            if await interruptible_async_sleep(retry_delay_seconds(attempt), lambda: stop_requested(cfg)):
                break

        except BrightDataUnavailable as e:
            last_error = str(e)
            _is_timeout = "timed out" in last_error.lower()
            if await interruptible_async_sleep(retry_delay_seconds(attempt, is_timeout=_is_timeout), lambda: stop_requested(cfg)):
                break
        except Exception as e:
            last_error = repr(e)
            if await interruptible_async_sleep(retry_delay_seconds(attempt), lambda: stop_requested(cfg)):
                break

    if stop_requested(cfg):
        return {
            "ok": False,
            "status": "CANCELLED",
            "provider": provider_name,
            "attempt": attempt_limit,
            "rows": [],
            "error": "Stop requested",
        }

    if last_error and (
        "Brightdata" in last_error
        or "HTTP " in last_error
        or "timed out" in last_error.lower()
        or "blocked/challenge" in last_error.lower()
    ):
        raise BrightDataUnavailable(last_error)

    return {
        "ok": False,
        "status": "FAILED",
        "provider": provider_name,
        "attempt": attempt_limit,
        "rows": [],
        "error": last_error,
    }


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    client: BrightDataClient,
    cfg: Config,
    write_lock: asyncio.Lock,
    counters: dict,
    counters_lock: asyncio.Lock,
    session=None,
) -> None:
    date_checked = datetime.now().strftime("%d/%m/%Y")
    time_checked = datetime.now().strftime("%H:%M")

    try:
        while True:
            if stop_requested(cfg):
                # Drain pending jobs so queue.join() can complete.
                while True:
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    else:
                        queue.task_done()
                break

            job = await queue.get()
            try:
                if job is None:
                    break

                if stop_requested(cfg):
                    continue

                logging.info(
                    "[W%s] Start %s -> %s | %s",
                    worker_id,
                    job.origin,
                    job.destination,
                    job.departure_date.strftime("%Y-%m-%d"),
                )

                try:
                    result = await scrape_job_with_brightdata(
                        client=client,
                        job=job,
                        cfg=cfg,
                        provider_name="Brightdata",
                        date_checked=date_checked,
                        time_checked=time_checked,
                        session=session,
                    )
                except BrightDataUnavailable as e:
                    fatal_stop = getattr(cfg, "fatal_stop", None)
                    already_stopping = bool(fatal_stop and fatal_stop.is_set())

                    async with counters_lock:
                        counters["brightdata_errors"] += 1
                        error_count = counters["brightdata_errors"]
                        counters["processed"] += 1
                        counters["failed"] += 1
                        processed = counters["processed"]
                        success = counters["success"]
                        failed = counters["failed"]
                        total = counters["total"]

                    logging.warning("[W%s] Brightdata error for %s: %s", worker_id, job.key, e)

                    # Always record so the retry loop can pick this job up.
                    record = {
                        "job_key": job.key,
                        "origin": job.origin,
                        "destination": job.destination,
                        "departure_date": job.departure_date.strftime("%Y-%m-%d"),
                        "status": "FAILED",
                        "provider": "Brightdata",
                        "attempt": cfg.retries,
                        "rows": [],
                        "error": str(e),
                        "checked_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    await append_jsonl(cfg.error_jsonl, record, write_lock)
                    logging.info(
                        "[W%s] FAILED %s | Progress %s/%s | OK=%s Failed=%s",
                        worker_id, job.key, processed, total, success, failed,
                    )

                    # Only pause the main run if multiple workers are failing (systemic outage).
                    # The retry loop will recover any dates missed during the pause.
                    if error_count >= BRIGHTDATA_FATAL_THRESHOLD and fatal_stop and not already_stopping:
                        logging.warning(
                            BRIGHTDATA_WARNING + " (%s/%s worker failures — pausing main run, retry loop will recover remaining jobs)",
                            error_count, BRIGHTDATA_FATAL_THRESHOLD,
                        )
                        fatal_stop.set()
                        if cfg.progress_callback:
                            cfg.progress_callback(0, counters["total"], BRIGHTDATA_WARNING)
                    continue

                record = {
                    "job_key": job.key,
                    "origin": job.origin,
                    "destination": job.destination,
                    "departure_date": job.departure_date.strftime("%Y-%m-%d"),
                    "status": result["status"],
                    "provider": result["provider"],
                    "attempt": result["attempt"],
                    "rows": result["rows"],
                    "error": result["error"],
                    "checked_at": datetime.now().isoformat(timespec="seconds"),
                }

                if result["ok"]:
                    await append_jsonl(cfg.raw_jsonl, record, write_lock)
                    status_label = "DONE"
                else:
                    await append_jsonl(cfg.error_jsonl, record, write_lock)
                    status_label = result["status"]

                async with counters_lock:
                    counters["processed"] += 1
                    if result["ok"]:
                        counters["success"] += 1
                    else:
                        counters["failed"] += 1

                    processed = counters["processed"]
                    success = counters["success"]
                    failed = counters["failed"]
                    total = counters["total"]

                logging.info(
                    "[W%s] %s %s | Progress %s/%s | OK=%s Failed=%s",
                    worker_id,
                    status_label,
                    job.key,
                    processed,
                    total,
                    success,
                    failed,
                )
                if cfg.progress_callback and not stop_requested(cfg):
                    cfg.progress_callback(
                        processed,
                        total,
                        f"Fast Airnorth {job.origin}->{job.destination} {job.departure_date.strftime('%Y-%m-%d')} {status_label}",
                    )

                if cfg.delay_max > 0 and not stop_requested(cfg):
                    await asyncio.sleep(random.uniform(cfg.delay_min, cfg.delay_max))
            finally:
                queue.task_done()

    except Exception as e:
        logging.exception("[W%s] Worker crashed: %s", worker_id, e)

    finally:
        pass

# ══════════════════════════════════════════════════════
# Brightdata startup
# ══════════════════════════════════════════════════════

async def open_brightdata_client(cfg: Config, session=None) -> Optional[BrightDataClient]:
    try:
        client = get_brightdata_client()
        logging.info("Brightdata zone: %s", client.zone)
        if client.country:
            logging.info("Brightdata country override: %s", client.country)
        await verify_brightdata(client, cfg, session=session)
        return client
    except BrightDataUnavailable as e:
        logging.warning(BRIGHTDATA_WARNING)
        logging.warning("Brightdata error: %s", e)
        return None


# ══════════════════════════════════════════════════════
# CLI / setup
# ══════════════════════════════════════════════════════

def parse_route(value: str) -> tuple[str, str]:
    value = value.strip().upper().replace("→", "-")

    if "-" not in value:
        raise argparse.ArgumentTypeError("Route must look like BME-KNX")

    origin, destination = value.split("-", 1)
    route = (origin.strip(), destination.strip())

    if route not in ROUTES:
        valid = ", ".join([f"{o}-{d}" for o, d in ROUTES])
        raise argparse.ArgumentTypeError(f"Invalid route. Valid routes: {valid}")

    return route


def interactive_route_select() -> list[tuple[str, str]]:
    print(f"\n{'═' * 60}")
    print("  Airnorth Fare Tracker")
    print("  Select route(s):")

    for i, (origin, destination) in enumerate(ROUTES, 1):
        print(f"    {i}. {origin} → {destination}")

    print(f"    {len(ROUTES) + 1}. All routes\n")

    while True:
        try:
            choice = int(input(f"  Enter choice (1-{len(ROUTES) + 1}): ").strip())

            if 1 <= choice <= len(ROUTES):
                return [ROUTES[choice - 1]]

            if choice == len(ROUTES) + 1:
                return list(ROUTES)

        except Exception:
            pass

        print("  Invalid choice. Try again.")


def build_config() -> Config:
    parser = argparse.ArgumentParser(description="Fast async Airnorth fare tracker")

    parser.add_argument("--all", action="store_true", help="Scrape all routes")
    parser.add_argument(
        "--route",
        type=parse_route,
        action="append",
        help="Route like BME-KNX. Can be used multiple times.",
    )
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS_OUT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--goto-timeout-ms", type=int, default=40000)
    parser.add_argument("--selector-timeout-ms", type=int, default=15000)
    parser.add_argument("--cloudflare-timeout-s", type=int, default=25)
    parser.add_argument("--delay-min", type=float, default=1.5)
    parser.add_argument("--delay-max", type=float, default=2.0)
    parser.add_argument("--no-block-assets", action="store_true")
    parser.add_argument(
        "--resume-dir",
        type=str,
        default=None,
        help="Existing output run folder to resume, e.g. output/airnorth_20260506_143000",
    )

    args = parser.parse_args()

    if args.all:
        selected_routes = list(ROUTES)
    elif args.route:
        selected_routes = args.route
    else:
        selected_routes = interactive_route_select()

    if args.resume_dir:
        run_dir = Path(args.resume_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        run_id = run_dir.name.replace("airnorth_", "")
    else:
        run_id = make_run_id()
        run_dir = OUTPUT_ROOT / f"airnorth_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

    output_stamp = make_output_stamp(run_id)
    raw_jsonl = run_dir / f"airnorth_raw_{run_id}.jsonl"
    error_jsonl = run_dir / f"airnorth_errors_{run_id}.jsonl"
    final_csv = run_dir / f"Fare_Tracker_Airnorth_{output_stamp}.csv"
    final_xlsx = run_dir / f"Fare_Tracker_Airnorth_{output_stamp}.xlsx"

    workers = max(1, args.workers)

    # Safety cap: Web Unlocker supports concurrency, but runaway parallelism can
    # still hurt reliability/cost. Override with AIRNORTH_WORKER_SAFETY_CAP.
    if workers > WORKER_SAFETY_CAP:
        print(f"Workers capped at {WORKER_SAFETY_CAP} for reliability.")
        workers = WORKER_SAFETY_CAP

    return Config(
        days_out=args.days,
        workers=workers,
        retries=max(1, min(BRIGHTDATA_MAX_ATTEMPTS, args.retries)),
        goto_timeout_ms=args.goto_timeout_ms,
        selector_timeout_ms=args.selector_timeout_ms,
        cloudflare_timeout_s=args.cloudflare_timeout_s,
        delay_min=max(0, args.delay_min),
        delay_max=max(0, args.delay_max),
        block_assets=not args.no_block_assets,
        selected_routes=selected_routes,
        run_dir=run_dir,
        raw_jsonl=raw_jsonl,
        error_jsonl=error_jsonl,
        final_csv=final_csv,
        final_xlsx=final_xlsx,
        progress_callback=None,
    )


def create_jobs(cfg: Config) -> list[Job]:
    today = datetime.now()

    jobs = []

    for origin, destination in cfg.selected_routes:
        for i in range(cfg.days_out):
            jobs.append(
                Job(
                    origin=origin,
                    destination=destination,
                    departure_date=today + timedelta(days=i),
                )
            )

    return jobs


# ══════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════

def make_aiohttp_session(cfg: Config):
    if aiohttp is None:
        logging.warning("aiohttp not installed. Falling back to urllib thread requests.")
        return None

    timeout = aiohttp.ClientTimeout(total=BRIGHTDATA_REQUEST_TIMEOUT_S)
    connector = aiohttp.TCPConnector(
        limit=max(cfg.workers * 2, cfg.workers, 1),
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )
    return aiohttp.ClientSession(timeout=timeout, connector=connector)


async def run_config(cfg: Config) -> None:
    setup_logging(cfg.run_dir)

    logging.info("Run folder: %s", cfg.run_dir)
    logging.info("Routes: %s", ", ".join([f"{o}-{d}" for o, d in cfg.selected_routes]))
    logging.info("Days out: %s", cfg.days_out)
    logging.info("Workers: %s", cfg.workers)
    logging.info("Retries: %s", cfg.retries)
    logging.info("Brightdata max attempts: %s", BRIGHTDATA_MAX_ATTEMPTS)
    logging.info("Asset blocking enabled: %s", cfg.block_assets)

    jobs = create_jobs(cfg)
    completed_keys = read_completed_keys(cfg.raw_jsonl)

    pending_jobs = [job for job in jobs if job.key not in completed_keys]

    logging.info("Total jobs: %s", len(jobs))
    logging.info("Already completed: %s", len(completed_keys))
    logging.info("Pending jobs: %s", len(pending_jobs))

    if not pending_jobs:
        logging.info("Nothing pending. Building final files from existing raw JSONL.")
        write_final_files(cfg)
        return

    queue: asyncio.Queue = asyncio.Queue()

    for job in pending_jobs:
        await queue.put(job)

    write_lock = asyncio.Lock()
    counters_lock = asyncio.Lock()

    counters = {
        "total": len(pending_jobs),
        "processed": 0,
        "success": 0,
        "failed": 0,
        "brightdata_errors": 0,
    }

    if cfg.fatal_stop is None:
        cfg = replace(cfg, fatal_stop=asyncio.Event())

    session = make_aiohttp_session(cfg)
    try:
        client = await open_brightdata_client(cfg, session=session)
        if not client:
            if cfg.progress_callback:
                cfg.progress_callback(0, len(pending_jobs), BRIGHTDATA_WARNING)
            return

        tasks = [
            asyncio.create_task(
                worker(
                    worker_id=i + 1,
                    queue=queue,
                    client=client,
                    cfg=cfg,
                    write_lock=write_lock,
                    counters=counters,
                    counters_lock=counters_lock,
                    session=session,
                )
            )
            for i in range(cfg.workers)
        ]

        await queue.join()

        for _ in tasks:
            await queue.put(None)

        await asyncio.gather(*tasks)

        # Always run the retry loop — it compares against the full job list so
        # CANCELLED, FAILED, and never-recorded dates are all recovered.
        await retry_failed_jobs(cfg, client, all_jobs=jobs, session=session)
    finally:
        if session:
            await session.close()

    write_final_files(cfg)

    logging.info("Completed.")
    logging.info("Output folder: %s", cfg.run_dir)


async def scrape_airnorth_fast(
    selected_routes=None,
    days_out=DEFAULT_DAYS_OUT,
    workers=8,
    progress_callback=None,
    stop_requested=None,
) -> dict:
    run_id = make_run_id()
    output_stamp = make_output_stamp(run_id)
    run_dir = OUTPUT_ROOT / f"airnorth_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    cfg = Config(
        days_out=days_out,
        workers=max(1, min(WORKER_SAFETY_CAP, workers)),
        retries=3,
        goto_timeout_ms=40000,
        selector_timeout_ms=15000,
        cloudflare_timeout_s=25,
        delay_min=1.5,
        delay_max=2.0,
        block_assets=True,
        selected_routes=selected_routes or list(ROUTES),
        run_dir=run_dir,
        raw_jsonl=run_dir / f"airnorth_raw_{run_id}.jsonl",
        error_jsonl=run_dir / f"airnorth_errors_{run_id}.jsonl",
        final_csv=run_dir / f"Fare_Tracker_Airnorth_{output_stamp}.csv",
        final_xlsx=run_dir / f"Fare_Tracker_Airnorth_{output_stamp}.xlsx",
        progress_callback=progress_callback,
        stop_requested=stop_requested,
    )

    await run_config(cfg)
    return {
        "rows": load_rows_from_jsonl(cfg.raw_jsonl),
        "csv_path": str(cfg.final_csv) if cfg.final_csv.exists() else None,
        "xlsx_path": str(cfg.final_xlsx) if cfg.final_xlsx.exists() else None,
        "run_dir": str(cfg.run_dir),
    }


async def async_main() -> None:
    cfg = build_config()
    await run_config(cfg)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nStopped by user. You can resume using --resume-dir with the output folder.")
    except Exception as e:
        logging.exception("Fatal error: %s", e)
        raise


if __name__ == "__main__":
    main()
