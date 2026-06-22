"""
Airnorth Fare Tracker — Async Playwright Version
================================================
Fast, reliable, non-blocking scraper with:

- async_playwright
- queue workers
- retries
- Oxylabs primary CDP
- Bright Data fallback support
- JSONL crash-safe checkpointing
- resume support
- final CSV + Excel export

Install:
    pip install playwright beautifulsoup4 pandas openpyxl python-dotenv
    playwright install chromium

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
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout


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
    ("KNX", "BME"),
]

AIRLINE = "Airnorth"
SOURCE = "airnorth.com.au"
BASE_URL = "https://secure.airnorth.com.au/ibe/availability"
HOMEPAGE_URL = "https://www.airnorth.com.au/"
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
    "Status",
    "Provider",
    "Attempt",
]

READY_SELECTOR = ".js-scheduled-flight, .no-flights, [class*='no-flight']"

BLOCKED_RESOURCE_TYPES = {"image", "font", "media"}

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
    use_fallback: bool
    selected_routes: list[tuple[str, str]]
    run_dir: Path
    raw_jsonl: Path
    error_jsonl: Path
    final_csv: Path
    final_xlsx: Path
    progress_callback: object = None
    stop_requested: object = None


@dataclass(frozen=True)
class Job:
    origin: str
    destination: str
    departure_date: datetime

    @property
    def key(self) -> str:
        return f"{self.origin}-{self.destination}-{self.departure_date.strftime('%Y-%m-%d')}"


@dataclass
class BrowserBundle:
    name: str
    browser: object


def stop_requested(cfg: Config) -> bool:
    callback = cfg.stop_requested
    if not callback:
        return False
    try:
        return bool(callback())
    except Exception:
        return False


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
    )


# ══════════════════════════════════════════════════════
# Env / proxy helpers
# ══════════════════════════════════════════════════════

def build_oxylabs_cdp_url() -> str:
    """
    Builds Oxylabs CDP URL from env variables.

    Required:
        OXY_USER
        OXY_PASS
        OXY_ENDPOINT
    """
    oxy_user = os.getenv("OXY_USER", "").strip()
    oxy_pass = os.getenv("OXY_PASS", "").strip()
    oxy_endpoint = os.getenv("OXY_ENDPOINT", "ubc.oxylabs.io").strip()

    if not oxy_user or not oxy_pass:
        raise RuntimeError(
            "Missing Oxylabs credentials. Add OXY_USER and OXY_PASS in .env"
        )

    safe_user = quote(oxy_user, safe="")
    safe_pass = quote(oxy_pass, safe="")

    return f"wss://{safe_user}:{safe_pass}@{oxy_endpoint}"


def get_bright_proxy_config() -> Optional[dict]:
    """
    Optional Bright Data residential proxy fallback.

    .env example:
        BRIGHT_PROXY_SERVER=http://brd.superproxy.io:22225
        BRIGHT_PROXY_USERNAME=...
        BRIGHT_PROXY_PASSWORD=...
    """
    server = os.getenv("BRIGHT_PROXY_SERVER", "").strip()
    username = os.getenv("BRIGHT_PROXY_USERNAME", "").strip()
    password = os.getenv("BRIGHT_PROXY_PASSWORD", "").strip()

    if not server:
        return None

    if not server.startswith(("http://", "https://", "socks5://")):
        server = "http://" + server

    proxy = {"server": server}

    if username and password:
        proxy["username"] = username
        proxy["password"] = password

    return proxy


def get_bright_cdp_url() -> Optional[str]:
    """
    Optional Bright Data Browser/CDP fallback.
    """
    value = os.getenv("BRIGHT_CDP_URL", "").strip()
    return value or None


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
            "Provider",
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
            df.groupby(["Origin", "Destination", "Status"])
            .size()
            .reset_index(name="Count")
        )
        summary.to_excel(writer, index=False, sheet_name="Summary")

    logging.info("Final CSV saved: %s", cfg.final_csv)
    logging.info("Final Excel saved: %s", cfg.final_xlsx)


# ══════════════════════════════════════════════════════
# Playwright helpers
# ══════════════════════════════════════════════════════

async def block_unneeded_assets(route) -> None:
    """
    Speed optimization: block images, fonts, media.
    Do not block JS/CSS because the booking page may need them.
    """
    if route.request.resource_type in BLOCKED_RESOURCE_TYPES:
        await route.abort()
    else:
        await route.continue_()


async def create_page(bundle: BrowserBundle, cfg: Config):
    context = await bundle.browser.new_context(
        viewport={"width": 1366, "height": 768},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        locale="en-AU",
        timezone_id="Australia/Darwin",
    )

    context.set_default_timeout(cfg.selector_timeout_ms)
    context.set_default_navigation_timeout(cfg.goto_timeout_ms)

    if cfg.block_assets:
        await context.route("**/*", block_unneeded_assets)

    page = await context.new_page()
    return context, page


async def warmup_page(page, provider_name: str, cfg: Config) -> None:
    try:
        await page.goto(
            HOMEPAGE_URL,
            wait_until="domcontentloaded",
            timeout=cfg.goto_timeout_ms,
        )
        await wait_until_not_blocked(page, cfg.cloudflare_timeout_s)
        await asyncio.sleep(random.uniform(0.5, 1.5))
        logging.info("[%s] Warmup complete", provider_name)
    except Exception as e:
        logging.warning("[%s] Warmup warning: %s", provider_name, e)


async def wait_until_not_blocked(page, timeout_s: int) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout_s

    while asyncio.get_running_loop().time() < deadline:
        try:
            title = await page.title()
            html = await page.content()

            if not looks_blocked(title, html):
                return True

        except Exception:
            pass

        await asyncio.sleep(1)

    return False


async def wait_for_result_content(page, cfg: Config) -> bool:
    """
    Waits until flight content/no-flight content appears.
    Returns True if content likely loaded, False if timed out.
    """
    try:
        await page.wait_for_selector(
            READY_SELECTOR,
            timeout=cfg.selector_timeout_ms,
        )
        return True
    except PWTimeout:
        html = await page.content()
        lower_html = html.lower()

        if ".js-scheduled-flight" in lower_html:
            return True

        if any(marker in lower_html for marker in NO_FLIGHT_MARKERS):
            return True

        return False


# ══════════════════════════════════════════════════════
# Scraping
# ══════════════════════════════════════════════════════

def build_url(job: Job) -> str:
    return (
        f"{BASE_URL}?tripType=ONE_WAY"
        f"&depPort={job.origin}"
        f"&arrPort={job.destination}"
        f"&departureDate={job.departure_date.strftime('%d.%m.%Y')}"
        f"&adult=1&child=0&infant=0"
    )


def build_rows(
    job: Job,
    flights: list[dict],
    status: str,
    provider: str,
    attempt: int,
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
                "Status": status,
                "Provider": provider,
                "Attempt": attempt,
            }
        )

    return rows


async def scrape_job_with_page(
    page,
    job: Job,
    cfg: Config,
    provider_name: str,
    date_checked: str,
    time_checked: str,
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

    for attempt in range(1, cfg.retries + 1):
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
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=cfg.goto_timeout_ms,
            )

            not_blocked = await wait_until_not_blocked(
                page,
                timeout_s=cfg.cloudflare_timeout_s,
            )

            if not not_blocked:
                last_error = "Challenge/block page did not clear"
                await page.goto("about:blank")
                if stop_requested(cfg):
                    break
                await asyncio.sleep(min(10, attempt * 2))
                continue

            content_loaded = await wait_for_result_content(page, cfg)
            html = await page.content()
            title = await page.title()

            if looks_blocked(title, html):
                last_error = "Blocked/challenge page detected after content wait"
                await page.goto("about:blank")
                if stop_requested(cfg):
                    break
                await asyncio.sleep(min(10, attempt * 2))
                continue

            flights = parse_flights(html)

            if flights:
                rows = build_rows(
                    job=job,
                    flights=flights,
                    status="OK",
                    provider=provider_name,
                    attempt=attempt,
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

            if not content_loaded:
                last_error = "Selector/content timeout and parser returned empty"
            else:
                last_error = "Parser returned empty despite page load"

            await page.goto("about:blank")
            if stop_requested(cfg):
                break
            await asyncio.sleep(min(10, attempt * 2))

        except Exception as e:
            last_error = repr(e)

            try:
                await page.goto("about:blank")
            except Exception:
                pass

            if stop_requested(cfg):
                break
            await asyncio.sleep(min(10, attempt * 2))

    if stop_requested(cfg):
        return {
            "ok": False,
            "status": "CANCELLED",
            "provider": provider_name,
            "attempt": cfg.retries,
            "rows": [],
            "error": "Stop requested",
        }

    return {
        "ok": False,
        "status": "FAILED",
        "provider": provider_name,
        "attempt": cfg.retries,
        "rows": [],
        "error": last_error,
    }


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    primary: BrowserBundle,
    fallback: Optional[BrowserBundle],
    cfg: Config,
    write_lock: asyncio.Lock,
    counters: dict,
    counters_lock: asyncio.Lock,
) -> None:
    primary_context = None
    primary_page = None
    fallback_context = None
    fallback_page = None

    date_checked = datetime.now().strftime("%d/%m/%Y")
    time_checked = datetime.now().strftime("%H:%M")

    try:
        primary_context, primary_page = await create_page(primary, cfg)
        await warmup_page(primary_page, f"{primary.name}/W{worker_id}", cfg)

        if fallback and cfg.use_fallback:
            fallback_context, fallback_page = await create_page(fallback, cfg)
            await warmup_page(fallback_page, f"{fallback.name}/W{worker_id}", cfg)

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

                result = await scrape_job_with_page(
                    page=primary_page,
                    job=job,
                    cfg=cfg,
                    provider_name=primary.name,
                    date_checked=date_checked,
                    time_checked=time_checked,
                )

                if (
                    not result["ok"]
                    and result["status"] != "CANCELLED"
                    and fallback_page
                    and cfg.use_fallback
                    and not stop_requested(cfg)
                ):
                    logging.warning(
                        "[W%s] Primary failed for %s. Trying fallback. Error: %s",
                        worker_id,
                        job.key,
                        result["error"],
                    )

                    result = await scrape_job_with_page(
                        page=fallback_page,
                        job=job,
                        cfg=cfg,
                        provider_name=fallback.name,
                        date_checked=date_checked,
                        time_checked=time_checked,
                    )

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
        if primary_context:
            try:
                await primary_context.close()
            except Exception:
                pass

        if fallback_context:
            try:
                await fallback_context.close()
            except Exception:
                pass

# ══════════════════════════════════════════════════════
# Browser startup
# ══════════════════════════════════════════════════════

async def connect_primary_browser(playwright) -> BrowserBundle:
    oxy_cdp_url = build_oxylabs_cdp_url()
    browser = await playwright.chromium.connect_over_cdp(
        oxy_cdp_url,
        timeout=60000,
    )
    return BrowserBundle(name="Oxylabs", browser=browser)


async def connect_fallback_browser(playwright) -> Optional[BrowserBundle]:
    bright_cdp_url = get_bright_cdp_url()

    if bright_cdp_url:
        browser = await playwright.chromium.connect_over_cdp(
            bright_cdp_url,
            timeout=60000,
        )
        return BrowserBundle(name="BrightData-CDP", browser=browser)

    bright_proxy = get_bright_proxy_config()

    if bright_proxy:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=bright_proxy,
        )
        return BrowserBundle(name="BrightData-Proxy", browser=browser)

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
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--goto-timeout-ms", type=int, default=40000)
    parser.add_argument("--selector-timeout-ms", type=int, default=15000)
    parser.add_argument("--cloudflare-timeout-s", type=int, default=25)
    parser.add_argument("--delay-min", type=float, default=0.4)
    parser.add_argument("--delay-max", type=float, default=1.2)
    parser.add_argument("--no-block-assets", action="store_true")
    parser.add_argument("--no-fallback", action="store_true")
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
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = OUTPUT_ROOT / f"airnorth_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

    raw_jsonl = run_dir / f"airnorth_raw_{run_id}.jsonl"
    error_jsonl = run_dir / f"airnorth_errors_{run_id}.jsonl"
    final_csv = run_dir / f"Fare_Tracker_Airnorth_{run_id}.csv"
    final_xlsx = run_dir / f"Fare_Tracker_Airnorth_{run_id}.xlsx"

    workers = max(1, args.workers)

    # Safety cap: too many parallel browser contexts often reduces reliability.
    if workers > 10:
        print("Workers capped at 10 for reliability.")
        workers = 10

    return Config(
        days_out=args.days,
        workers=workers,
        retries=max(1, args.retries),
        goto_timeout_ms=args.goto_timeout_ms,
        selector_timeout_ms=args.selector_timeout_ms,
        cloudflare_timeout_s=args.cloudflare_timeout_s,
        delay_min=max(0, args.delay_min),
        delay_max=max(0, args.delay_max),
        block_assets=not args.no_block_assets,
        use_fallback=not args.no_fallback,
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

async def run_config(cfg: Config) -> None:
    setup_logging(cfg.run_dir)

    logging.info("Run folder: %s", cfg.run_dir)
    logging.info("Routes: %s", ", ".join([f"{o}-{d}" for o, d in cfg.selected_routes]))
    logging.info("Days out: %s", cfg.days_out)
    logging.info("Workers: %s", cfg.workers)
    logging.info("Retries: %s", cfg.retries)
    logging.info("Fallback enabled: %s", cfg.use_fallback)
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
    }

    primary = None
    fallback = None

    async with async_playwright() as playwright:
        try:
            primary = await connect_primary_browser(playwright)
            logging.info("Primary browser connected: %s", primary.name)

            if cfg.use_fallback:
                fallback = await connect_fallback_browser(playwright)

                if fallback:
                    logging.info("Fallback browser connected: %s", fallback.name)
                else:
                    logging.info("No fallback configured. Continuing primary-only.")

            tasks = [
                asyncio.create_task(
                    worker(
                        worker_id=i + 1,
                        queue=queue,
                        primary=primary,
                        fallback=fallback,
                        cfg=cfg,
                        write_lock=write_lock,
                        counters=counters,
                        counters_lock=counters_lock,
                    )
                )
                for i in range(cfg.workers)
            ]

            await queue.join()

            for _ in tasks:
                await queue.put(None)

            await asyncio.gather(*tasks)

        finally:
            if primary:
                try:
                    await primary.browser.close()
                except Exception:
                    pass

            if fallback:
                try:
                    await fallback.browser.close()
                except Exception:
                    pass

    write_final_files(cfg)

    logging.info("Completed.")
    logging.info("Output folder: %s", cfg.run_dir)


async def scrape_airnorth_fast(
    selected_routes=None,
    days_out=DEFAULT_DAYS_OUT,
    workers=4,
    progress_callback=None,
    stop_requested=None,
) -> dict:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = OUTPUT_ROOT / f"airnorth_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    cfg = Config(
        days_out=days_out,
        workers=max(1, min(10, workers)),
        retries=3,
        goto_timeout_ms=40000,
        selector_timeout_ms=15000,
        cloudflare_timeout_s=25,
        delay_min=0.4,
        delay_max=1.2,
        block_assets=True,
        use_fallback=True,
        selected_routes=selected_routes or list(ROUTES),
        run_dir=run_dir,
        raw_jsonl=run_dir / f"airnorth_raw_{run_id}.jsonl",
        error_jsonl=run_dir / f"airnorth_errors_{run_id}.jsonl",
        final_csv=run_dir / f"Fare_Tracker_Airnorth_{run_id}.csv",
        final_xlsx=run_dir / f"Fare_Tracker_Airnorth_{run_id}.xlsx",
        progress_callback=progress_callback,
        stop_requested=stop_requested,
    )

    await run_config(cfg)
    return {
        "rows": load_rows_from_jsonl(cfg.raw_jsonl),
        "csv_path": str(cfg.final_csv),
        "xlsx_path": str(cfg.final_xlsx),
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

