"""
Rex Airlines Flight Scraper — TIME FIX v2
==========================================
FIX 1: normalise_time() ab sirf raw time return karta hai — koi conversion nahi.
        Website pe jo time hai (9:50am, 1:35pm) wahi Excel mein jayega.

FIX 2: Same time bug fixed — har ZL number ke BAAD ka PEHLA time lega.
        Pehle ZL ke aas-paas (pehle + baad) dono side scan hoti thi,
        jis se dono flights ko same time milta tha.
        Ab sirf ZL ke BAAD wali window scan hogi departure time ke liye.

Routes (confirmed):
  PER ↔ ALH  PER ↔ EPR  PER ↔ CVQ
  PER ↔ MJK  CVQ ↔ MJK
"""

import asyncio
import os
import re
import sys
import time
import argparse
import json
import random
import requests
import tempfile
import traceback
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from playwright.async_api import async_playwright, Browser
from openpyxl import load_workbook, Workbook

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ORIGINAL_STDOUT = sys.stdout
ORIGINAL_STDERR = sys.stderr

# ─────────────────────────────────────────────────────────────
#  BRIGHT DATA CREDENTIALS
# ─────────────────────────────────────────────────────────────
BD_BROWSER_HOST = os.getenv("BD_BROWSER_HOST", "brd.superproxy.io")
BD_BROWSER_PORT = os.getenv("BD_BROWSER_PORT", "9222")
BD_BROWSER_SELENIUM_PORT = os.getenv("BD_BROWSER_SELENIUM_PORT", "9515")
BD_BROWSER_USER = os.getenv(
    "BD_BROWSER_USER",
    "brd-customer-hl_fbc4a16a-zone-cont_rex",
)
BD_BROWSER_PASS = os.getenv("BD_BROWSER_PASS", "072res2p22t3")
BD_AUTH_TOKEN = os.getenv(
    "BD_AUTH_TOKEN",
    "7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7",
)
BD_WEB_UNLOCKER_ZONE = os.getenv("BD_WEB_UNLOCKER_ZONE", "cron_rex")
BD_UNLOCKER_COUNTRY = os.getenv("BD_UNLOCKER_COUNTRY", "au")
BD_UNLOCKER_ENDPOINT = os.getenv(
    "BD_UNLOCKER_ENDPOINT",
    "https://api.brightdata.com/request",
)
BD_BROWSER_WSS = os.getenv(
    "BD_BROWSER_WSS",
    f"wss://{BD_BROWSER_USER}:{BD_BROWSER_PASS}@{BD_BROWSER_HOST}:{BD_BROWSER_PORT}",
)
BD_BROWSER_SELENIUM_URL = os.getenv(
    "BD_BROWSER_SELENIUM_URL",
    f"https://{BD_BROWSER_USER}:{BD_BROWSER_PASS}@{BD_BROWSER_HOST}:{BD_BROWSER_SELENIUM_PORT}",
)

SENSITIVE_BRIGHTDATA_URL_RE = re.compile(
    r"(?:wss|https)://[^\s'\"<>]+@brd\.superproxy\.io:\d+/?"
)


def redact_sensitive_text(text: str) -> str:
    return SENSITIVE_BRIGHTDATA_URL_RE.sub(
        "brightdata://<redacted>@brd.superproxy.io", str(text)
    )

# Empty-result warning counter. Default is intentionally high so 84-day runs
# keep recording every date even when many dates have no flights.
MAX_EMPTY_STREAK = int(os.getenv("REX_MAX_EMPTY_STREAK", "999999"))
REX_TIMEZONE = os.getenv("REX_TIMEZONE", "Australia/Perth")
REX_LOCALE = os.getenv("REX_LOCALE", "en-AU")
try:
    REX_TZ = ZoneInfo(REX_TIMEZONE)
except ZoneInfoNotFoundError:
    print(f"⚠️  Invalid REX_TIMEZONE={REX_TIMEZONE!r}; using Australia/Perth")
    REX_TIMEZONE = "Australia/Perth"
    REX_TZ = ZoneInfo(REX_TIMEZONE)


def web_unlocker_get(url: str) -> str:
    """Bright Data Web Unlocker API se HTML fetch karo."""
    payload = {"zone": BD_WEB_UNLOCKER_ZONE, "url": url, "format": "raw"}
    if BD_UNLOCKER_COUNTRY:
        payload["country"] = BD_UNLOCKER_COUNTRY

    resp = requests.post(
        BD_UNLOCKER_ENDPOINT,
        json=payload,
        headers={"Authorization": f"Bearer {BD_AUTH_TOKEN}",
                 "Content-Type": "application/json"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.text


def check_web_unlocker() -> bool:
    """Run Bright Data's lightweight Web Unlocker test request."""
    test_url = "https://geo.brdtest.com/welcome.txt?product=unlocker&method=api"
    try:
        text = web_unlocker_get(test_url).strip()
        print(f"✅ Web Unblocker check OK: {text[:180]}")
        return True
    except Exception as exc:
        print(f"⚠️  Web Unblocker check failed: {exc}")
        return False

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────

AIRPORT_MAP = {
    "ALH": "Albany",
    "PER": "Perth",
    "EPR": "Esperance",
    "CVQ": "Carnarvon",
    "MJK": "Monkey Mia",
}

ALL_ROUTES = [
    ("PER", "ALH"), ("ALH", "PER"),
    ("PER", "EPR"), ("EPR", "PER"),
    ("PER", "CVQ"), ("CVQ", "PER"),
    ("CVQ", "MJK"), ("MJK", "CVQ"),
]

CONNECTING_ROUTES = {("CVQ", "MJK"), ("MJK", "CVQ")}

# Yeh routes Rex pe ribbon nahi dikhate — hamesha fresh search karenge
NO_RIBBON_ROUTES = set()

TOTAL_DAYS = int(os.getenv("REX_TOTAL_DAYS", "84"))
START_OFFSET_DAYS = int(os.getenv("REX_START_OFFSET_DAYS", "1"))
OUTPUT_EXCEL = os.getenv("REX_OUTPUT_EXCEL", "rex_results_all_routes.xlsx")
DEBUG_DIR = os.getenv("REX_DEBUG_DIR", "rex_debug")
LOG_DIR = os.getenv("REX_LOG_DIR", "rex_logs")
RUN_ID = os.getenv("REX_RUN_ID", datetime.now(REX_TZ).strftime("%Y%m%d"))

MAX_ATTEMPTS = int(os.getenv("REX_MAX_ATTEMPTS", "3"))
FINAL_RETRY_ROUNDS = int(os.getenv("REX_FINAL_RETRY_ROUNDS", "1"))
RETRY_BACKOFF_SECONDS = float(os.getenv("REX_RETRY_BACKOFF_SECONDS", "8"))
# Pause (seconds) between routes so Bright Data can rotate to a fresh IP.
# A fresh IP avoids hitting Rex's reCAPTCHA quota that was exhausted by the previous route.
INTER_ROUTE_DELAY_SECONDS = int(os.getenv("REX_INTER_ROUTE_DELAY_SECONDS", "30"))
MAX_RETRY_BACKOFF_SECONDS = float(os.getenv("REX_MAX_RETRY_BACKOFF_SECONDS", "90"))
JOB_TIMEOUT_SECONDS = int(os.getenv("REX_JOB_TIMEOUT_SECONDS", "240"))
NAVIGATION_MODE = os.getenv("REX_NAVIGATION_MODE", "ribbon").lower()
RESUME_ENABLED = os.getenv("REX_RESUME", "1").lower() not in {"0", "false", "no"}
CAPTCHA_DISABLED_LOOP_THRESHOLD = int(os.getenv("REX_CAPTCHA_DISABLED_LOOP_THRESHOLD", "3"))
CAPTCHA_RECOVERY_MAX = int(os.getenv("REX_CAPTCHA_RECOVERY_MAX", "2"))
CAPTCHA_RECOVERY_STRATEGY = os.getenv("REX_CAPTCHA_RECOVERY_STRATEGY", "refill-then-new-context").lower()
CAPTCHA_RECOVERY_STRATEGIES = {
    "none",
    "reload",
    "refill",
    "new-context",
    "reload-then-refill",
    "refill-then-new-context",
}

CSV_FIELDS = [
    "Date Checked", "Time Checked", "Airline",
    "Date of Departure", "Time of Departure",
    "Origin", "Destination",
    "Fare Price", "Fare Class", "Source",
    "Run ID", "Status", "Comment", "Retry Count", "Debug Artifacts",
]

STATUS_SUCCESS = "SUCCESS"
STATUS_NO_FARE = "NO_FARE_AVAILABLE"
STATUS_FAILED = "FAILED_AFTER_RETRIES"
STATUS_SITE_UNAVAILABLE = "SITE_UNAVAILABLE"
STATUS_STRUCTURE = "POSSIBLE_WEBSITE_STRUCTURE_ISSUE"
STATUS_STRUCTURE_VISIBLE = "RESULT_STRUCTURE_VISIBLE"
STATUS_TIMEOUT = "PAGE_TIMEOUT"
STATUS_BLOCKED = "BLOCKED_OR_VERIFICATION_REQUIRED"
STATUS_ROUTE_SETUP_FAILED = "ROUTE_SETUP_FAILED"
COMPLETED_STATUSES = {STATUS_SUCCESS, STATUS_NO_FARE}
RETRYABLE_FAILURE_STATUSES = {
    STATUS_FAILED,
    STATUS_SITE_UNAVAILABLE,
    STATUS_STRUCTURE,
    STATUS_TIMEOUT,
    STATUS_BLOCKED,
    STATUS_ROUTE_SETUP_FAILED,
}

RIBBON_SELECTORS = [
    ".calendar .day",
    ".date-tab",
    ".ribbon-date",
    ".calendar-day",
]

CARD_SELECTORS = [
    ".trip-select .trip",
    ".flight-select-row",
    ".avail-flight-row",
    ".flight-option",
    "div.trip",
    "li.trip",
    ".avail-row",
    "tr.flight-row",
]

FLIGHT_LIST_SELECTORS = [
    ".trip-select",
    ".flight-results",
    ".avail-flights",
    ".departing-block",
    ".flight-list",
    "#flightResults",
]


# ─────────────────────────────────────────────────────────────
#  UTILITIES
# ─────────────────────────────────────────────────────────────

def rex_now() -> datetime:
    """Current date/time in the configured Rex timezone."""
    return datetime.now(REX_TZ).replace(tzinfo=None)


def today_dt() -> datetime:
    return rex_now().replace(hour=0, minute=0, second=0, microsecond=0)


def build_date_list() -> list[datetime]:
    t = today_dt() + timedelta(days=START_OFFSET_DAYS)
    return [t + timedelta(days=i) for i in range(TOTAL_DAYS)]


def output_date(dt: datetime) -> str:
    return dt.strftime("%d-%m-%Y")


def rex_form_date(dt: datetime) -> str:
    return dt.strftime("%d %b %Y")


def route_date_key(run_id: str, origin: str, dest: str, date_str: str) -> tuple[str, str, str, str]:
    return (run_id, origin, dest, date_str)


def normalise_time(raw: str) -> str:
    """
    FIX 1: Koi conversion nahi — website ka raw time as-is return karo.
    e.g. "9:50am" stays "9:50am"  |  "1:35pm" stays "1:35pm"
    Sirf whitespace trim karo.
    """
    return raw.strip()


def extract_all_times_from_text(text: str) -> list[str]:
    """Card text se saare times (12hr ya 24hr) order mein nikalo."""
    times = re.findall(r'\d{1,2}:\d{2}\s*[aApP][mM]', text)
    if times:
        return [t.strip() for t in times]
    return [t.strip() for t in re.findall(r'\b\d{1,2}:\d{2}\b', text)]


def extract_times_per_zl(card_text: str) -> dict[str, str]:
    """
    FINAL FIX: Rex card text mein time order hamesha yeh hoti hai:
      dep_time_1  arr_time_1  ZL_XXXX ...
      dep_time_2  arr_time_2  ZL_YYYY ...

    Toh card mein saare times collect karo.
    Har N-ve ZL ko times[N*2] milega (0-indexed departure times).

    Example:
      times = [9:50am, 12:00pm, 1:35pm, 3:35pm]
      ZL2417 (index 0) → times[0] = 9:50am  ✅
      ZL2268 (index 1) → times[2] = 1:35pm  ✅

    Safe fallback: agar times ka count ZL count se match na kare, toh
    available times sequential assign ho jaate hain bina crash ke.
    """
    all_times = extract_all_times_from_text(card_text)
    zl_matches = list(re.finditer(r'ZL\s?\d{3,4}', card_text))

    result = {}
    for idx, m in enumerate(zl_matches):
        flight = re.sub(r'\s', '', m.group())
        dep_idx = idx * 2  # departure time = even index (0, 2, 4, ...)
        if dep_idx < len(all_times):
            dep = all_times[dep_idx]
        elif idx < len(all_times):
            # Fallback: times count kam hai, sequential se lo
            dep = all_times[idx]
        else:
            dep = "-"
        result[flight] = dep
    return result


def _ensure_cents(val: str) -> str:
    return val if '.' in val else f"{val}.00"


def _format_price(dollars: str, cents: str | None = None) -> str:
    dollars = dollars.replace(",", "").strip()
    if cents is None:
        return f"${_ensure_cents(dollars)}"
    return f"${dollars}.{cents.strip()}"


def _extract_price_patterns(text: str) -> str:
    # Rex often renders cents as a separate visual fragment, e.g. "$196 65".
    patterns = [
        r'[Ff]rom\s*\$\s*([\d,]+)\.(\d{2})',
        r'\$\s*([\d,]+)\.(\d{2})',
        r'[Ff]rom\s*\$\s*([\d,]+)\s+(\d{2})(?!\d)',
        r'\$\s*([\d,]+)\s+(\d{2})(?!\d)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return _format_price(m.group(1), m.group(2))

    m = re.search(r'[Ff]rom\s*\$\s*(\d[\d,]*)', text)
    if m:
        return _format_price(m.group(1))
    m = re.search(r'\$\s*(\d[\d,]*)', text)
    if m:
        return _format_price(m.group(1))
    return "N/A"


class TeeStream:
    """Duplicate cron stdout/stderr to a log file while preserving console output."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, text):
        safe_text = redact_sensitive_text(text)
        for stream in self.streams:
            try:
                stream.write(safe_text)
                stream.flush()
            except (ValueError, IOError, OSError):
                pass

    def flush(self):
        for stream in self.streams:
            try:
                stream.flush()
            except (ValueError, IOError, OSError):
                pass

    def reconfigure(self, **kwargs):
        for stream in self.streams:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(**kwargs)


def configure_run_logging(log_dir: str, run_id: str):
    if not log_dir:
        return None
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"rex_brightdata_{run_id}_{rex_now():%Y%m%d_%H%M%S}.log")
    log_fh = open(log_path, "a", encoding="utf-8", errors="replace")
    sys.stdout = TeeStream(sys.stdout, log_fh)
    sys.stderr = TeeStream(sys.stderr, log_fh)
    print(f"🧾 Cron log: {os.path.abspath(log_path)}")
    return log_fh


def restore_run_logging(log_fh):
    if not log_fh:
        return
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    sys.stdout = ORIGINAL_STDOUT
    sys.stderr = ORIGINAL_STDERR
    try:
        log_fh.flush()
        log_fh.close()
    except Exception:
        pass


CHECKPOINT_EVERY = int(os.getenv("REX_CHECKPOINT_EVERY", "7"))


class OutputStore:
    """Atomic, resume-friendly Excel writer keyed by run/date/route."""

    def __init__(self, path: str, run_id: str):
        self.path = path
        self.run_id = run_id
        self._entries_since_checkpoint: int = 0  # checkpoint counter

    def _load(self):
        if os.path.exists(self.path):
            wb = load_workbook(self.path)
            ws = wb.active
            # openpyxl never returns max_row==0; check if first row has any values
            first_row_has_values = any(
                ws.cell(row=1, column=c).value
                for c in range(1, len(CSV_FIELDS) + 1)
            )
            if not first_row_has_values:
                ws.append(CSV_FIELDS)
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = "Rex Flight Data"
            ws.append(CSV_FIELDS)

        self._ensure_headers(ws)
        return wb, ws

    def _headers(self, ws) -> list[str]:
        return [cell.value or "" for cell in ws[1]]

    def _ensure_headers(self, ws):
        headers = self._headers(ws)
        if not any(headers):
            for idx, col_name in enumerate(CSV_FIELDS, 1):
                ws.cell(row=1, column=idx).value = col_name
            return

        changed = False
        for col_name in CSV_FIELDS:
            if col_name not in headers:
                headers.append(col_name)
                ws.cell(row=1, column=len(headers)).value = col_name
                changed = True
        if changed:
            print("   ℹ️  Added production status columns to existing workbook.")

    def _save_atomic(self, wb):
        out_dir = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(out_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{Path(self.path).stem}_",
            suffix=".xlsx",
            dir=out_dir,
        )
        os.close(fd)
        try:
            wb.save(tmp_path)
            os.replace(tmp_path, self.path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def _row_matches_job(self, ws, row_idx: int, run_id: str, origin: str, dest: str, date_str: str) -> bool:
        headers = self._headers(ws)
        values = {header: ws.cell(row=row_idx, column=idx + 1).value for idx, header in enumerate(headers)}
        return (
            str(values.get("Run ID") or "") == run_id
            and str(values.get("Origin") or "") == origin
            and str(values.get("Destination") or "") == dest
            and str(values.get("Date of Departure") or "") == date_str
        )

    def write_job_rows(self, origin: str, dest: str, date_str: str, rows: list[dict]):
        wb, ws = self._load()
        headers = self._headers(ws)

        for row_idx in range(ws.max_row, 1, -1):
            if self._row_matches_job(ws, row_idx, self.run_id, origin, dest, date_str):
                ws.delete_rows(row_idx, 1)

        for row in rows:
            row.setdefault("Run ID", self.run_id)
            row.setdefault("Status", STATUS_SUCCESS)
            row.setdefault("Comment", "")
            row.setdefault("Retry Count", 0)
            row.setdefault("Debug Artifacts", "")
            ws.append([row.get(field, "") for field in headers])
            self._entries_since_checkpoint += 1

        # ── CHECKPOINT: har 7 entries ke baad Excel force-save ──
        if self._entries_since_checkpoint >= CHECKPOINT_EVERY:
            self._save_atomic(wb)
            print(f"   💾 Checkpoint: {self._entries_since_checkpoint} entries ke baad Excel save kiya → {self.path}")
            self._entries_since_checkpoint = 0
        else:
            self._save_atomic(wb)

    def job_rows(self, origin: str, dest: str, date_str: str) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        wb, ws = self._load()
        headers = self._headers(ws)
        rows = []
        for row_idx in range(2, ws.max_row + 1):
            if self._row_matches_job(ws, row_idx, self.run_id, origin, dest, date_str):
                rows.append({
                    header: ws.cell(row=row_idx, column=idx + 1).value
                    for idx, header in enumerate(headers)
                })
        return rows

    def job_completed(self, origin: str, dest: str, date_str: str) -> bool:
        rows = self.job_rows(origin, dest, date_str)
        if not rows:
            return False
        return all((row.get("Status") or "") in COMPLETED_STATUSES for row in rows)

    def job_has_any_row(self, origin: str, dest: str, date_str: str) -> bool:
        return bool(self.job_rows(origin, dest, date_str))

    def failed_route_dates(self, origin: str, dest: str, date_strs: list[str]) -> list[str]:
        """Return the subset of date_strs that have retryable failure rows (not completed)."""
        failed = []
        for date_str in date_strs:
            if self.job_completed(origin, dest, date_str):
                continue                    # already SUCCESS / NO_FARE — skip
            rows = self.job_rows(origin, dest, date_str)
            if not rows:
                continue                    # never written yet — main run handles it
            statuses = {(row.get("Status") or "") for row in rows}
            if statuses & RETRYABLE_FAILURE_STATUSES:
                failed.append(date_str)
        return failed


OUTPUT_STORE: OutputStore | None = None


def append_rows(rows: list):
    if not rows:
        return
    if OUTPUT_STORE:
        grouped: dict[tuple[str, str, str], list[dict]] = {}
        for row in rows:
            key = (
                str(row.get("Origin", "")),
                str(row.get("Destination", "")),
                str(row.get("Date of Departure", "")),
            )
            grouped.setdefault(key, []).append(row)
        for (origin, dest, date_str), job_rows in grouped.items():
            OUTPUT_STORE.write_job_rows(origin, dest, date_str, job_rows)
        return

    if os.path.exists(OUTPUT_EXCEL):
        wb = load_workbook(OUTPUT_EXCEL)
        ws = wb.active
    else:
        wb = Workbook()
        ws = wb.active
        ws.title = "Rex Flight Data"
        ws.append(CSV_FIELDS)
    for row in rows:
        ws.append([row.get(f, "") for f in CSV_FIELDS])
    wb.save(OUTPUT_EXCEL)


# ─────────────────────────────────────────────────────────────
#  PRICE EXTRACTION
# ─────────────────────────────────────────────────────────────

def extract_price_from_card_text(card_text: str) -> str:
    return _extract_price_patterns(card_text)


def find_ribbon_end_position(full_body: str) -> int:
    markers = [
        "Departure Time",
        "Select your departing flight",
        "departing flight",
        "Fly Economy",
        "Select Fares",
    ]
    for marker in markers:
        pos = full_body.find(marker)
        if pos > 0:
            return pos
    # Improved fallback: search for first ZL number position which is a strong
    # signal that flight results section has started
    zl_match = re.search(r'ZL\s?\d{3,4}', full_body)
    if zl_match and zl_match.start() > 100:
        fallback_pos = max(0, zl_match.start() - 50)
        return fallback_pos
    # Last resort: 1/7th of body
    fallback = len(full_body) // 7
    return fallback


def extract_price_from_flight_window(full_body: str, flight_match,
                                     ribbon_end_pos: int):
    if flight_match.start() < ribbon_end_pos:
        return None

    after_start = flight_match.end()
    after_end   = min(len(full_body), after_start + 600)
    window      = full_body[after_start:after_end]

    price = _extract_price_patterns(window)
    if price != "N/A":
        return price
    return "N/A"


@dataclass
class SearchOutcome:
    ok: bool
    status: str
    reason: str = ""
    retryable: bool = True
    loaded: bool = False
    debug_artifacts: list[str] = dc_field(default_factory=list)


@dataclass
class JobResult:
    status: str
    rows: list[dict] = dc_field(default_factory=list)
    comment: str = ""
    retryable: bool = True
    retry_count: int = 0
    debug_artifacts: list[str] = dc_field(default_factory=list)

    @property
    def completed(self) -> bool:
        return self.status in COMPLETED_STATUSES


# ─────────────────────────────────────────────────────────────
#  SCRAPER
# ─────────────────────────────────────────────────────────────

class RexScraper:

    def __init__(
        self,
        headless=False,
        debug_dir: str = DEBUG_DIR,
        max_attempts: int = MAX_ATTEMPTS,
        final_retry_rounds: int = FINAL_RETRY_ROUNDS,
        retry_backoff: float = RETRY_BACKOFF_SECONDS,
        max_retry_backoff: float = MAX_RETRY_BACKOFF_SECONDS,
        job_timeout: int = JOB_TIMEOUT_SECONDS,
        navigation_mode: str = NAVIGATION_MODE,
        resume: bool = RESUME_ENABLED,
        captcha_disabled_threshold: int = CAPTCHA_DISABLED_LOOP_THRESHOLD,
        captcha_recovery_max: int = CAPTCHA_RECOVERY_MAX,
        captcha_recovery_strategy: str = CAPTCHA_RECOVERY_STRATEGY,
    ):
        self.headless = headless
        self._last_ribbon_price = ""
        self.debug_dir = debug_dir
        self.max_attempts = max(1, max_attempts)
        self.final_retry_rounds = max(0, final_retry_rounds)
        self.retry_backoff = max(0.0, retry_backoff)
        self.max_retry_backoff = max(1.0, max_retry_backoff)
        self.job_timeout = max(30, job_timeout)
        self.navigation_mode = (navigation_mode or "ribbon").lower()
        self.resume = resume
        self.captcha_disabled_threshold = max(1, captcha_disabled_threshold)
        self.captcha_recovery_max = max(0, captcha_recovery_max)
        self.captcha_recovery_strategy = (captcha_recovery_strategy or "refill-then-new-context").lower()
        # Bright Data playwright instance — stored so _new_job_context can
        # reconnect transparently when the browser session is killed.
        self._pw_instance = None
        if self.captcha_recovery_strategy not in CAPTCHA_RECOVERY_STRATEGIES:
            print(
                f"⚠️  Unknown captcha recovery strategy {self.captcha_recovery_strategy!r}; "
                "using refill-then-new-context"
            )
            self.captcha_recovery_strategy = "refill-then-new-context"

    async def save_debug_artifacts(self, page, label: str, metadata: dict | None = None) -> list[str]:
        os.makedirs(self.debug_dir, exist_ok=True)
        safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_") or "page"
        ts = rex_now().strftime("%Y%m%d_%H%M%S")
        png_path = os.path.join(self.debug_dir, f"{safe_label}_{ts}.png")
        html_path = os.path.join(self.debug_dir, f"{safe_label}_{ts}.html")
        json_path = os.path.join(self.debug_dir, f"{safe_label}_{ts}.json")
        artifacts = []

        try:
            await page.screenshot(path=png_path, full_page=True)
            artifacts.append(os.path.abspath(png_path))
            print(f"   📸 Debug screenshot: {os.path.abspath(png_path)}")
        except Exception as exc:
            print(f"   ⚠️  Screenshot failed: {exc}")

        try:
            html = await page.content()
            with open(html_path, "w", encoding="utf-8") as fh:
                fh.write(html)
            artifacts.append(os.path.abspath(html_path))
            print(f"   🧾 Debug HTML: {os.path.abspath(html_path)}")
        except Exception as exc:
            print(f"   ⚠️  HTML dump failed: {exc}")

        meta = dict(metadata or {})
        try:
            meta.setdefault("url", page.url)
        except Exception:
            pass
        meta.setdefault("saved_at", rex_now().isoformat(sep=" "))
        try:
            body = (await page.inner_text("body", timeout=3000)).strip()
            meta["body_text_snippet"] = body[:1500]
        except Exception:
            pass
        try:
            with open(json_path, "w", encoding="utf-8") as fh:
                json.dump(meta, fh, ensure_ascii=False, indent=2)
            artifacts.append(os.path.abspath(json_path))
            print(f"   🧭 Debug metadata: {os.path.abspath(json_path)}")
        except Exception as exc:
            print(f"   ⚠️  Debug metadata dump failed: {exc}")

        return artifacts

    async def page_has_rex_server_error(self, page) -> bool:
        try:
            text = (await page.inner_text("body", timeout=3000)).lower()
        except Exception:
            return False
        markers = [
            "server error in '/' application",
            "timeout expired",
            "connection from the pool",
            "max pool size was reached",
            "an unhandled exception occurred",
            "runtime error",
        ]
        return any(marker in text for marker in markers)

    async def page_has_verification_or_block(self, page) -> bool:
        try:
            text = (await page.inner_text("body", timeout=3000)).lower()
            html = (await page.content()).lower()
        except Exception:
            return False
        markers = [
            "please verify your details",
            "google.com/recaptcha",
            "grecaptcha",
            "txtcaptcha",
            "captcha",
            "access denied",
            "blocked",
        ]
        return any(marker in text or marker in html for marker in markers)

    async def page_has_rex_verification_page(self, page) -> bool:
        try:
            text = re.sub(r"\s+", " ", (await page.inner_text("body", timeout=3000)).lower())
            html = (await page.content()).lower()
        except Exception:
            return False
        return (
            "please verify your details" in text
            or "divcaptcha" in html
            or "txtcaptcha" in html
            or "grecaptcha.render" in html
        )

    async def _force_enable_rex_continue(self, page) -> bool:
        """
        reCAPTCHA quota exceeded hone pe Google callback kabhi fire nahi karta,
        isliye button.availCont hamesha disabled rehta hai.
        Yeh function Rex ka captcha callback replicate karta hai via JS:
          - #txtcaptcha value = 'captchad'  (Rex internally check karta hai)
          - button.availCont ka 'disabled' attribute JS se remove karta hai
        Isse bina valid Google token ke bhi Continue click ho sakta hai.
        """
        try:
            enabled = await page.evaluate(
                """() => {
                    try {
                        const txt = document.getElementById('txtcaptcha');
                        if (txt) txt.value = 'captchad';
                        const btn = document.querySelector('button.availCont');
                        if (!btn) return false;
                        btn.removeAttribute('disabled');
                        btn.disabled = false;
                        return true;
                    } catch(e) {
                        return false;
                    }
                }"""
            )
            if enabled:
                print("   ⚡ Force-enabled Rex Continue button (reCAPTCHA quota workaround)")
            return bool(enabled)
        except Exception:
            return False

    async def click_rex_verification_continue(self, page, timeout: int = 15000) -> bool:
        try:
            btn = page.locator("button.availCont").first
            await btn.wait_for(state="visible", timeout=timeout)
            # Normal path: wait for Google callback to enable the button
            try:
                await page.wait_for_function(
                    """() => {
                        const btn = document.querySelector('button.availCont');
                        return !!btn && !btn.disabled && !btn.hasAttribute('disabled');
                    }""",
                    timeout=timeout,
                )
            except Exception:
                # Button still disabled — reCAPTCHA quota likely exceeded.
                # Force-enable via JS (replicates Rex's own captcha callback).
                print("   ⚠️  Continue still disabled — attempting force-enable (quota workaround)...")
                if not await self._force_enable_rex_continue(page):
                    return False
                await asyncio.sleep(1)
            await btn.click(timeout=5000)
            print("   ✅ Clicked Rex verification Continue button")
            await asyncio.sleep(2)
            return True
        except Exception:
            return False

    async def page_has_no_fare_signal(self, page) -> bool:
        try:
            text = re.sub(r"\s+", " ", (await page.inner_text("body", timeout=5000)).lower())
        except Exception:
            return False
        patterns = [
            r"\bno\s+(?:available\s+)?flights?\b",
            r"\bno\s+(?:available\s+)?fares?\b",
            r"\bunable\s+to\s+find\s+flights?\b",
            r"\bflights?.{0,80}\bnot\s+available\b",
            r"\bfares?.{0,80}\bnot\s+available\b",
            r"\bsold\s+out\b",
        ]
        return any(re.search(pattern, text) for pattern in patterns)

    async def page_has_expected_result_structure(self, page) -> bool:
        selectors = CARD_SELECTORS + FLIGHT_LIST_SELECTORS + [
            "text=/Select your departing flight/i",
            "text=/Departure Time/i",
            "text=/Select Fares/i",
        ]
        for sel in selectors:
            try:
                if await page.locator(sel).count():
                    return True
            except Exception:
                pass
        try:
            body = await page.inner_text("body", timeout=3000)
            return bool(re.search(r'ZL\s?\d{3,4}', body))
        except Exception:
            return False

    async def open_rex_home(self, page, label: str, attempts: int = 4) -> bool:
        for attempt in range(1, attempts + 1):
            retry_ts = int(datetime.now(REX_TZ).timestamp())
            url = f"https://www.rex.com.au/?codex_retry={retry_ts}_{attempt}"
            print(f"   🌐 Loading Rex homepage (attempt {attempt}/{attempts})...")
            try:
                try:
                    await page.goto(url, wait_until="commit", timeout=70000)
                except Exception:
                    await page.goto(url, wait_until="domcontentloaded", timeout=70000)
            except Exception as exc:
                print(f"   ⚠️  Homepage load failed: {exc}")

            await self.wait_for_protection_to_clear(page, f"{label}_attempt_{attempt}", timeout=120)

            if await self.page_has_rex_server_error(page):
                print("   ⚠️  Rex returned server error / connection-pool timeout")
                await self.save_debug_artifacts(page, f"{label}_server_error_attempt_{attempt}")
                if attempt < attempts:
                    await asyncio.sleep(8 * attempt)
                    continue

            if await self.prepare_rex_home(page, f"{label}_not_ready"):
                return True

            if attempt < attempts:
                await asyncio.sleep(5 * attempt)

        return False

    async def wait_for_brightdata_captcha(self, page, detect_timeout: int = 60000) -> str:
        """
        Ask Bright Data's Scraping Browser to solve any captcha on the current page.

        Returns one of three string values:
          "solved"     — captcha was solved successfully ("solved" or "solve_finished" status).
                         Continue button should be enabled; page will proceed.
          "failed"     — Bright Data returned a hard failure ("invalid" or "solve_failed").
                         Rex's reCAPTCHA quota is exhausted for this IP; caller should
                         fast-exit rather than keep looping.
          "not_solved" — captcha not detected, timed out, or unknown; caller may still
                         attempt force-enable as a fallback.
        """
        try:
            client = await page.context.new_cdp_session(page)
            result = await client.send("Captcha.waitForSolve", {"detectTimeout": detect_timeout})
            # Observed statuses from Bright Data Scraping Browser:
            #   "solved"       — captcha widget solved, token injected
            #   "solve_finished" — Bright Data completed a solve (functionally identical to "solved")
            #   "not_detected" — no captcha widget found on the page
            #   "solve_failed" — Bright Data attempted but failed
            #   "invalid"      — Rex server rejected the captcha token (quota exhausted)
            #   "timeout"      — detectTimeout elapsed with no solve
            status = (result or {}).get("status", "unknown") if isinstance(result, dict) else "unknown"
            if status in ("solved", "solve_finished"):
                print(f"   ✅ Bright Data captcha solved successfully (status={status!r})")
                return "solved"
            elif status in ("solve_failed", "invalid"):
                print(
                    f"   ❌ Bright Data captcha hard-failed (status={status!r}) — "
                    "reCAPTCHA quota likely exhausted for this IP"
                )
                return "failed"
            else:
                print(f"   ⚠️  Bright Data captcha status: {status!r} — not solved")
                return "not_solved"
        except Exception as exc:
            print(f"   ⚠️  Bright Data captcha wait exception: {exc}")
            return "not_solved"

    async def page_looks_like_cloudflare_or_challenge(self, page) -> bool:
        try:
            text = re.sub(r"\s+", " ", (await page.inner_text("body", timeout=3000)).lower())
            title = (await page.title()).lower()
        except Exception:
            return False
        markers = [
            "cloudflare",
            "verify you are human",
            "checking your browser",
            "just a moment",
            "turnstile",
            "challenge-platform",
            "captcha",
        ]
        return any(marker in text or marker in title for marker in markers)

    async def wait_for_protection_to_clear(self, page, label: str, timeout: int = 120) -> bool:
        """Give Bright Data Browser API time to solve Cloudflare/captcha pages."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        solver_started = False

        while loop.time() < deadline:
            if await self.rex_home_ready(page):
                return True

            if await self.page_has_rex_server_error(page):
                return False

            if await self.page_looks_like_cloudflare_or_challenge(page):
                if not solver_started:
                    print("   🛡️  Protection challenge detected; waiting for Bright Data Browser API solver...")
                    solver_started = True
                # Cap captcha wait to remaining time to avoid overrunning deadline
                remaining_ms = max(1000, int((deadline - loop.time()) * 1000))
                captcha_wait_ms = min(30000, remaining_ms)
                await self.wait_for_brightdata_captcha(page, detect_timeout=captcha_wait_ms)
                await asyncio.sleep(2)
                continue

            if not solver_started:
                remaining_ms = max(1000, int((deadline - loop.time()) * 1000))
                captcha_wait_ms = min(10000, remaining_ms)
                await self.wait_for_brightdata_captcha(page, detect_timeout=captcha_wait_ms)
                solver_started = True

            await asyncio.sleep(1)

        print(f"   ⚠️  Protection/form wait timed out after {timeout}s")
        await self.save_debug_artifacts(page, f"{label}_protection_timeout")
        return False

    async def click_continue_if_present(self, page) -> bool:
        selectors = [
            "button:has-text('Continue')",
            "a:has-text('Continue')",
            "text=/^\\s*Continue\\s*$/i",
            "button:has-text('continiew')",
            "a:has-text('continiew')",
            "input[type='button'][value*='Continue']",
            "input[type='submit'][value*='Continue']",
            "[id*='continue' i]",
            "[class*='continue' i]",
        ]

        roots = [page] + list(page.frames)
        for root in roots:
            for pattern in [re.compile(r"continue", re.I), re.compile(r"continiew", re.I)]:
                try:
                    loc = root.get_by_role("button", name=pattern).first
                    if await loc.count() and await loc.is_visible(timeout=500):
                        try:
                            if await loc.is_disabled(timeout=500):
                                continue
                        except Exception:
                            pass
                        await loc.click(timeout=5000, force=True)
                        print("   ✅ Clicked Continue button")
                        await asyncio.sleep(2)
                        return True
                except Exception:
                    pass

            for sel in selectors:
                try:
                    loc = root.locator(sel).first
                    if await loc.count() and await loc.is_visible(timeout=500):
                        try:
                            if await loc.is_disabled(timeout=500):
                                continue
                        except Exception:
                            pass
                        await loc.click(timeout=5000, force=True)
                        print(f"   ✅ Clicked Continue button ({sel})")
                        await asyncio.sleep(2)
                        return True
                except Exception:
                    pass

        return False

    async def rex_home_ready(self, page) -> bool:
        selectors = [
            "label[for*='rbTripType_oneway']",
            "input[id*='rbTripType_oneway']",
            "#ContentPlaceHolder1_BookingHomepageV21_OriginAirport + .select2-container",
            "#ContentPlaceHolder1_BookingHomepageV21_DestinationAirport + .select2-container",
            "#datefilter",
        ]
        for sel in selectors:
            try:
                if await page.locator(sel).count():
                    return True
            except Exception:
                pass
        return False

    async def prepare_rex_home(self, page, label: str, timeout: int = 90) -> bool:
        print("   ⏳ Waiting for Rex homepage form / Continue button...")
        deadline = asyncio.get_running_loop().time() + timeout
        captcha_waited = False

        while asyncio.get_running_loop().time() < deadline:
            if await self.rex_home_ready(page):
                print("   ✅ Rex booking form is ready")
                return True

            if await self.click_continue_if_present(page):
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass

                form_deadline = asyncio.get_running_loop().time() + 18
                while asyncio.get_running_loop().time() < form_deadline:
                    if await self.rex_home_ready(page):
                        print("   ✅ Rex booking form is ready after Continue")
                        return True
                    if await self.page_has_rex_server_error(page):
                        print("   ⚠️  Rex server error appeared after Continue")
                        await self.save_debug_artifacts(page, f"{label}_after_continue_server_error")
                        return False
                    await asyncio.sleep(1)

                print("   ⚠️  Continue clicked but booking form did not load; retrying homepage fresh")
                await self.save_debug_artifacts(page, f"{label}_after_continue_stuck")
                return False

            if not captcha_waited:
                print("   ⏳ Waiting for Bright Data captcha solver if present...")
                await self.wait_for_brightdata_captcha(page, detect_timeout=60000)
                captcha_waited = True
                continue

            await asyncio.sleep(1.5)

        print("   ❌ Rex booking form did not appear in time")
        await self.save_debug_artifacts(page, label)
        try:
            body = (await page.inner_text("body", timeout=3000)).strip()
            if body:
                print("   📝 Page text snippet:")
                print("   " + body[:700].replace("\n", "\n   "))
        except Exception:
            pass
        return False

    async def click_one_way(self, page, label: str) -> bool:
        try:
            ok = await page.evaluate(
                """() => {
                    const one = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_rbTripType_oneway');
                    const ret = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_rbTripType_return');
                    if (!one) return false;
                    one.checked = true;
                    if (ret) ret.checked = false;
                    one.dispatchEvent(new Event('change', { bubbles: true }));
                    one.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
                    if (typeof setOneWayDate === 'function') setOneWayDate();
                    return true;
                }"""
            )
            if ok:
                return True
        except Exception:
            pass

        selectors = [
            "label[for*='rbTripType_oneway']",
            "label:has-text('One way')",
            "label:has-text('One-way')",
            "input[id*='rbTripType_oneway']",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count():
                    await loc.click(timeout=10000, force=True)
                    return True
            except Exception:
                pass

        print("   ❌ Could not select one-way trip type")
        await self.save_debug_artifacts(page, label)
        return False

    async def select_airport_code(self, page, select_id: str, code: str, label: str,
                                  wait_for_option: bool = False) -> bool:
        locator = page.locator(f"#{select_id}").first
        try:
            await locator.wait_for(state="attached", timeout=15000)
        except Exception:
            print(f"   ❌ Missing airport selector: {label} ({select_id})")
            await self.save_debug_artifacts(page, f"missing_{label}_{code}")
            return False

        if wait_for_option:
            deadline = asyncio.get_running_loop().time() + 20
            last_options = []
            while asyncio.get_running_loop().time() < deadline:
                try:
                    option_state = await page.evaluate(
                        """([id, code]) => {
                            const el = document.getElementById(id);
                            if (!el) return { found: false, options: [] };
                            const options = Array.from(el.options).map((opt) => ({
                                value: (opt.value || '').trim(),
                                text: (opt.textContent || '').trim()
                            }));
                            const found = options.some((opt) =>
                                opt.value === code ||
                                opt.text.toUpperCase().includes(`(${code})`) ||
                                opt.text.toUpperCase() === code
                            );
                            return { found, options };
                        }""",
                        [select_id, code],
                    )
                    last_options = option_state.get("options", [])
                    if option_state.get("found"):
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)
            else:
                print(f"   ❌ {label} option did not appear: {code}")
                await self.save_debug_artifacts(
                    page,
                    f"missing_{label}_option_{code}",
                    {"airport_code": code, "available_options": last_options},
                )
                return False

        try:
            result = await page.evaluate(
                """([id, code]) => {
                    const el = document.getElementById(id);
                    if (!el) return { ok: false, reason: 'selector missing' };
                    const option = Array.from(el.options).find((opt) => {
                        const value = (opt.value || '').trim();
                        const text = (opt.textContent || '').trim().toUpperCase();
                        return value === code || text.includes(`(${code})`) || text === code;
                    });
                    if (!option) {
                        return {
                            ok: false,
                            reason: `option ${code} missing`,
                            available: Array.from(el.options).map((opt) => ({
                                value: (opt.value || '').trim(),
                                text: (opt.textContent || '').trim()
                            })).filter((opt) => opt.value || opt.text)
                        };
                    }
                    el.value = option.value;
                    option.selected = true;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    if (window.jQuery) window.jQuery(el).trigger('change');
                    return { ok: true, selected: el.value };
                }""",
                [select_id, code],
            )
        except Exception as exc:
            print(f"   ❌ Could not set {label} airport {code}: {exc}")
            await self.save_debug_artifacts(page, f"set_{label}_{code}_failed")
            return False

        if not result.get("ok"):
            print(f"   ❌ Could not set {label} airport {code}: {result.get('reason')}")
            await self.save_debug_artifacts(
                page,
                f"set_{label}_{code}_missing",
                {"airport_code": code, "available_options": result.get("available", [])},
            )
            return False

        try:
            selected = await page.evaluate(
                """([id, code]) => {
                    const el = document.getElementById(id);
                    if (!el) return '';
                    const opt = el.options[el.selectedIndex];
                    const text = opt ? (opt.textContent || '').toUpperCase() : '';
                    return el.value === code || text.includes(`(${code})`) ? el.value : '';
                }""",
                [select_id, code],
            )
        except Exception:
            selected = ""

        if not selected:
            print(f"   ❌ {label.title()} airport did not remain selected: {code}")
            await self.save_debug_artifacts(page, f"verify_{label}_{code}_failed")
            return False

        await asyncio.sleep(0.5)
        print(f"   ✅ {label.title()} airport selected: {code}")
        return True

    async def select_route_fields(self, page, origin_code: str, dest_code: str) -> bool:
        origin_id = "ContentPlaceHolder1_BookingHomepageV21_OriginAirport"
        dest_id = "ContentPlaceHolder1_BookingHomepageV21_DestinationAirport"
        if not await self.select_airport_code(page, origin_id, origin_code, "origin"):
            return False
        if not await self.select_airport_code(page, dest_id, dest_code, "destination", wait_for_option=True):
            return False
        return True

    def parse_tab_date(self, text: str) -> datetime | None:
        m = re.search(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\s+(\d{1,2})\s+'
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
            text, re.IGNORECASE
        )
        if not m:
            return None
        day, mon = int(m.group(1)), m.group(2).capitalize()
        now = rex_now()
        for year in [now.year, now.year + 1]:
            try:
                dt = datetime.strptime(f"{day} {mon} {year}", "%d %b %Y")
                if abs((dt - now).days) < 200:
                    return dt
            except ValueError:
                pass
        return None

    async def click_ribbon_tab(self, page, target_dt: datetime) -> str:
        for sel in RIBBON_SELECTORS:
            tabs = await page.query_selector_all(sel)
            if not tabs:
                continue
            for tab in tabs:
                raw = (await tab.inner_text()).strip().replace("\n", " ")
                tab_dt = self.parse_tab_date(raw)
                if not tab_dt or tab_dt.date() != target_dt.date():
                    continue

                cls       = (await tab.get_attribute("class") or "").lower()
                raw_lower = raw.lower()
                no_flight = any([
                    "unavailable" in raw_lower,
                    "unavailable" in cls,
                    "disabled"    in cls,
                    "no-flight"   in cls,
                    "noflight"    in cls,
                    "greyed"      in cls,
                    "inactive"    in cls,
                ])
                if no_flight:
                    print(f"   ℹ️  No-flight tab: '{raw[:70]}'")
                    self._last_ribbon_price = ""
                    return "unavailable"

                rp = re.search(r'\$\s*([\d,]+(?:\.\d{2})?)', raw)
                if rp:
                    val = rp.group(1)
                    self._last_ribbon_price = f"${_ensure_cents(val)}"
                    print(f"   🎫 Ribbon price: {self._last_ribbon_price}")

                await tab.click(force=True)
                return "clicked"
        return "not_found"

    async def go_next_ribbon(self, page) -> bool:
        for sel in [
            ".calendar .arrow.next", ".ribbon-next", "button.next-week",
            "[aria-label='Next week']", ".date-nav-next",
            "span.arrow.right", "button[class*='next']",
            "span[class*='arrow']", "a[class*='next']",
        ]:
            btn = await page.query_selector(sel)
            if btn:
                if (await btn.get_attribute("disabled")) is not None:
                    return False
                cls = (await btn.get_attribute("class") or "").lower()
                if "disabled" in cls:
                    return False
                await btn.click()
                await asyncio.sleep(3)
                return True
        return False

    async def wait_for_flights_loaded(self, page, target_dt: datetime,
                                       timeout: int = 15,
                                       strict_date: bool = False) -> bool:
        exp_day = str(target_dt.day)
        exp_mon = target_dt.strftime("%b")
        date_ok    = False
        flights_ok = False
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        last_body_check = 0.0

        while loop.time() < deadline:
            if not date_ok:
                for sel in [
                    ".departing-block h2.date", ".selected-date",
                    "h2.date", ".flight-date-header", ".date-heading",
                    "h2", ".date-display",
                ]:
                    try:
                        txt = await page.locator(sel).first.inner_text(timeout=500)
                        if exp_day in txt and exp_mon in txt:
                            date_ok = True
                            break
                    except:
                        pass

            if not flights_ok:
                for sel in CARD_SELECTORS:
                    try:
                        cards = await page.query_selector_all(sel)
                    except Exception:
                        cards = []
                    if cards:
                        for card in cards[:3]:
                            try:
                                txt = (await card.inner_text(timeout=700)).strip()
                                if re.search(r'ZL\s?\d{3,4}', txt):
                                    flights_ok = True
                                    break
                            except Exception:
                                pass
                    if flights_ok:
                        break

                now = loop.time()
                if not flights_ok and now - last_body_check >= 1.0:
                    last_body_check = now
                    try:
                        body_snippet = await page.inner_text("body", timeout=1000)
                        if re.search(r'ZL\s?\d{3,4}', body_snippet[:5000]):
                            flights_ok = True
                    except Exception:
                        pass

            if date_ok and flights_ok:
                print(f"   ✅ Page loaded — date synced + flights visible")
                return True

            await asyncio.sleep(0.5)

        if flights_ok and not strict_date:
            print(f"   ⚠️  Flights visible but date header uncertain — proceeding")
            return True

        if flights_ok and strict_date:
            print(f"   ⚠️  Flights visible but target date header not confirmed")
            return False

        print(f"   ⚠️  Load timeout ({timeout}s) — extracting anyway")
        return False

    async def wait_for_search_result(self, page, target_dt: datetime,
                                     timeout: int = 45) -> SearchOutcome:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        last_url = ""
        structure_seen = False
        verification_seen = False
        verification_disabled_loops = 0
        # Tracks total verification page encounters regardless of click outcome.
        # Force-enable makes click_rex_verification_continue return True even when
        # the real captcha was never solved, so verification_disabled_loops (which
        # resets on True) never accumulates.  This counter never resets and provides
        # the actual infinite-loop guard.
        total_verification_loops = 0
        max_total_verification = max(
            self.captcha_disabled_threshold * 3,
            int(os.getenv("REX_MAX_VERIFICATION_LOOPS", "12")),
        )

        while loop.time() < deadline:
            try:
                last_url = page.url
            except Exception:
                pass

            if await self.page_has_rex_server_error(page):
                return SearchOutcome(False, STATUS_SITE_UNAVAILABLE,
                                     "Rex returned an ASP.NET/server error", retryable=True)

            if await self.page_has_rex_verification_page(page):
                total_verification_loops += 1
                verification_seen = True

                if total_verification_loops > max_total_verification:
                    return SearchOutcome(
                        False,
                        STATUS_BLOCKED,
                        (
                            f"Rex verification page persisted across {total_verification_loops} loops "
                            "(reCAPTCHA quota exceeded — force-enable clicks not accepted by server)"
                        ),
                        retryable=True,
                    )

                print(
                    f"   🧩 Rex verification page detected (loop {total_verification_loops}/{max_total_verification}); "
                    "waiting for Bright Data captcha solver..."
                )
                remaining_seconds = max(1.0, deadline - loop.time())
                solver_timeout = int(min(30000, max(8000, remaining_seconds * 1000)))
                click_timeout = int(min(10000, max(3000, remaining_seconds * 1000)))
                captcha_status = await self.wait_for_brightdata_captcha(page, detect_timeout=solver_timeout)
                if captcha_status == "failed":
                    # Bright Data returned invalid/solve_failed — Rex's reCAPTCHA quota is
                    # exhausted for this IP.  Continuing to loop will just waste the remaining
                    # time budget (each loop ≈ 10–30 s) before hitting the hard 240 s timeout.
                    # Bail now so the outer retry can attempt a different approach / IP.
                    return SearchOutcome(
                        False,
                        STATUS_BLOCKED,
                        (
                            f"Rex reCAPTCHA hard-failed (invalid/solve_failed) on loop "
                            f"{total_verification_loops}/{max_total_verification} — "
                            "reCAPTCHA quota exhausted for this Bright Data IP"
                        ),
                        retryable=True,
                    )
                if captcha_status != "solved":
                    # not_detected / timeout / unknown — button won't be enabled by the
                    # normal captcha path; jump straight to force-enable inside
                    # click_rex_verification_continue.
                    print("   ⚡ Captcha not solved by Bright Data — using force-enable path directly")
                if await self.click_rex_verification_continue(page, timeout=click_timeout):
                    verification_disabled_loops = 0
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    await asyncio.sleep(2)
                    continue
                verification_disabled_loops += 1
                print(
                    "   ⏳ Rex verification Continue is still disabled; "
                    f"solver cycle {verification_disabled_loops}/{self.captcha_disabled_threshold}"
                )
                if verification_disabled_loops >= self.captcha_disabled_threshold:
                    return SearchOutcome(
                        False,
                        STATUS_BLOCKED,
                        (
                            "Rex verification Continue stayed disabled after "
                            f"{verification_disabled_loops} Bright Data solver cycle(s)"
                        ),
                        retryable=True,
                    )
                await asyncio.sleep(2)
                continue

            if await self.page_has_verification_or_block(page):
                return SearchOutcome(False, STATUS_BLOCKED,
                                     "Rex/Bright Data verification or block page detected", retryable=True)

            try:
                body = await page.inner_text("body", timeout=1000)
            except Exception:
                body = ""

            if re.search(r'ZL\s?\d{3,4}', body):
                loaded = await self.wait_for_flights_loaded(page, target_dt, timeout=3)
                return SearchOutcome(True, STATUS_SUCCESS, "Flight results visible", retryable=False, loaded=loaded)

            if await self.page_has_no_fare_signal(page):
                return SearchOutcome(True, STATUS_NO_FARE,
                                     "Rex page explicitly indicates no flight/fare availability",
                                     retryable=False, loaded=True)

            if await self.page_has_expected_result_structure(page):
                structure_seen = True

            await asyncio.sleep(1.0)

        if structure_seen:
            return SearchOutcome(True, STATUS_STRUCTURE_VISIBLE,
                                 f"Rex result structure appeared, but no flight or no-fare signal arrived within {timeout}s",
                                 retryable=True, loaded=True)

        if verification_seen:
            return SearchOutcome(False, STATUS_BLOCKED,
                                 f"Rex verification page did not clear within {timeout}s",
                                 retryable=True)

        return SearchOutcome(False, STATUS_TIMEOUT,
                             f"Timed out waiting for Rex search result after {timeout}s; last_url={last_url}",
                             retryable=True)

    async def set_departure_date_direct(self, page, target_dt: datetime) -> bool:
        date_text = rex_form_date(target_dt)
        try:
            result = await page.evaluate(
                """(dateText) => {
                    const dateInput = document.getElementById('datefilter');
                    const dep = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_HDepartureDate');
                    const ret = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_HReturnDate');
                    const resident = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_hdnWAResidentFare');
                    if (!dateInput || !dep) return false;
                    dateInput.value = dateText;
                    dep.value = dateText;
                    if (ret) ret.value = '';
                    if (resident && resident.value === '') resident.value = 'false';
                    for (const el of [dateInput, dep, ret, resident].filter(Boolean)) {
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }
                    return true;
                }""",
                date_text,
            )
            if result:
                print(f"   📅 Departure date set directly: {date_text}")
                return True
        except Exception as exc:
            print(f"   ⚠️  Direct date set failed; using date picker fallback: {exc}")
        return False

    # ── Extract flights ──────────────────────────────────────
    async def select_departure_date(self, page, target_dt: datetime) -> bool:
        """Select a date from the currently visible Rex date picker."""
        try:
            print(f"   📅 Selecting departure date: {target_dt.strftime('%d-%m-%Y')}")
            if await self.set_departure_date_direct(page, target_dt):
                return True

            visible_date_input = page.locator("#datefilter:visible")
            date_input = (
                visible_date_input.first
                if await visible_date_input.count()
                else page.locator("#datefilter").first
            )
            await date_input.wait_for(state="attached", timeout=15000)

            try:
                await date_input.scroll_into_view_if_needed(timeout=2500)
            except Exception as exc:
                print(f"   ⚠️  Date input scroll was unstable; using click fallback: {exc}")

            picker = page.locator(".daterangepicker:visible").last
            for attempt in range(1, 4):
                try:
                    await date_input.click(timeout=5000, force=True)
                except Exception as exc:
                    print(f"   ⚠️  Date input click attempt {attempt} failed: {exc}")

                try:
                    await picker.wait_for(state="visible", timeout=2500)
                    break
                except Exception:
                    pass

                try:
                    await page.evaluate(
                        """() => {
                            const isVisible = (el) => {
                                const rect = el.getBoundingClientRect();
                                const style = window.getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 &&
                                       style.display !== 'none' && style.visibility !== 'hidden';
                            };
                            const inputs = Array.from(document.querySelectorAll('#datefilter'));
                            const el = inputs.find(isVisible) || inputs[0];
                            if (!el) return false;
                            el.scrollIntoView({block: 'center', inline: 'center'});
                            el.focus();
                            for (const type of ['mousedown', 'mouseup', 'click']) {
                                el.dispatchEvent(new MouseEvent(type, {
                                    bubbles: true,
                                    cancelable: true,
                                    view: window
                                }));
                            }
                            return true;
                        }"""
                    )
                    await picker.wait_for(state="visible", timeout=2500)
                    break
                except Exception as exc:
                    if attempt == 3:
                        print(f"   ⚠️  Date picker did not open after fallback clicks: {exc}")
                        return False
                    await asyncio.sleep(1)

            await picker.wait_for(state="visible", timeout=10000)

            def parse_picker_month(month_txt: str):
                for fmt in ("%b %Y", "%B %Y"):
                    try:
                        return datetime.strptime(month_txt, fmt)
                    except ValueError:
                        pass
                return None

            target_month = datetime(target_dt.year, target_dt.month, 1)
            for _ in range(14):
                month_dates = []
                months = picker.locator(".month")
                for idx in range(await months.count()):
                    try:
                        month_txt = (await months.nth(idx).inner_text(timeout=1000)).strip()
                        month_dt = parse_picker_month(month_txt)
                        if month_dt:
                            month_dates.append(month_dt)
                    except Exception:
                        pass

                if any(m.year == target_dt.year and m.month == target_dt.month
                       for m in month_dates):
                    break

                if not month_dates:
                    await asyncio.sleep(0.5)
                    continue

                nav_sel = ".next" if target_month > max(month_dates) else ".prev"
                nav = picker.locator(nav_sel).first
                if not await nav.count():
                    print(f"   ⚠️  Date picker navigation missing: {nav_sel}")
                    return False
                await nav.click(timeout=5000, force=True)
                await asyncio.sleep(0.4)
            else:
                print(f"   ⚠️  Could not reach date picker month: {target_dt:%b %Y}")
                return False

            calendar = picker
            calendars = picker.locator(".drp-calendar")
            calendar_count = await calendars.count()
            if calendar_count == 0:
                calendars = picker.locator(".calendar")
                calendar_count = await calendars.count()

            for idx in range(calendar_count):
                candidate = calendars.nth(idx)
                try:
                    month_txt = (await candidate.locator(".month").first.inner_text(timeout=1000)).strip()
                    month_dt = parse_picker_month(month_txt)
                    if (month_dt and month_dt.year == target_dt.year and
                            month_dt.month == target_dt.month):
                        calendar = candidate
                        break
                except Exception:
                    pass

            day_re = re.compile(rf"^\s*{target_dt.day}\s*$")
            cells = calendar.locator("td.available:not(.off):visible").filter(has_text=day_re)

            for idx in range(await cells.count()):
                cell = cells.nth(idx)
                try:
                    if await cell.is_visible(timeout=500):
                        await cell.click(timeout=5000, force=True)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    pass

            clicked = await calendar.evaluate(
                """(root, day) => {
                    const isVisible = (el) => {
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 &&
                               style.display !== 'none' && style.visibility !== 'hidden';
                    };
                    const cells = Array.from(root.querySelectorAll('td.available:not(.off)'));
                    const cell = cells.find((td) =>
                        td.textContent.trim() === String(day) && isVisible(td)
                    );
                    if (!cell) return false;
                    cell.click();
                    return true;
                }""",
                target_dt.day,
            )
            if clicked:
                await asyncio.sleep(0.5)
                return True

            print(f"   ⚠️  Date picker day not clickable: {target_dt:%d %b %Y}")
            return False

        except Exception as exc:
            print(f"   ⚠️  Date picker selection failed: {exc}")
            return False

    async def extract_flights(self, page, date_str, origin, dest) -> list:
        now     = rex_now()
        ck_date = now.strftime("%d-%m-%Y")
        ck_time = now.strftime("%H:%M:%S")
        data, seen = [], set()
        connecting = (origin, dest) in CONNECTING_ROUTES

        # ── STEP 1: Card selectors ─────────────────────────────
        for sel in CARD_SELECTORS:
            rows = await page.query_selector_all(sel)
            if not rows:
                continue
            print(f"   🔍 Card selector '{sel}' → {len(rows)} row(s)")

            for row in rows:
                try:
                    text = (await row.inner_text(timeout=1500)).strip()
                except Exception:
                    continue
                flat  = text.replace("\n", " ")

                # Build ZL→time mapping for this card in one pass
                zl_time_map = extract_times_per_zl(text)
                if not zl_time_map:
                    continue

                price = extract_price_from_card_text(flat)

                for f_no, dep in zl_time_map.items():
                    print(f"      Card: {f_no}  dep={dep}  price={price}")
                    key = f"{f_no}-{dep}"
                    if key not in seen:
                        data.append(self._row(ck_date, ck_time, f_no,
                                              date_str, dep, origin, dest, price))
                        seen.add(key)

            if data:
                print(f"   ✅ Card extraction done: {len(data)} flight(s)")
                return data

        # ── STEP 2: Body text scan (fallback) ─────────────────
        print("   ⚠️ No card selector matched — body text scan (ribbon-aware).")
        try:
            full_body = await page.inner_text("body", timeout=5000)
        except Exception as exc:
            print(f"   ⚠️ Body text scan failed: {exc}")
            return data

        ribbon_end = find_ribbon_end_position(full_body)
        print(f"   📍 Ribbon area ends at ~char {ribbon_end}")

        zl_matches = list(re.finditer(r'ZL\s?\d{3,4}', full_body))
        print(f"   🔍 Total ZL matches: {len(zl_matches)} "
              f"(ribbon cutoff pos: {ribbon_end})")

        flights_raw = []
        # Body scan: saare times collect karo ribbon ke baad
        body_after_ribbon = full_body[ribbon_end:]
        all_body_times = extract_all_times_from_text(body_after_ribbon)
        zl_in_body = [m for m in zl_matches if m.start() >= ribbon_end]

        for idx, m in enumerate(zl_in_body):
            f_no = re.sub(r'\s', '', m.group())

            # Same logic: N-ve flight ka dep time = times[N*2]
            dep_idx = idx * 2
            dep = all_body_times[dep_idx] if dep_idx < len(all_body_times) else "-"

            key = f"{f_no}-{dep}"
            if key in seen:
                continue

            price = extract_price_from_flight_window(full_body, m, ribbon_end)
            if price is None:
                continue

            flights_raw.append((f_no, dep, key, price))
            seen.add(key)

        if not flights_raw:
            return data

        print(f"   🔍 Body scan (post-ribbon) found: {[f[0] for f in flights_raw]}")

        if connecting:
            flight_area = full_body[ribbon_end:]
            m_fp = re.search(r'[Ff]rom\s*\$\s*([\d,]+\.\d{2})', flight_area)
            combined_price = (f"${m_fp.group(1)}" if m_fp
                              else self._last_ribbon_price or "N/A")
            print(f"   🔗 Connecting route — combined price: {combined_price}")
            for f_no, dep, key, _ in flights_raw:
                data.append(self._row(ck_date, ck_time, f_no,
                                      date_str, dep, origin, dest, combined_price))
        else:
            for f_no, dep, key, price in flights_raw:
                print(f"      ✈️  {f_no}  {dep} → {price}")
                data.append(self._row(ck_date, ck_time, f_no,
                                      date_str, dep, origin, dest, price))

        return data

    def _row(self, ck_date, ck_time, airline, dep_date,
             dep_time, orig, dest, price, fare_class="Economy",
             source="Rex Website", status=STATUS_SUCCESS, comment="",
             retry_count=0, debug_artifacts=""):
        # Use RUN_ID at call time (not class-init time) so it reflects any
        # CLI override that happened after module import.
        current_run_id = RUN_ID
        return {
            "Date Checked":      ck_date,
            "Time Checked":      ck_time,
            "Airline":           airline,
            "Date of Departure": dep_date,
            "Time of Departure": dep_time,   # Raw as-is from website
            "Origin":            orig,
            "Destination":       dest,
            "Fare Price":        price,
            "Fare Class":        fare_class,
            "Source":            source,
            "Run ID":            current_run_id,
            "Status":            status,
            "Comment":           comment,
            "Retry Count":       retry_count,
            "Debug Artifacts":    debug_artifacts,
        }

    def _no_flight(self, date_str, orig, dest, comment="Rex reported no fare/flight available for this date"):
        now = rex_now()
        return self._row(now.strftime("%d-%m-%Y"), now.strftime("%H:%M:%S"),
                         "no flight", date_str, "-", orig, dest, "N/A",
                         fare_class="", status=STATUS_NO_FARE, comment=comment)

    def _site_unavailable(self, date_str, orig, dest, reason="Rex site unavailable"):
        now = rex_now()
        row = self._row(now.strftime("%d-%m-%Y"), now.strftime("%H:%M:%S"),
                        "site unavailable", date_str, "-", orig, dest, "N/A",
                        fare_class="", status=STATUS_SITE_UNAVAILABLE,
                        comment=reason)
        row["Source"] = "Rex Website - unavailable"
        return row

    def _failed_job_row(self, date_str, orig, dest, status, comment,
                        retry_count=0, debug_artifacts: list[str] | str | None = None):
        now = rex_now()
        if isinstance(debug_artifacts, list):
            debug_text = " | ".join(debug_artifacts)
        else:
            debug_text = debug_artifacts or ""
        return self._row(
            now.strftime("%d-%m-%Y"),
            now.strftime("%H:%M:%S"),
            "scrape failed",
            date_str,
            "-",
            orig,
            dest,
            "N/A",
            fare_class="",
            source="Rex Website - failed",
            status=status,
            comment=comment,
            retry_count=retry_count,
            debug_artifacts=debug_text,
        )

    def write_unavailable_route_rows(self, origin_code, dest_code, reason: str):
        print(f"   🧾 Writing unavailable rows: {reason}")
        rows = [
            self._site_unavailable(dt.strftime("%d-%m-%Y"), origin_code, dest_code, reason)
            for dt in build_date_list()
        ]
        append_rows(rows)

    # ─────────────────────────────────────────────────────────
    #  FRESH SEARCH (jab ribbon mein date nahi milti)
    # ─────────────────────────────────────────────────────────
    async def submit_search_form(self, page) -> bool:
        submit_selector = "#ContentPlaceHolder1_BookingHomepageV21_SubmitBooking"
        try:
            await page.locator(submit_selector).click(timeout=15000, force=True)
            return True
        except Exception as exc:
            print(f"   ⚠️  Submit button click failed; using postback fallback: {exc}")

        try:
            return await page.evaluate(
                """() => {
                    if (typeof SubmitBookingForm === 'function' && SubmitBookingForm() === false) {
                        return false;
                    }
                    if (typeof __doPostBack === 'function') {
                        __doPostBack('ctl00$ContentPlaceHolder1$BookingHomepageV21$SubmitBooking', '');
                        return true;
                    }
                    const btn = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_SubmitBooking');
                    if (!btn) return false;
                    btn.click();
                    return true;
                }"""
            )
        except Exception as exc:
            print(f"   ❌ Submit fallback failed: {exc}")
            return False

    async def submit_route_search_once(self, page, origin_code: str, dest_code: str,
                                       target_dt: datetime, label_prefix: str) -> SearchOutcome:
        """Fill Rex homepage for one route/date and submit the availability search."""
        if not await self.open_rex_home(page, f"{label_prefix}_home"):
            return SearchOutcome(False, STATUS_SITE_UNAVAILABLE,
                                 "Rex homepage did not load or booking form was not ready",
                                 retryable=True)

        if not await self.click_one_way(page, f"{label_prefix}_one_way_failed"):
            return SearchOutcome(False, STATUS_STRUCTURE,
                                 "Could not select one-way trip type",
                                 retryable=True)

        if not await self.select_route_fields(page, origin_code, dest_code):
            return SearchOutcome(False, STATUS_STRUCTURE,
                                 "Could not select origin/destination from Rex form",
                                 retryable=True)

        if not await self.select_departure_date(page, target_dt):
            return SearchOutcome(False, STATUS_STRUCTURE,
                                 "Could not set departure date on Rex form",
                                 retryable=True)

        if not await self.submit_search_form(page):
            return SearchOutcome(False, STATUS_STRUCTURE,
                                 "Could not submit Rex booking form",
                                 retryable=True)

        print("   ⏳ Bright Data CAPTCHA solver active — auto-solving Google Captcha...")
        await self.wait_for_brightdata_captcha(page, detect_timeout=60000)
        await self.click_continue_if_present(page)
        return SearchOutcome(True, "SUBMITTED", "Rex search form submitted", retryable=True)

    def _captcha_page_recovery_action(self, recovery_no: int) -> str:
        strategy = self.captcha_recovery_strategy
        if strategy in {"none", "new-context"}:
            return strategy
        if strategy == "reload":
            return "reload"
        if strategy == "reload-then-refill":
            return "reload" if recovery_no == 1 else "refill"
        return "refill"

    async def wait_for_search_result_with_recovery(
        self,
        page,
        origin_code: str,
        dest_code: str,
        target_dt: datetime,
        timeout: int,
        label_prefix: str,
    ) -> SearchOutcome:
        """Wait for availability, recovering when Rex reCAPTCHA stays disabled."""
        recovery_no = 0
        last_outcome = SearchOutcome(False, STATUS_TIMEOUT, "Search wait did not run", retryable=True)
        recovery_artifacts: list[str] = []

        while True:
            outcome = await self.wait_for_search_result(page, target_dt, timeout=timeout)
            last_outcome = outcome
            if outcome.status != STATUS_BLOCKED or not outcome.retryable:
                outcome.debug_artifacts.extend(recovery_artifacts)
                return outcome
            if self.captcha_recovery_strategy == "none" or recovery_no >= self.captcha_recovery_max:
                outcome.debug_artifacts.extend(recovery_artifacts)
                return outcome

            recovery_no += 1
            action = self._captcha_page_recovery_action(recovery_no)
            if action in {"none", "new-context"}:
                outcome.debug_artifacts.extend(recovery_artifacts)
                return outcome

            date_str = output_date(target_dt)
            print(
                "   🔄 Rex verification recovery "
                f"{recovery_no}/{self.captcha_recovery_max}: {action} "
                f"for {origin_code}->{dest_code} {date_str}"
            )
            artifacts = await self.save_debug_artifacts(
                page,
                f"{label_prefix}_{origin_code}_{dest_code}_{date_str}_captcha_recovery_{recovery_no}",
                {
                    "run_id": RUN_ID,
                    "origin": origin_code,
                    "destination": dest_code,
                    "departure_date": date_str,
                    "status": outcome.status,
                    "reason": outcome.reason,
                    "recovery_no": recovery_no,
                    "recovery_action": action,
                    "captcha_disabled_threshold": self.captcha_disabled_threshold,
                },
            )
            last_outcome.debug_artifacts.extend(artifacts)
            recovery_artifacts.extend(artifacts)

            if action == "reload":
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=70000)
                except Exception as exc:
                    print(f"   ⚠️  Captcha recovery reload failed: {exc}")
                await asyncio.sleep(3)
                continue

            refill_outcome = await self.submit_route_search_once(
                page,
                origin_code,
                dest_code,
                target_dt,
                f"{label_prefix}_captcha_refill_{recovery_no}",
            )
            if not refill_outcome.ok:
                refill_outcome.debug_artifacts.extend(artifacts)
                return refill_outcome
            await asyncio.sleep(3)

        return last_outcome

    async def do_fresh_search(self, page, origin_code: str, dest_code: str,
                               target_dt: datetime) -> SearchOutcome:
        """
        Ribbon se date nahi mili → Rex homepage se fresh search karo.
        Same origin/dest, sirf date change.

        Steps:
          1. rex.com.au pe jao
          2. One-way select karo
          3. Origin/Dest fill karo
          4. Target date datepicker se select karo
          5. Search submit karo
          6. Flight list load hone ka wait karo

        Returns structured status so failures are not mis-recorded as "no flight".
        """
        origin_name = AIRPORT_MAP.get(origin_code, origin_code)
        dest_name = AIRPORT_MAP.get(dest_code, dest_code)
        print(f"   🔄 Fresh search: {origin_code} ({origin_name}) → {dest_code} ({dest_name}) "
              f"on {target_dt.strftime('%d %b %Y')}")
        try:
            submit = await self.submit_route_search_once(
                page,
                origin_code,
                dest_code,
                target_dt,
                "fresh_search",
            )
            if not submit.ok:
                return submit

            outcome = await self.wait_for_search_result_with_recovery(
                page,
                origin_code,
                dest_code,
                target_dt,
                timeout=120,
                label_prefix="fresh_search",
            )
            await asyncio.sleep(1)
            print(f"   ✅ Fresh search finished — status={outcome.status}, loaded={outcome.loaded}")
            return outcome

        except Exception as e:
            print(f"   ❌ Fresh search failed: {e}")
            return SearchOutcome(False, STATUS_FAILED, f"Fresh search exception: {e}", retryable=True)

    def _apply_row_metadata(self, rows: list[dict], status: str, comment: str,
                            retry_count: int, debug_artifacts: list[str] | None = None):
        debug_text = " | ".join(debug_artifacts or [])
        for row in rows:
            row["Run ID"] = RUN_ID
            row["Status"] = status
            row["Comment"] = comment
            row["Retry Count"] = retry_count
            row["Debug Artifacts"] = debug_text

    def _backoff_delay(self, attempt: int) -> float:
        if self.retry_backoff <= 0:
            return 0.0
        jitter = random.uniform(0.0, min(3.0, self.retry_backoff))
        return min(self.max_retry_backoff, self.retry_backoff * (2 ** max(0, attempt - 1)) + jitter)

    # Keywords that indicate the Bright Data WebSocket session has been dropped.
    _BROWSER_CLOSED_MARKERS = (
        "target page, context or browser has been closed",
        "browser has been closed",
        "connection closed",
        "websocket",
        "target closed",
    )

    def _is_browser_closed_error(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return any(marker in msg for marker in self._BROWSER_CLOSED_MARKERS)

    async def _reconnect_browser(self) -> Browser | None:
        """
        Re-establish the Bright Data CDP connection when the current browser
        session has been killed (e.g. after a prolonged CAPTCHA storm).
        Returns the new browser object, or None on failure.
        """
        if not self._pw_instance:
            print("   ❌ Cannot reconnect — playwright instance not stored (run_route must set self._pw_instance)")
            return None
        print("   🔌 Browser session dead — attempting reconnect to Bright Data...")
        for attempt in range(1, 4):
            try:
                new_browser = await self._pw_instance.chromium.connect_over_cdp(BD_BROWSER_WSS)
                print(f"   ✅ Bright Data reconnected (attempt {attempt}/3)")
                return new_browser
            except Exception as exc:
                print(f"   ⚠️  Reconnect attempt {attempt}/3 failed: {exc}")
                if attempt < 3:
                    await asyncio.sleep(8 * attempt)
        print("   ❌ All reconnect attempts exhausted — browser unavailable")
        return None

    async def _new_job_context(self, browser):
        """
        Create a new browser context + page.
        If the browser session has been killed by Bright Data, transparently
        reconnect up to 2 times before raising.
        """
        effective_browser = browser
        for reconnect_no in range(3):  # 0 = original, 1 = first reconnect, 2 = second
            try:
                context = await effective_browser.new_context(
                    viewport={"width": 1366, "height": 900},
                    timezone_id=REX_TIMEZONE,
                    locale=REX_LOCALE,
                )
                context.set_default_timeout(30000)
                context.set_default_navigation_timeout(70000)
                page = await context.new_page()
                return context, page
            except Exception as exc:
                if reconnect_no < 2 and self._is_browser_closed_error(exc):
                    new_browser = await self._reconnect_browser()
                    if new_browser:
                        effective_browser = new_browser
                        continue
                raise

    async def scrape_date_once(self, page, origin_code: str, dest_code: str,
                               target_dt: datetime, attempt: int) -> JobResult:
        date_str = output_date(target_dt)
        label_base = f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_attempt_{attempt}"

        outcome = await self.do_fresh_search(page, origin_code, dest_code, target_dt)
        if outcome.status == STATUS_NO_FARE:
            row = self._no_flight(date_str, origin_code, dest_code, outcome.reason)
            self._apply_row_metadata([row], STATUS_NO_FARE, outcome.reason, attempt - 1)
            return JobResult(STATUS_NO_FARE, [row], outcome.reason, retryable=False, retry_count=attempt - 1)

        if not outcome.ok and outcome.status != STATUS_STRUCTURE_VISIBLE:
            artifacts = await self.save_debug_artifacts(
                page,
                f"{label_base}_{outcome.status}",
                {
                    "run_id": RUN_ID,
                    "origin": origin_code,
                    "destination": dest_code,
                    "departure_date": date_str,
                    "attempt": attempt,
                    "status": outcome.status,
                    "reason": outcome.reason,
                },
            )
            return JobResult(
                outcome.status,
                [],
                outcome.reason,
                retryable=outcome.retryable,
                retry_count=attempt - 1,
                debug_artifacts=artifacts,
            )

        flights = await self.extract_flights(page, date_str, origin_code, dest_code)
        real_flights = [row for row in flights if row.get("Airline") != "no flight"]
        if real_flights:
            missing_prices = [
                row for row in real_flights
                if not row.get("Fare Price") or row.get("Fare Price") in {"N/A", "-"}
            ]
            if missing_prices:
                comment = (
                    "Flight rows were parsed, but one or more fare prices were missing. "
                    "This may be a Rex page structure change or a partially loaded fare panel."
                )
                artifacts = await self.save_debug_artifacts(
                    page,
                    f"{label_base}_missing_price",
                    {
                        "run_id": RUN_ID,
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": date_str,
                        "attempt": attempt,
                        "status": STATUS_STRUCTURE,
                        "reason": comment,
                        "parsed_flights": len(real_flights),
                    },
                )
                self._apply_row_metadata(real_flights, STATUS_STRUCTURE, comment, attempt - 1, artifacts)
                return JobResult(
                    STATUS_STRUCTURE,
                    real_flights,
                    comment,
                    retryable=True,
                    retry_count=attempt - 1,
                    debug_artifacts=artifacts,
                )

            comment = "Flight and fare data parsed from Rex availability page"
            self._apply_row_metadata(real_flights, STATUS_SUCCESS, comment, attempt - 1)
            return JobResult(STATUS_SUCCESS, real_flights, comment, retryable=False, retry_count=attempt - 1)

        if await self.page_has_no_fare_signal(page):
            comment = "Rex page explicitly indicates no flight/fare availability"
            row = self._no_flight(date_str, origin_code, dest_code, comment)
            self._apply_row_metadata([row], STATUS_NO_FARE, comment, attempt - 1)
            return JobResult(STATUS_NO_FARE, [row], comment, retryable=False, retry_count=attempt - 1)

        if await self.page_has_rex_server_error(page):
            status = STATUS_SITE_UNAVAILABLE
            comment = "Rex returned a server error after search submission"
        elif await self.page_has_verification_or_block(page):
            status = STATUS_BLOCKED
            comment = "Rex/Bright Data verification or block page detected after search submission"
        elif not await self.page_has_expected_result_structure(page):
            status = STATUS_STRUCTURE
            comment = "Expected Rex availability page structure was not found"
        else:
            status = STATUS_STRUCTURE
            comment = "Rex result structure loaded, but no flight rows or explicit no-fare signal could be parsed"

        artifacts = await self.save_debug_artifacts(
            page,
            f"{label_base}_{status}",
            {
                "run_id": RUN_ID,
                "origin": origin_code,
                "destination": dest_code,
                "departure_date": date_str,
                "attempt": attempt,
                "status": status,
                "reason": comment,
            },
        )
        return JobResult(status, [], comment, retryable=True, retry_count=attempt - 1, debug_artifacts=artifacts)

    async def scrape_job_with_retries(self, browser, origin_code: str, dest_code: str,
                                      target_dt: datetime) -> JobResult:
        date_str = output_date(target_dt)
        last_result: JobResult | None = None

        for attempt in range(1, self.max_attempts + 1):
            context = None
            page = None
            print(f"   🔁 Attempt {attempt}/{self.max_attempts} for {origin_code}->{dest_code} {date_str}")
            try:
                context, page = await self._new_job_context(browser)
                result = await asyncio.wait_for(
                    self.scrape_date_once(page, origin_code, dest_code, target_dt, attempt),
                    timeout=self.job_timeout,
                )
                result.retry_count = attempt - 1
                last_result = result
                if result.completed:
                    return result
                print(f"   ⚠️  Attempt ended with {result.status}: {result.comment}")
                if not result.retryable:
                    return result
            except asyncio.TimeoutError:
                comment = f"Route/date job exceeded hard timeout of {self.job_timeout}s"
                artifacts = []
                if page:
                    artifacts = await self.save_debug_artifacts(
                        page,
                        f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_attempt_{attempt}_hard_timeout",
                        {
                            "run_id": RUN_ID,
                            "origin": origin_code,
                            "destination": dest_code,
                            "departure_date": date_str,
                            "attempt": attempt,
                            "status": STATUS_TIMEOUT,
                            "reason": comment,
                        },
                    )
                last_result = JobResult(
                    STATUS_TIMEOUT,
                    [],
                    comment,
                    retryable=True,
                    retry_count=attempt - 1,
                    debug_artifacts=artifacts,
                )
                print(f"   ⏱️  {comment}")
            except Exception as exc:
                comment = f"Unhandled scrape exception: {exc}"
                artifacts = []
                if page:
                    artifacts = await self.save_debug_artifacts(
                        page,
                        f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_attempt_{attempt}_exception",
                        {
                            "run_id": RUN_ID,
                            "origin": origin_code,
                            "destination": dest_code,
                            "departure_date": date_str,
                            "attempt": attempt,
                            "status": STATUS_FAILED,
                            "reason": comment,
                            "traceback": traceback.format_exc(),
                        },
                    )
                last_result = JobResult(
                    STATUS_FAILED,
                    [],
                    comment,
                    retryable=True,
                    retry_count=attempt - 1,
                    debug_artifacts=artifacts,
                )
                print(f"   ❌ {comment}")
                traceback.print_exc()
            finally:
                if context:
                    try:
                        await asyncio.wait_for(context.close(), timeout=20)
                    except Exception as exc:
                        print(f"   ⚠️  Context cleanup issue: {exc}")

            if attempt < self.max_attempts:
                delay = self._backoff_delay(attempt)
                if delay > 0:
                    print(f"   ⏳ Backing off {delay:.1f}s before retry")
                    await asyncio.sleep(delay)

        comment = "Job failed after configured retries"
        if last_result:
            comment = f"{comment}: {last_result.comment}"
            rows = last_result.rows
            status = last_result.status if last_result.status != STATUS_SUCCESS else STATUS_FAILED
            artifacts = last_result.debug_artifacts
            retry_count = max(0, self.max_attempts - 1)
        else:
            rows = []
            status = STATUS_FAILED
            artifacts = []
            retry_count = max(0, self.max_attempts - 1)

        if rows:
            self._apply_row_metadata(rows, status, comment, retry_count, artifacts)
        return JobResult(status, rows, comment, retryable=True,
                         retry_count=retry_count, debug_artifacts=artifacts)

    # ─────────────────────────────────────────────────────────
    #  RIBBON NAVIGATION (LEGACY — use _run_ribbon_route_production via run_route)
    #  DEPRECATED: This method bypasses OUTPUT_STORE and resume logic.
    #  Kept only for backward-compat. Do NOT call directly in production.
    # ─────────────────────────────────────────────────────────
    async def run_ribbon_route(self, page, origin_code, dest_code):
        print("   ⚠️  WARNING: run_ribbon_route() is deprecated and bypasses OUTPUT_STORE. "
              "Use run_route() instead.")
        all_dates    = build_date_list()
        origin_name  = AIRPORT_MAP.get(origin_code, origin_code)
        dest_name    = AIRPORT_MAP.get(dest_code, dest_code)
        empty_streak = 0  # auto-stop counter

        for idx, target_dt in enumerate(all_dates, 1):
            date_str = target_dt.strftime("%d-%m-%Y")

            print(f"\n{'═'*60}")
            print(f"📅 [{idx}/{TOTAL_DAYS}]  {target_dt.strftime('%A, %d %b %Y')}")
            print(f"{'─'*60}")

            self._last_ribbon_price = ""

            # ── Step 1: Ribbon se try karo ───────────────────
            tab_result = "not_found"
            for attempt in range(30):
                tab_result = await self.click_ribbon_tab(page, target_dt)
                if tab_result in ("clicked", "unavailable"):
                    break
                print(f"   ➡️  Not in view — advancing ribbon (attempt {attempt+1})...")
                moved = await self.go_next_ribbon(page)
                if not moved:
                    print("   ⛔ Ribbon end reached.")
                    tab_result = "ribbon_end"
                    break
                await asyncio.sleep(3)
            else:
                tab_result = "not_found"

            # ── Step 2: Ribbon nahi mili → fresh search ──────
            if tab_result in ("not_found", "ribbon_end"):
                print(f"   🔄 Ribbon mein date nahi mili — fresh search karta hoon...")
                outcome = await self.do_fresh_search(
                    page, origin_code, dest_code, target_dt
                )
                if outcome.status == STATUS_NO_FARE:
                    print("   ❌ Fresh search confirmed no fare/no flight")
                    append_rows([self._no_flight(date_str, origin_code, dest_code, outcome.reason)])
                    continue
                if not outcome.ok:
                    print(f"   ❌ Fresh search failed — {outcome.status}: {outcome.reason}")
                    append_rows([self._failed_job_row(
                        date_str, origin_code, dest_code, outcome.status, outcome.reason
                    )])
                    continue
                # Fresh search ke baad ribbon se ek baar aur try
                tab_result_after = await self.click_ribbon_tab(page, target_dt)
                if tab_result_after == "unavailable":
                    print("   ❌ Fresh search ke baad bhi unavailable — 'no flight'")
                    append_rows([self._no_flight(date_str, origin_code, dest_code)])
                    continue
                # 'clicked' ya 'not_found' — direct extract karo (page already loaded)
                print(f"   ℹ️  Fresh search ke baad tab_result={tab_result_after} — extracting directly")

            elif tab_result == "unavailable":
                print("   ❌ No flight → 'no flight'")
                append_rows([self._no_flight(date_str, origin_code, dest_code)])
                continue

            # ── Step 3: Page load wait + extract ─────────────
            await self.wait_for_flights_loaded(page, target_dt, timeout=15)
            await asyncio.sleep(1)

            flights = await self.extract_flights(page, date_str, origin_code, dest_code)
            real_flights = [f for f in flights if f.get("Airline", "") != "no flight"]
            if real_flights:
                empty_streak = 0
                print(f"   ✅ {len(real_flights)} flight(s):")
                for f in real_flights:
                    print(f"      ✈️  {f['Airline']}  {f['Time of Departure']}  {f['Fare Price']}")
                append_rows(real_flights)
            else:
                empty_streak += 1
                print(f"   ⚠️  0 found → 'no flight'  (empty streak: {empty_streak}/{MAX_EMPTY_STREAK})")
                append_rows([self._no_flight(date_str, origin_code, dest_code)])
                if empty_streak >= MAX_EMPTY_STREAK:
                    print(f"   🛑 AUTO-STOP: {MAX_EMPTY_STREAK} consecutive empty results — route aborted.")
                    break

            print(f"   ✅ {idx}/{TOTAL_DAYS} done.")

    # ─────────────────────────────────────────────────────────
    #  FRESH SEARCH ROUTE (LEGACY — use _run_fresh_route_production via run_route)
    #  DEPRECATED: Bypasses OUTPUT_STORE and resume logic.
    #  Kept only for backward-compat. Do NOT call directly in production.
    # ─────────────────────────────────────────────────────────
    async def run_fresh_search_route(self, page, origin_code, dest_code):
        print("   ⚠️  WARNING: run_fresh_search_route() is deprecated and bypasses OUTPUT_STORE. "
              "Use run_route() instead.")
        all_dates   = build_date_list()
        origin_name = AIRPORT_MAP.get(origin_code, origin_code)
        dest_name   = AIRPORT_MAP.get(dest_code, dest_code)
        empty_streak = 0  # auto-stop counter

        for idx, target_dt in enumerate(all_dates, 1):
            date_str = target_dt.strftime("%d-%m-%Y")

            print(f"\n{'═'*60}")
            print(f"📅 [{idx}/{TOTAL_DAYS}]  {target_dt.strftime('%A, %d %b %Y')}")
            print(f"{'─'*60}")

            self._last_ribbon_price = ""

            # Ribbon nahi — seedha fresh search
            outcome = await self.do_fresh_search(
                page, origin_code, dest_code, target_dt
            )
            if outcome.status == STATUS_NO_FARE:
                print("   ❌ Fresh search confirmed no fare/no flight")
                append_rows([self._no_flight(date_str, origin_code, dest_code, outcome.reason)])
                continue
            if not outcome.ok:
                print(f"   ❌ Fresh search failed — {outcome.status}: {outcome.reason}")
                append_rows([self._failed_job_row(
                    date_str, origin_code, dest_code, outcome.status, outcome.reason
                )])
                continue

            flights = await self.extract_flights(page, date_str, origin_code, dest_code)
            real_flights = [f for f in flights if f.get("Airline", "") != "no flight"]
            if real_flights:
                empty_streak = 0
                print(f"   ✅ {len(real_flights)} flight(s):")
                for f in real_flights:
                    print(f"      ✈️  {f['Airline']}  {f['Time of Departure']}  {f['Fare Price']}")
                append_rows(real_flights)
            else:
                empty_streak += 1
                print(f"   ⚠️  0 found — 'no flight'  (empty streak: {empty_streak}/{MAX_EMPTY_STREAK})")
                append_rows([self._no_flight(date_str, origin_code, dest_code)])
                if empty_streak >= MAX_EMPTY_STREAK:
                    print(f"   🛑 AUTO-STOP: {MAX_EMPTY_STREAK} consecutive empty results — route aborted.")
                    break

            print(f"   ✅ {idx}/{TOTAL_DAYS} done.")

    # ─────────────────────────────────────────────────────────
    #  ROUTE RUNNER
    # ─────────────────────────────────────────────────────────
    def _write_result(self, origin_code: str, dest_code: str, date_str: str,
                      result: JobResult):
        rows = result.rows
        if not rows:
            rows = [
                self._failed_job_row(
                    date_str,
                    origin_code,
                    dest_code,
                    result.status,
                    result.comment or "Job failed without parsed rows",
                    result.retry_count,
                    result.debug_artifacts,
                )
            ]
        if OUTPUT_STORE:
            OUTPUT_STORE.write_job_rows(origin_code, dest_code, date_str, rows)
        else:
            append_rows(rows)

    async def setup_route_page_once(self, page, origin_code: str, dest_code: str,
                                    initial_dt: datetime) -> SearchOutcome:
        """Open Rex once, select the route/date once, and land on the availability page."""
        submit = await self.submit_route_search_once(
            page,
            origin_code,
            dest_code,
            initial_dt,
            "route_once",
        )
        if not submit.ok:
            return submit

        return await self.wait_for_search_result_with_recovery(
            page,
            origin_code,
            dest_code,
            initial_dt,
            timeout=150,
            label_prefix="route_once",
        )

    async def _close_context_safely(self, context, label: str = "context"):
        if context:
            try:
                await context.close()
            except Exception as exc:
                print(f"   ⚠️  {label} cleanup issue: {exc}")

    async def _setup_route_context_with_retries(
        self,
        browser,
        origin_code: str,
        dest_code: str,
        initial_dt: datetime,
        label_prefix: str,
    ):
        """Create a context and setup the route/date, retrying stuck captcha contexts."""
        last_outcome: SearchOutcome | None = None
        date_str = output_date(initial_dt)
        # Track which browser we're using — may be swapped for a fresh Bright Data
        # connection if captcha quota is exhausted on the current IP.
        effective_browser = browser

        for attempt in range(1, self.max_attempts + 1):
            context = None
            page = None
            print(
                f"   🔁 Route setup attempt {attempt}/{self.max_attempts} "
                f"for {origin_code}->{dest_code} {date_str}"
            )
            try:
                context, page = await self._new_job_context(effective_browser)
                setup = await asyncio.wait_for(
                    self.setup_route_page_once(page, origin_code, dest_code, initial_dt),
                    timeout=self.job_timeout,
                )
                last_outcome = setup
                if setup.ok:
                    return context, page, setup

                artifacts = await self.save_debug_artifacts(
                    page,
                    f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_{label_prefix}_attempt_{attempt}_{setup.status}",
                    {
                        "run_id": RUN_ID,
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": date_str,
                        "attempt": attempt,
                        "status": setup.status,
                        "reason": setup.reason,
                        "label": label_prefix,
                    },
                )
                setup.debug_artifacts.extend(artifacts)
                print(f"   ⚠️  Route setup attempt failed: {setup.status} — {setup.reason}")
                if not setup.retryable:
                    # Non-retryable: close context to avoid leak, caller gets None,None
                    await self._close_context_safely(context, "non-retryable route setup context")
                    return None, None, setup

            except asyncio.TimeoutError:
                reason = f"Route setup exceeded hard timeout of {self.job_timeout}s"
                artifacts = []
                if page:
                    artifacts = await self.save_debug_artifacts(
                        page,
                        f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_{label_prefix}_attempt_{attempt}_timeout",
                        {
                            "run_id": RUN_ID,
                            "origin": origin_code,
                            "destination": dest_code,
                            "departure_date": date_str,
                            "attempt": attempt,
                            "status": STATUS_TIMEOUT,
                            "reason": reason,
                            "label": label_prefix,
                        },
                    )
                last_outcome = SearchOutcome(False, STATUS_TIMEOUT, reason, retryable=True,
                                             debug_artifacts=artifacts)
                print(f"   ⏱️  {reason}")
            except Exception as exc:
                reason = f"Route setup exception: {exc}"
                artifacts = []
                if page:
                    artifacts = await self.save_debug_artifacts(
                        page,
                        f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_{label_prefix}_attempt_{attempt}_exception",
                        {
                            "run_id": RUN_ID,
                            "origin": origin_code,
                            "destination": dest_code,
                            "departure_date": date_str,
                            "attempt": attempt,
                            "status": STATUS_FAILED,
                            "reason": reason,
                            "traceback": traceback.format_exc(),
                            "label": label_prefix,
                        },
                    )
                last_outcome = SearchOutcome(False, STATUS_FAILED, reason, retryable=True,
                                             debug_artifacts=artifacts)
                print(f"   ❌ {reason}")

            await self._close_context_safely(context, "failed route setup context")
            if attempt < self.max_attempts:
                # If captcha quota is exhausted, reconnect to Bright Data so the next
                # attempt gets a fresh IP with a clean reCAPTCHA quota.
                if last_outcome and last_outcome.status == STATUS_BLOCKED:
                    print(
                        f"   🔄 Captcha quota exhausted on attempt {attempt} — "
                        "reconnecting Bright Data for a fresh IP..."
                    )
                    new_browser = await self._reconnect_browser()
                    if new_browser:
                        effective_browser = new_browser
                        print("   ✅ Fresh Bright Data session — next attempt will use new IP")
                    else:
                        print("   ⚠️  Reconnect failed — retrying with same browser")
                delay = self._backoff_delay(attempt)
                if delay > 0:
                    print(f"   ⏳ Backing off {delay:.1f}s before route setup retry")
                    await asyncio.sleep(delay)

        if not last_outcome:
            last_outcome = SearchOutcome(False, STATUS_FAILED,
                                         "Route setup did not produce a result",
                                         retryable=True)
        return None, None, last_outcome

    async def extract_current_page_result(self, page, origin_code: str, dest_code: str,
                                          target_dt: datetime, attempt: int,
                                          label_prefix: str,
                                          require_date_sync: bool = True) -> JobResult:
        date_str = output_date(target_dt)

        if await self.page_has_rex_server_error(page):
            comment = "Rex returned a server error while reading the availability page"
            artifacts = await self.save_debug_artifacts(
                page,
                f"{label_prefix}_{STATUS_SITE_UNAVAILABLE}",
                {
                    "run_id": RUN_ID,
                    "origin": origin_code,
                    "destination": dest_code,
                    "departure_date": date_str,
                    "attempt": attempt,
                    "status": STATUS_SITE_UNAVAILABLE,
                    "reason": comment,
                },
            )
            return JobResult(STATUS_SITE_UNAVAILABLE, [], comment, True, attempt - 1, artifacts)

        if await self.page_has_rex_verification_page(page):
            outcome = await self.wait_for_search_result_with_recovery(
                page,
                origin_code,
                dest_code,
                target_dt,
                timeout=90,
                label_prefix=f"{label_prefix}_verification",
            )
            if not outcome.ok and outcome.status != STATUS_STRUCTURE_VISIBLE:
                artifacts = list(outcome.debug_artifacts)
                artifacts.extend(await self.save_debug_artifacts(
                    page,
                    f"{label_prefix}_{STATUS_BLOCKED}",
                    {
                        "run_id": RUN_ID,
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": date_str,
                        "attempt": attempt,
                        "status": outcome.status,
                        "reason": outcome.reason,
                    },
                ))
                return JobResult(
                    outcome.status,
                    [],
                    outcome.reason,
                    outcome.retryable,
                    attempt - 1,
                    artifacts,
                )

        if await self.page_has_verification_or_block(page):
            comment = "Rex/Bright Data verification or block page detected while reading availability"
            artifacts = await self.save_debug_artifacts(
                page,
                f"{label_prefix}_{STATUS_BLOCKED}",
                {
                    "run_id": RUN_ID,
                    "origin": origin_code,
                    "destination": dest_code,
                    "departure_date": date_str,
                    "attempt": attempt,
                    "status": STATUS_BLOCKED,
                    "reason": comment,
                },
            )
            return JobResult(STATUS_BLOCKED, [], comment, True, attempt - 1, artifacts)

        loaded = await self.wait_for_flights_loaded(
            page,
            target_dt,
            timeout=30,
            strict_date=require_date_sync,
        )
        if not loaded and require_date_sync and not await self.page_has_no_fare_signal(page):
            comment = "Ribbon page did not confirm the requested departure date before extraction"
            artifacts = await self.save_debug_artifacts(
                page,
                f"{label_prefix}_{STATUS_STRUCTURE}_date_not_synced",
                {
                    "run_id": RUN_ID,
                    "origin": origin_code,
                    "destination": dest_code,
                    "departure_date": date_str,
                    "attempt": attempt,
                    "status": STATUS_STRUCTURE,
                    "reason": comment,
                },
            )
            return JobResult(STATUS_STRUCTURE, [], comment, True, attempt - 1, artifacts)

        flights = await self.extract_flights(page, date_str, origin_code, dest_code)
        real_flights = [row for row in flights if row.get("Airline") != "no flight"]
        if real_flights:
            missing_prices = [
                row for row in real_flights
                if not row.get("Fare Price") or row.get("Fare Price") in {"N/A", "-"}
            ]
            if missing_prices:
                comment = (
                    "Flight rows were parsed, but one or more fare prices were missing. "
                    "This may be a Rex page structure change or a partially loaded fare panel."
                )
                artifacts = await self.save_debug_artifacts(
                    page,
                    f"{label_prefix}_missing_price",
                    {
                        "run_id": RUN_ID,
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": date_str,
                        "attempt": attempt,
                        "status": STATUS_STRUCTURE,
                        "reason": comment,
                        "parsed_flights": len(real_flights),
                    },
                )
                self._apply_row_metadata(real_flights, STATUS_STRUCTURE, comment, attempt - 1, artifacts)
                return JobResult(STATUS_STRUCTURE, real_flights, comment, True, attempt - 1, artifacts)

            comment = "Flight and fare data parsed from Rex availability ribbon page"
            self._apply_row_metadata(real_flights, STATUS_SUCCESS, comment, attempt - 1)
            return JobResult(STATUS_SUCCESS, real_flights, comment, False, attempt - 1)

        if await self.page_has_no_fare_signal(page):
            comment = "Rex page explicitly indicates no flight/fare availability"
            row = self._no_flight(date_str, origin_code, dest_code, comment)
            self._apply_row_metadata([row], STATUS_NO_FARE, comment, attempt - 1)
            return JobResult(STATUS_NO_FARE, [row], comment, False, attempt - 1)

        if not await self.page_has_expected_result_structure(page):
            status = STATUS_STRUCTURE
            comment = "Expected Rex availability page structure was not found"
        else:
            status = STATUS_STRUCTURE
            comment = "Rex availability page loaded, but no flight rows or explicit no-fare signal could be parsed"

        artifacts = await self.save_debug_artifacts(
            page,
            f"{label_prefix}_{status}",
            {
                "run_id": RUN_ID,
                "origin": origin_code,
                "destination": dest_code,
                "departure_date": date_str,
                "attempt": attempt,
                "status": status,
                "reason": comment,
            },
        )
        return JobResult(status, [], comment, True, attempt - 1, artifacts)

    async def navigate_ribbon_to_date(self, page, target_dt: datetime,
                                      max_moves: int = 30) -> str:
        for attempt in range(max_moves):
            tab_result = await self.click_ribbon_tab(page, target_dt)
            if tab_result in ("clicked", "unavailable"):
                return tab_result

            print(f"   ➡️  Date not visible in ribbon — advancing (attempt {attempt + 1}/{max_moves})...")
            moved = await self.go_next_ribbon(page)
            if not moved:
                print("   ⛔ Ribbon end reached or next button unavailable.")
                return "ribbon_end"

        return "not_found"

    async def scrape_ribbon_date_once(self, page, origin_code: str, dest_code: str,
                                      target_dt: datetime, attempt: int) -> JobResult:
        date_str = output_date(target_dt)
        label = f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_ribbon_attempt_{attempt}"
        self._last_ribbon_price = ""

        tab_result = await self.navigate_ribbon_to_date(page, target_dt)
        if tab_result == "unavailable":
            comment = "Rex date ribbon marks this date unavailable"
            row = self._no_flight(date_str, origin_code, dest_code, comment)
            self._apply_row_metadata([row], STATUS_NO_FARE, comment, attempt - 1)
            return JobResult(STATUS_NO_FARE, [row], comment, False, attempt - 1)

        if tab_result not in {"clicked"}:
            comment = f"Target date was not reachable in Rex date ribbon: {tab_result}"
            artifacts = await self.save_debug_artifacts(
                page,
                f"{label}_{STATUS_STRUCTURE}_{tab_result}",
                {
                    "run_id": RUN_ID,
                    "origin": origin_code,
                    "destination": dest_code,
                    "departure_date": date_str,
                    "attempt": attempt,
                    "status": STATUS_STRUCTURE,
                    "reason": comment,
                },
            )
            return JobResult(STATUS_STRUCTURE, [], comment, True, attempt - 1, artifacts)

        try:
            await page.wait_for_load_state("domcontentloaded", timeout=15000)
        except Exception:
            pass
        await self.click_continue_if_present(page)
        return await self.extract_current_page_result(
            page,
            origin_code,
            dest_code,
            target_dt,
            attempt,
            label,
            require_date_sync=True,
        )

    async def _scrape_ribbon_date_with_timeout(
        self,
        page,
        origin_code: str,
        dest_code: str,
        target_dt: datetime,
        attempt: int,
    ) -> JobResult:
        date_str = output_date(target_dt)
        try:
            return await asyncio.wait_for(
                self.scrape_ribbon_date_once(page, origin_code, dest_code, target_dt, attempt=attempt),
                timeout=self.job_timeout,
            )
        except asyncio.TimeoutError:
            comment = f"Ribbon route/date job exceeded hard timeout of {self.job_timeout}s"
            artifacts = []
            if page:
                artifacts = await self.save_debug_artifacts(
                    page,
                    f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_ribbon_timeout_attempt_{attempt}",
                    {
                        "run_id": RUN_ID,
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": date_str,
                        "attempt": attempt,
                        "status": STATUS_TIMEOUT,
                        "reason": comment,
                    },
                )
            return JobResult(STATUS_TIMEOUT, [], comment, True, attempt - 1, artifacts)
        except Exception as exc:
            comment = f"Ribbon scrape exception: {exc}"
            artifacts = []
            if page:
                artifacts = await self.save_debug_artifacts(
                    page,
                    f"{RUN_ID}_{origin_code}_{dest_code}_{date_str}_ribbon_exception_attempt_{attempt}",
                    {
                        "run_id": RUN_ID,
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": date_str,
                        "attempt": attempt,
                        "status": STATUS_FAILED,
                        "reason": comment,
                        "traceback": traceback.format_exc(),
                    },
                )
            return JobResult(STATUS_FAILED, [], comment, True, attempt - 1, artifacts)

    async def _recover_ribbon_date_after_block(
        self,
        browser,
        context,
        page,
        origin_code: str,
        dest_code: str,
        target_dt: datetime,
        failed_result: JobResult,
    ):
        """For a stuck verification page, rebuild the route at the same date and retry once."""
        if failed_result.status != STATUS_BLOCKED:
            return context, page, failed_result
        if self.captcha_recovery_strategy == "none" or self.captcha_recovery_max <= 0:
            return context, page, failed_result

        date_str = output_date(target_dt)
        print(
            "   🔄 Captcha recovery: rebuilding route page for same date "
            f"{origin_code}->{dest_code} {date_str}"
        )
        await self._close_context_safely(context, "blocked ribbon context")
        new_context, new_page, setup = await self._setup_route_context_with_retries(
            browser,
            origin_code,
            dest_code,
            target_dt,
            "ribbon_same_date_recovery",
        )
        if not setup.ok:
            return new_context, new_page, JobResult(
                setup.status,
                [],
                f"Same-date captcha recovery setup failed: {setup.reason}",
                setup.retryable,
                max(failed_result.retry_count + 1, 1),
                setup.debug_artifacts or failed_result.debug_artifacts,
            )

        retry_result = await self._scrape_ribbon_date_with_timeout(
            new_page,
            origin_code,
            dest_code,
            target_dt,
            attempt=max(failed_result.retry_count + 2, 2),
        )
        retry_result.retry_count = max(retry_result.retry_count, failed_result.retry_count + 1)
        return new_context, new_page, retry_result

    async def _run_ribbon_route_production(self, browser, origin_code: str, dest_code: str,
                                           dates: list[datetime]):
        failed_jobs: list[datetime] = []
        context = None
        page = None
        needs_route_refresh = False

        try:
            context, page, setup = await self._setup_route_context_with_retries(
                browser,
                origin_code,
                dest_code,
                dates[0],
                "route_setup",
            )
            if not setup.ok:
                for target_dt in dates:
                    result = JobResult(
                        setup.status,
                        [],
                        setup.reason,
                        setup.retryable,
                        max(0, self.max_attempts - 1),
                        setup.debug_artifacts,
                    )
                    self._write_result(origin_code, dest_code, output_date(target_dt), result)
                return

            for idx, target_dt in enumerate(dates, 1):
                date_str = output_date(target_dt)
                print(f"\n{'═'*60}")
                print(f"📅 [{idx}/{len(dates)}]  {target_dt.strftime('%A, %d %b %Y')}")
                print(f"{'─'*60}")

                if self.resume and OUTPUT_STORE and OUTPUT_STORE.job_completed(origin_code, dest_code, date_str):
                    print(f"   ↩️  Resume: already completed for run {RUN_ID}; skipping.")
                    continue

                if needs_route_refresh or not page:
                    await self._close_context_safely(context, "stale ribbon context")
                    context, page, setup = await self._setup_route_context_with_retries(
                        browser,
                        origin_code,
                        dest_code,
                        target_dt,
                        "route_refresh",
                    )
                    needs_route_refresh = False
                    if not setup.ok:
                        result = JobResult(
                            setup.status,
                            [],
                            f"Route refresh failed before this date: {setup.reason}",
                            setup.retryable,
                            max(0, self.max_attempts - 1),
                            setup.debug_artifacts,
                        )
                        self._write_result(origin_code, dest_code, date_str, result)
                        print(f"   ⚠️  Recorded route refresh failure: {result.status} — {result.comment}")
                        failed_jobs.append(target_dt)
                        needs_route_refresh = True
                        continue

                result = await self._scrape_ribbon_date_with_timeout(
                    page,
                    origin_code,
                    dest_code,
                    target_dt,
                    attempt=1,
                )

                context, page, result = await self._recover_ribbon_date_after_block(
                    browser,
                    context,
                    page,
                    origin_code,
                    dest_code,
                    target_dt,
                    result,
                )

                if result.status in {STATUS_BLOCKED, STATUS_TIMEOUT, STATUS_FAILED}:
                    needs_route_refresh = True
                elif result.status == STATUS_STRUCTURE and result.retryable:
                    needs_route_refresh = True
                else:
                    needs_route_refresh = False

                self._write_result(origin_code, dest_code, date_str, result)
                if result.completed:
                    if result.status == STATUS_SUCCESS:
                        print(f"   ✅ {len(result.rows)} flight row(s) saved.")
                    else:
                        print(f"   ℹ️  No fare: {result.comment}")
                else:
                    cond = result.status
                    reason = result.comment or "Unknown"
                    print(f"   ❌ {cond} — {reason}")
                    failed_jobs.append(target_dt)

        finally:
            await self._close_context_safely(context, "route context")

        for retry_round in range(1, self.final_retry_rounds + 1):
            retry_targets = [
                dt for dt in failed_jobs
                if not (OUTPUT_STORE and OUTPUT_STORE.job_completed(origin_code, dest_code, output_date(dt)))
            ]
            if not retry_targets:
                break

            print(f"\n{'═'*60}")
            print(f"🔁 Ribbon final retry round {retry_round}/{self.final_retry_rounds}: {len(retry_targets)} job(s)")
            print(f"{'═'*60}")
            still_failed: list[datetime] = []

            context = None
            page = None
            try:
                context, page, setup = await self._setup_route_context_with_retries(
                    browser,
                    origin_code,
                    dest_code,
                    retry_targets[0],
                    f"final_retry_{retry_round}_setup",
                )
                if not setup.ok:
                    raise RuntimeError(setup.reason)

                for target_dt in retry_targets:
                    date_str = output_date(target_dt)
                    if not page:
                        context, page, setup = await self._setup_route_context_with_retries(
                            browser,
                            origin_code,
                            dest_code,
                            target_dt,
                            f"final_retry_{retry_round}_refresh",
                        )
                        if not setup.ok:
                            result = JobResult(
                                setup.status,
                                [],
                                f"Final retry route refresh failed: {setup.reason}",
                                setup.retryable,
                                retry_round,
                                setup.debug_artifacts,
                            )
                            self._write_result(origin_code, dest_code, date_str, result)
                            print(
                                f"   ⚠️  Final retry still failing "
                                f"{origin_code}->{dest_code} {date_str}: {result.status}"
                            )
                            still_failed.append(target_dt)
                            continue

                    result = await self._scrape_ribbon_date_with_timeout(
                        page,
                        origin_code,
                        dest_code,
                        target_dt,
                        attempt=retry_round + 1,
                    )

                    context, page, result = await self._recover_ribbon_date_after_block(
                        browser,
                        context,
                        page,
                        origin_code,
                        dest_code,
                        target_dt,
                        result,
                    )

                    if result.status in {STATUS_BLOCKED, STATUS_TIMEOUT, STATUS_FAILED}:
                        await self._close_context_safely(context, "unusable final retry context")
                        context = None
                        page = None

                    self._write_result(origin_code, dest_code, date_str, result)
                    if result.completed:
                        print(f"   ✅ Retry OK: {origin_code}→{dest_code} {date_str}")
                    else:
                        cond = result.status
                        reason = result.comment or "Unknown"
                        print(f"   ❌ Retry {cond} — {reason}")
                        still_failed.append(target_dt)
            except Exception as exc:
                print(f"   ⚠️  Could not start ribbon final retry round: {exc}")
                still_failed = retry_targets
            finally:
                await self._close_context_safely(context, "final retry context")

            failed_jobs = still_failed

        missing = []
        if OUTPUT_STORE:
            for target_dt in dates:
                date_str = output_date(target_dt)
                if not OUTPUT_STORE.job_has_any_row(origin_code, dest_code, date_str):
                    missing.append(target_dt)
                    guard = JobResult(
                        STATUS_FAILED,
                        [],
                        "Completeness guard wrote this row because no ribbon output existed for the expected route/date",
                        True,
                        max(0, self.max_attempts - 1),
                    )
                    self._write_result(origin_code, dest_code, date_str, guard)

        if missing:
            print(f"   🚨 Completeness guard added {len(missing)} missing route/date row(s).")

    async def _run_fresh_route_production(self, browser, origin_code: str, dest_code: str,
                                          dates: list[datetime]):
        failed_jobs: list[datetime] = []

        for idx, target_dt in enumerate(dates, 1):
            date_str = output_date(target_dt)
            print(f"\n{'═'*60}")
            print(f"📅 [{idx}/{len(dates)}]  {target_dt.strftime('%A, %d %b %Y')}")
            print(f"{'─'*60}")

            if self.resume and OUTPUT_STORE and OUTPUT_STORE.job_completed(origin_code, dest_code, date_str):
                print(f"   ↩️  Resume: already completed for run {RUN_ID}; skipping.")
                continue

            result = await self.scrape_job_with_retries(browser, origin_code, dest_code, target_dt)
            self._write_result(origin_code, dest_code, date_str, result)

            if result.completed:
                if result.status == STATUS_SUCCESS:
                    print(f"   ✅ {len(result.rows)} flight row(s) saved.")
                else:
                    print(f"   ℹ️  No fare: {result.comment}")
            else:
                cond = result.status
                reason = result.comment or "Unknown"
                print(f"   ❌ {cond} — {reason}")
                failed_jobs.append(target_dt)

        for retry_round in range(1, self.final_retry_rounds + 1):
            retry_targets = [
                dt for dt in failed_jobs
                if not (OUTPUT_STORE and OUTPUT_STORE.job_completed(origin_code, dest_code, output_date(dt)))
            ]
            if not retry_targets:
                break

            print(f"\n{'═'*60}")
            print(f"🔁 Final retry round {retry_round}/{self.final_retry_rounds}: {len(retry_targets)} job(s)")
            print(f"{'═'*60}")
            still_failed: list[datetime] = []

            for target_dt in retry_targets:
                date_str = output_date(target_dt)
                result = await self.scrape_job_with_retries(browser, origin_code, dest_code, target_dt)
                self._write_result(origin_code, dest_code, date_str, result)
                if result.completed:
                    print(f"   ✅ Retry OK: {origin_code}→{dest_code} {date_str}")
                else:
                    cond = result.status
                    reason = result.comment or "Unknown"
                    print(f"   ❌ Retry {cond} — {reason}")
                    still_failed.append(target_dt)

            failed_jobs = still_failed

        missing = []
        if OUTPUT_STORE:
            for target_dt in dates:
                date_str = output_date(target_dt)
                if not OUTPUT_STORE.job_has_any_row(origin_code, dest_code, date_str):
                    missing.append(target_dt)
                    guard = JobResult(
                        STATUS_FAILED,
                        [],
                        "Internal completeness guard wrote this row because no output existed for the expected route/date",
                        retryable=True,
                        retry_count=max(0, self.max_attempts - 1),
                    )
                    self._write_result(origin_code, dest_code, date_str, guard)

        if missing:
            print(f"   🚨 Completeness guard added {len(missing)} missing route/date row(s).")

    async def _run_cleanup_retry_pass(
        self,
        browser,
        origin_code: str,
        dest_code: str,
        dates: list[datetime],
    ):
        """
        After the main run (ribbon or fresh-search) finishes, scan OUTPUT_STORE for any
        dates that still have retryable failure rows (BLOCKED / TIMEOUT / FAILED etc.)
        and retry them with a freshly connected Bright Data session.

        This is the key safety net: even if reCAPTCHA quota was exhausted for the
        original browser session, reconnecting gives a new IP and a second chance.
        """
        if not OUTPUT_STORE:
            return

        date_strs_all = [output_date(dt) for dt in dates]
        failed_strs = OUTPUT_STORE.failed_route_dates(origin_code, dest_code, date_strs_all)
        if not failed_strs:
            print(f"\n   ✅ Cleanup pass: all {len(dates)} dates completed — no retries needed.")
            return

        failed_set = set(failed_strs)
        failed_dates = [dt for dt in dates if output_date(dt) in failed_set]

        print(f"\n{'─'*60}")
        print(
            f"🔄 Cleanup retry pass: {len(failed_dates)} date(s) still failed for "
            f"{origin_code}→{dest_code}"
        )
        print(f"   Dates: {', '.join(failed_strs[:10])}{'...' if len(failed_strs) > 10 else ''}")
        print(f"{'─'*60}")

        # Reconnect to Bright Data — new connection = potential new IP = fresh quota.
        # Even a brief pause before reconnecting helps the pool rotate.
        print("   🔌 Reconnecting to Bright Data for fresh IP (cleanup pass)...")
        await asyncio.sleep(5)
        cleanup_browser = await self._reconnect_browser()
        effective_browser = cleanup_browser if cleanup_browser else browser

        succeeded = 0
        still_failed = 0
        for target_dt in failed_dates:
            date_str = output_date(target_dt)
            if OUTPUT_STORE.job_completed(origin_code, dest_code, date_str):
                print(f"   ↩️  {date_str}: completed since scan — skipping.")
                continue

            print(f"\n   🔁 Cleanup retry: {origin_code}→{dest_code} {date_str}")
            try:
                result = await self.scrape_job_with_retries(
                    effective_browser, origin_code, dest_code, target_dt
                )
            except Exception as exc:
                print(f"   ❌ Cleanup retry exception for {date_str}: {exc}")
                result = JobResult(
                    STATUS_FAILED,
                    [],
                    f"Cleanup retry exception: {exc}",
                    retryable=True,
                )

            self._write_result(origin_code, dest_code, date_str, result)
            if result.completed:
                print(f"   ✅ Cleanup retry succeeded: {date_str} ({result.status})")
                succeeded += 1
            else:
                print(f"   ❌ Cleanup retry still failed: {date_str} — {result.status}: {result.comment}")
                still_failed += 1
                # If blocked again, reconnect for the next date to try a different IP
                if result.status == STATUS_BLOCKED:
                    new_browser = await self._reconnect_browser()
                    if new_browser:
                        effective_browser = new_browser

        print(f"\n   📋 Cleanup pass done: {succeeded} recovered, {still_failed} still failed.")

    async def run_route(self, origin_code, dest_code):
        origin_name = AIRPORT_MAP.get(origin_code, origin_code)
        dest_name   = AIRPORT_MAP.get(dest_code, dest_code)
        dates       = build_date_list()
        route_key   = (origin_code, dest_code)
        route_navigation_mode = "fresh" if route_key in NO_RIBBON_ROUTES else self.navigation_mode

        print(f"\n{'█'*60}")
        print(f"  ROUTE : {origin_code} ({origin_name}) → {dest_code} ({dest_name})")
        rtype = "Connecting" if route_key in CONNECTING_ROUTES else "Independent"
        print(f"  Type  : {rtype}")
        print(f"  Scrape window: {dates[0].strftime('%d-%m-%Y')} → {dates[-1].strftime('%d-%m-%Y')}")
        print(f"  Output: {OUTPUT_EXCEL}")
        print(f"  Mode  : {route_navigation_mode}")
        print(f"  Retry : attempts={self.max_attempts}, final_rounds={self.final_retry_rounds}, job_timeout={self.job_timeout}s")
        print(
            f"  Captcha: strategy={self.captcha_recovery_strategy}, "
            f"disabled_threshold={self.captcha_disabled_threshold}, "
            f"same_page_recoveries={self.captcha_recovery_max}"
        )
        print(f"{'█'*60}")

        async with async_playwright() as p:
            self._pw_instance = p   # stored so _new_job_context can reconnect on session drop
            print("🔌 Connecting to Bright Data Browser API...")
            browser = None
            try:
                browser = await p.chromium.connect_over_cdp(BD_BROWSER_WSS)
                print("✅ Browser API connected.")
            except Exception as exc:
                print(f"❌ Could not connect to Bright Data Browser API: {exc}")
                for dt in dates:
                    result = JobResult(
                        STATUS_SITE_UNAVAILABLE,
                        [],
                        f"Could not connect to Bright Data Browser API: {exc}",
                        retryable=True,
                    )
                    self._write_result(origin_code, dest_code, output_date(dt), result)
                return

            try:
                if route_navigation_mode == "ribbon":
                    await self._run_ribbon_route_production(browser, origin_code, dest_code, dates)
                else:
                    await self._run_fresh_route_production(browser, origin_code, dest_code, dates)

            except KeyboardInterrupt:
                print("\n⛔ Interrupted.")
            except Exception as e:
                print(f"\n❌ Fatal error: {e}")
                traceback.print_exc()
                if OUTPUT_STORE:
                    for dt in dates:
                        date_str = output_date(dt)
                        if not OUTPUT_STORE.job_has_any_row(origin_code, dest_code, date_str):
                            self._write_result(
                                origin_code,
                                dest_code,
                                date_str,
                                JobResult(
                                    STATUS_ROUTE_SETUP_FAILED,
                                    [],
                                    f"Route-level failure before this job completed: {e}",
                                    retryable=True,
                                ),
                            )
            else:
                # Main run completed without exception — run cleanup pass to recover
                # any dates still showing retryable failures (e.g. captcha-blocked).
                try:
                    await self._run_cleanup_retry_pass(browser, origin_code, dest_code, dates)
                except KeyboardInterrupt:
                    print("\n⛔ Cleanup pass interrupted.")
                except Exception as exc:
                    print(f"\n⚠️  Cleanup pass encountered an error (non-fatal): {exc}")
            finally:
                if browser:
                    await browser.close()

        print(f"\n  📊 Output saved: {OUTPUT_EXCEL}\n")


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────

def print_usage():
    print("""
Usage:
  python rex_brightdata.py --days 84 PER ALH EPR PER
  python rex_brightdata.py --days 84 --routes PER-ALH,EPR-PER
  python rex_brightdata.py PER ALH
  python rex_brightdata.py --list
""")
    for i, (o, d) in enumerate(ALL_ROUTES, 1):
        rtype = "🔗 connecting" if (o, d) in CONNECTING_ROUTES else "✈️  independent"
        print(f"  {i:2d}. {o} → {d}  "
              f"({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})  [{rtype}]")
    print()


def parse_routes(ns) -> list[tuple[str, str]]:
    if ns.routes:
        route_tokens = [r.strip() for r in ns.routes.split(",") if r.strip()]
        pairs = []
        for token in route_tokens:
            parts = re.split(r"[-:>]", token.upper())
            parts = [p for p in parts if p]
            if len(parts) != 2:
                raise ValueError(f"Bad route format: {token}")
            pairs.append((parts[0], parts[1]))
        return pairs

    if ns.route_codes:
        if len(ns.route_codes) % 2 != 0:
            raise ValueError("Route codes must be supplied in ORIGIN DEST pairs.")
        codes = [c.upper() for c in ns.route_codes]
        return list(zip(codes[0::2], codes[1::2]))

    return ALL_ROUTES


def parse_args():
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("route_codes", nargs="*", help="Route pairs, e.g. PER ALH EPR PER")
    parser.add_argument("--routes", help="Comma list, e.g. PER-ALH,EPR-PER")
    parser.add_argument("--days", type=int, default=TOTAL_DAYS, help="Number of dates to scrape")
    parser.add_argument(
        "--start-offset-days",
        type=int,
        default=START_OFFSET_DAYS,
        help="Days after today to start scraping; default 1 skips same-day searches",
    )
    parser.add_argument("--output", default=OUTPUT_EXCEL, help="Excel output file")
    parser.add_argument("--debug-dir", default=DEBUG_DIR, help="Directory for failed-job screenshots/HTML/metadata")
    parser.add_argument("--log-dir", default=LOG_DIR, help="Directory for cron-friendly run logs")
    parser.add_argument("--run-id", default=RUN_ID, help="Resume key written to output; default is Rex-local YYYYMMDD")
    parser.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS, help="Attempts per route/date before final retry queue")
    parser.add_argument("--final-retry-rounds", type=int, default=FINAL_RETRY_ROUNDS, help="Extra end-of-route retry rounds for failed jobs")
    parser.add_argument("--retry-backoff", type=float, default=RETRY_BACKOFF_SECONDS, help="Base retry backoff seconds")
    parser.add_argument("--max-retry-backoff", type=float, default=MAX_RETRY_BACKOFF_SECONDS, help="Maximum retry backoff seconds")
    parser.add_argument("--job-timeout", type=int, default=JOB_TIMEOUT_SECONDS, help="Hard timeout seconds per route/date attempt")
    parser.add_argument(
        "--captcha-disabled-threshold",
        type=int,
        default=CAPTCHA_DISABLED_LOOP_THRESHOLD,
        help="Bright Data solver cycles to allow while Rex verification Continue remains disabled",
    )
    parser.add_argument(
        "--captcha-recovery-max",
        type=int,
        default=CAPTCHA_RECOVERY_MAX,
        help="Same-page captcha recoveries before relying on normal new-context retries",
    )
    parser.add_argument(
        "--captcha-recovery-strategy",
        choices=sorted(CAPTCHA_RECOVERY_STRATEGIES),
        default=CAPTCHA_RECOVERY_STRATEGY if CAPTCHA_RECOVERY_STRATEGY in CAPTCHA_RECOVERY_STRATEGIES else "refill-then-new-context",
        help="Recovery action when Rex reCAPTCHA stays disabled",
    )
    parser.add_argument(
        "--navigation-mode",
        choices=["fresh", "ribbon"],
        default=NAVIGATION_MODE if NAVIGATION_MODE in {"fresh", "ribbon"} else "ribbon",
        help="ribbon is default: select route once, then move dates via Rex ribbon; fresh reselects route/date per job",
    )
    parser.add_argument("--no-resume", action="store_true", help="Do not skip route/date jobs already completed for this run id")
    parser.add_argument(
        "--inter-route-delay",
        type=int,
        default=INTER_ROUTE_DELAY_SECONDS,
        metavar="SECS",
        help="Seconds to pause between routes so Bright Data can rotate to a fresh IP (default: 30)",
    )
    parser.add_argument("--list", action="store_true", help="Show supported routes")
    parser.add_argument("--skip-unblocker-check", action="store_true")
    return parser.parse_args()


def interactive_route_selection() -> list[tuple[str, str]]:
    """
    Interactive route selector — runs when no route args given on command line.
    User specific routes ya ALL choose kar sakta hai.
    """
    print("\n" + "═" * 60)
    print("  REX AIRLINES SCRAPER — ROUTE SELECTION")
    print("═" * 60)
    print()
    print("  Available routes:")
    print()
    for i, (o, d) in enumerate(ALL_ROUTES, 1):
        rtype = "🔗 connecting" if (o, d) in CONNECTING_ROUTES else "✈️  direct   "
        print(f"    {i:2d}.  {o} → {d}  ({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})  [{rtype}]")
    print()
    print("    0.  ✅ ALL ROUTES")
    print()

    while True:
        raw = input("  Enter route number(s) separated by comma (e.g. 1,3,5) or 0 for all: ").strip()
        if not raw:
            continue
        tokens = [t.strip() for t in raw.split(",") if t.strip()]
        try:
            selections = [int(t) for t in tokens]
        except ValueError:
            print("  ❌ Invalid input — sirf numbers enter karo.\n")
            continue

        if selections == [0]:
            print(f"\n  ✅ All {len(ALL_ROUTES)} routes selected.\n")
            return list(ALL_ROUTES)

        bad = [s for s in selections if s < 1 or s > len(ALL_ROUTES)]
        if bad:
            print(f"  ❌ Invalid number(s): {bad}. 1–{len(ALL_ROUTES)} range mein hona chahiye.\n")
            continue

        chosen = [ALL_ROUTES[s - 1] for s in selections]
        print("\n  Selected routes:")
        for o, d in chosen:
            print(f"    ✈️  {o} → {d}  ({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})")
        print()
        confirm = input("  Confirm? (y/n): ").strip().lower()
        if confirm in {"y", "yes", ""}:
            return chosen
        print("  Re-select karo.\n")


if __name__ == "__main__":
    ns = parse_args()

    if ns.list:
        print_usage()
        sys.exit(0)

    # ── ROUTE SELECTION ──────────────────────────────────────────
    # Agar command line pe koi route nahi diya, interactive menu show karo
    cli_has_routes = bool(ns.route_codes or ns.routes)
    if cli_has_routes:
        try:
            routes_to_run = parse_routes(ns)
        except ValueError as exc:
            print(f"❌ {exc}")
            print_usage()
            sys.exit(1)
        bad_routes = [route for route in routes_to_run if route not in ALL_ROUTES]
        if bad_routes:
            print(f"❌ Route(s) list mein nahi hai: {bad_routes}")
            print_usage()
            sys.exit(1)
    else:
        if sys.stdin.isatty():
            routes_to_run = interactive_route_selection()
        else:
            print("   ℹ️  No routes specified — running ALL routes (non-interactive/cron mode)")
            routes_to_run = list(ALL_ROUTES)

    # ── CONFIG APPLY ─────────────────────────────────────────────
    TOTAL_DAYS = max(1, ns.days)
    START_OFFSET_DAYS = max(0, ns.start_offset_days)
    OUTPUT_EXCEL = ns.output
    DEBUG_DIR = ns.debug_dir
    LOG_DIR = ns.log_dir
    RUN_ID = ns.run_id
    MAX_ATTEMPTS = max(1, ns.max_attempts)
    FINAL_RETRY_ROUNDS = max(0, ns.final_retry_rounds)
    RETRY_BACKOFF_SECONDS = max(0.0, ns.retry_backoff)
    MAX_RETRY_BACKOFF_SECONDS = max(1.0, ns.max_retry_backoff)
    JOB_TIMEOUT_SECONDS = max(30, ns.job_timeout)
    CAPTCHA_DISABLED_LOOP_THRESHOLD = max(1, ns.captcha_disabled_threshold)
    CAPTCHA_RECOVERY_MAX = max(0, ns.captcha_recovery_max)
    CAPTCHA_RECOVERY_STRATEGY = ns.captcha_recovery_strategy
    NAVIGATION_MODE = ns.navigation_mode
    RESUME_ENABLED = not ns.no_resume
    INTER_ROUTE_DELAY_SECONDS = max(0, ns.inter_route_delay)

    log_fh = configure_run_logging(LOG_DIR, RUN_ID)
    OUTPUT_STORE = OutputStore(OUTPUT_EXCEL, RUN_ID)

    if not ns.skip_unblocker_check:
        check_web_unlocker()

    scraper = RexScraper(
        headless=False,
        debug_dir=DEBUG_DIR,
        max_attempts=MAX_ATTEMPTS,
        final_retry_rounds=FINAL_RETRY_ROUNDS,
        retry_backoff=RETRY_BACKOFF_SECONDS,
        max_retry_backoff=MAX_RETRY_BACKOFF_SECONDS,
        job_timeout=JOB_TIMEOUT_SECONDS,
        navigation_mode=NAVIGATION_MODE,
        resume=RESUME_ENABLED,
        captcha_disabled_threshold=CAPTCHA_DISABLED_LOOP_THRESHOLD,
        captcha_recovery_max=CAPTCHA_RECOVERY_MAX,
        captcha_recovery_strategy=CAPTCHA_RECOVERY_STRATEGY,
    )
    start_dt = build_date_list()[0]

    # ── CLEAN RUN SUMMARY ────────────────────────────────────────
    print("═" * 60)
    print("  REX SCRAPER — RUN SUMMARY")
    print("═" * 60)
    print(f"  📋 Routes      : {len(routes_to_run)}")
    for o, d in routes_to_run:
        print(f"       {o} → {d}  ({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})")
    print(f"  📆 Dates/route : {TOTAL_DAYS}  (from {start_dt.strftime('%d-%m-%Y')})")
    print(f"  🗂  Output      : {OUTPUT_EXCEL}")
    print(f"  🔁 Resume      : {'Yes' if RESUME_ENABLED else 'No'}  |  Run ID: {RUN_ID}")
    print(f"  💾 Checkpoint  : every {CHECKPOINT_EVERY} entries")
    print(f"  🌐 Bright Data : Browser API + Web Unblocker")
    print("═" * 60 + "\n")

    # ── ROUTE EXECUTION ──────────────────────────────────────────
    route_status: dict[tuple[str, str], str] = {}

    try:
        for route_idx, (origin, dest) in enumerate(routes_to_run):
            try:
                asyncio.run(scraper.run_route(origin, dest))
                route_status[(origin, dest)] = "ok"
            except KeyboardInterrupt:
                print(f"\n⛔ Stopped at {origin}→{dest}.")
                route_status[(origin, dest)] = "interrupted"
                break
            except Exception as exc:
                route_status[(origin, dest)] = f"ERROR: {type(exc).__name__}: {exc}"
                print(f"\n❌ Route {origin}→{dest} failed: {type(exc).__name__}: {exc}")

            # Inter-route delay: pause so Bright Data can rotate to a fresh IP before
            # the next route opens a new browser session.  Helps avoid reCAPTCHA quota
            # exhaustion carrying over from one route to the next.
            if route_idx < len(routes_to_run) - 1 and INTER_ROUTE_DELAY_SECONDS > 0:
                print(
                    f"\n⏸️  Inter-route cooldown: {INTER_ROUTE_DELAY_SECONDS}s "
                    "(letting Bright Data IP pool rotate before next route)..."
                )
                time.sleep(INTER_ROUTE_DELAY_SECONDS)
    finally:
        # ── FINAL STATUS TABLE ───────────────────────────────────
        print("\n" + "═" * 60)
        print("  ROUTE RUN RESULTS")
        print("═" * 60)
        for (o, d) in routes_to_run:
            st = route_status.get((o, d), "not reached")
            if st == "ok":
                print(f"  ✅  {o} → {d}")
            elif st == "interrupted":
                print(f"  ⛔  {o} → {d}  (interrupted)")
            else:
                reason = st.replace("ERROR: ", "")
                condition = type(Exception()).__name__ if ":" not in reason else reason.split(":")[0].strip()
                detail    = reason.split(":", 1)[-1].strip() if ":" in reason else reason
                print(f"  ❌  {o} → {d}")
                print(f"       Condition : {condition}")
                print(f"       Reason    : {detail}")
        print("═" * 60)
        restore_run_logging(log_fh)
