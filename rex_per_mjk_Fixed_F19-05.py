"""
Rex Airlines — PER ↔ MJK Scraper (Selenium + Bright Data)
==========================================================
Only 2 routes: PER → MJK  and  MJK → PER

Flow for each date:
  1. Rex homepage open (fresh, new Selenium driver instance)
  2. Select one-way trip
  3. Fill origin / destination
  4. Set departure date
  5. Submit form + wait for CAPTCHA
  6. Extract flight data
  7. Save to Excel
  8. Quit driver → move to next date

Output format matches the original code — same Excel columns.
"""

import os
import re
import sys
import json
import time
import random
import tempfile
import traceback
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)
from openpyxl import load_workbook, Workbook

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ─────────────────────────────────────────────────────────────
#  BRIGHT DATA CREDENTIALS
# ─────────────────────────────────────────────────────────────
BD_BROWSER_HOST          = os.getenv("BD_BROWSER_HOST", "brd.superproxy.io")
BD_BROWSER_SELENIUM_PORT = os.getenv("BD_BROWSER_SELENIUM_PORT", "9515")
BD_AUTH_TOKEN            = os.getenv("BD_AUTH_TOKEN", "7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7")
BD_WEB_UNLOCKER_ZONE     = os.getenv("BD_WEB_UNLOCKER_ZONE", "cron_rex")
BD_UNLOCKER_COUNTRY      = os.getenv("BD_UNLOCKER_COUNTRY", "au")

# PER → MJK uses zone rex_root_8
BD_PER_MJK_USER = os.getenv("BD_PER_MJK_USER", "brd-customer-hl_fbc4a16a-zone-rex_root_8")
BD_PER_MJK_PASS = os.getenv("BD_PER_MJK_PASS", "46yox0svep00")

# MJK → PER uses zone rex_root_7
BD_MJK_PER_USER = os.getenv("BD_MJK_PER_USER", "brd-customer-hl_fbc4a16a-zone-rex_root_7")
BD_MJK_PER_PASS = os.getenv("BD_MJK_PER_PASS", "iqeo716xnvw1")

BD_PER_MJK_SELENIUM_URL = os.getenv(
    "BD_PER_MJK_SELENIUM_URL",
    f"https://{BD_PER_MJK_USER}:{BD_PER_MJK_PASS}@{BD_BROWSER_HOST}:{BD_BROWSER_SELENIUM_PORT}",
)
BD_MJK_PER_SELENIUM_URL = os.getenv(
    "BD_MJK_PER_SELENIUM_URL",
    f"https://{BD_MJK_PER_USER}:{BD_MJK_PER_PASS}@{BD_BROWSER_HOST}:{BD_BROWSER_SELENIUM_PORT}",
)

# Route → Selenium URL mapping
ROUTE_SELENIUM_URL: dict[tuple[str, str], str] = {
    ("PER", "MJK"): BD_PER_MJK_SELENIUM_URL,
    ("MJK", "PER"): BD_MJK_PER_SELENIUM_URL,
}

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
ROUTES = [("PER", "MJK"), ("MJK", "PER")]

AIRPORT_MAP = {
    "PER": "Perth",
    "MJK": "Monkey Mia (Shark Bay)",
}

REX_TIMEZONE  = os.getenv("REX_TIMEZONE", "Australia/Perth")
REX_TZ        = ZoneInfo(REX_TIMEZONE)
TOTAL_DAYS    = int(os.getenv("REX_TOTAL_DAYS", "84"))
START_OFFSET  = int(os.getenv("REX_START_OFFSET_DAYS", "0"))  # 0 = start from today
# Each run gets its own timestamped file by default — no data mixing between runs.
# Override with REX_OUTPUT_EXCEL env var if you want a fixed filename.
_RUN_TS      = datetime.now(ZoneInfo(os.getenv("REX_TIMEZONE", "Australia/Perth"))).strftime("%Y%m%d_%H%M%S")
OUTPUT_EXCEL  = os.getenv("REX_OUTPUT_EXCEL", f"rex_per_mjk_results_{_RUN_TS}.xlsx")
DEBUG_DIR     = os.getenv("REX_DEBUG_DIR", "rex_debug")
LOG_DIR       = os.getenv("REX_LOG_DIR", "rex_logs")
RUN_ID        = os.getenv("REX_RUN_ID", datetime.now(REX_TZ).strftime("%Y%m%d"))
MAX_ATTEMPTS  = int(os.getenv("REX_MAX_ATTEMPTS", "3"))
PAGE_TIMEOUT  = int(os.getenv("REX_PAGE_TIMEOUT", "90"))        # seconds
CAPTCHA_WAIT  = int(os.getenv("REX_CAPTCHA_WAIT", "60"))        # seconds

STATUS_SUCCESS  = "SUCCESS"
STATUS_NO_FARE  = "NO_FARE_AVAILABLE"
STATUS_FAILED   = "FAILED_AFTER_RETRIES"
STATUS_TIMEOUT  = "PAGE_TIMEOUT"
STATUS_BLOCKED  = "BLOCKED_OR_VERIFICATION_REQUIRED"
STATUS_STRUCT   = "POSSIBLE_WEBSITE_STRUCTURE_ISSUE"

CSV_FIELDS = [
    "Date Checked", "Time Checked", "Airline",
    "Date of Departure", "Time of Departure",
    "Origin", "Destination",
    "Fare Price", "Fare Class", "Source",
    "Run ID", "Status", "Comment", "Retry Count", "Debug Artifacts",
]

COMPLETED_STATUSES = {STATUS_SUCCESS, STATUS_NO_FARE}
RESUME_MODE        = False   # set to True via --resume CLI flag


# ─────────────────────────────────────────────────────────────
#  UTILITIES
# ─────────────────────────────────────────────────────────────

def rex_now() -> datetime:
    return datetime.now(REX_TZ).replace(tzinfo=None)


def today_dt() -> datetime:
    return rex_now().replace(hour=0, minute=0, second=0, microsecond=0)


def build_date_list() -> list[datetime]:
    t = today_dt() + timedelta(days=START_OFFSET)
    return [t + timedelta(days=i) for i in range(TOTAL_DAYS)]


def output_date(dt: datetime) -> str:
    return dt.strftime("%d-%m-%Y")


def rex_form_date(dt: datetime) -> str:
    """Rex form expects '20 May 2026' format."""
    return dt.strftime("%d %b %Y")


def _format_price(dollars: str, cents: str | None = None) -> str:
    dollars = re.sub(r"\s+", "", dollars).replace(",", "").strip()
    if cents is None:
        return f"${dollars}.00" if "." not in dollars else f"${dollars}"
    return f"${dollars}.{cents.strip()}"


def _extract_price_patterns(text: str) -> str:
    """
    Rex renders prices in fragmented ways — normalise and try all patterns.
    Handles: "$686.11", "$686 11", "From\n$\n686\n.\n11", "686 . 11 Select Fares"
    """
    flat = re.sub(r"[\s\n\r]+", " ", text)
    patterns = [
        r"[Ff]rom\s*\$\s*([\d,]+)\.(\d{2})",
        r"\$\s*([\d,]+)\.(\d{2})",
        r"[Ff]rom\s*\$\s*([\d,]+)\s+(\d{2})(?!\d)",
        r"\$\s*([\d,]+)\s+(\d{2})(?!\d)",
        r"[Ff]rom\s*\$\s*(\d[\d,\s]*\d)\s*[\.\s]\s*(\d{2})(?!\d)",
        r"([\d,]+)\s*\.\s*(\d{2})\s+[Ss]elect\s+[Ff]ares",
    ]
    for pattern in patterns:
        m = re.search(pattern, flat)
        if m:
            dollars_raw = re.sub(r"\s+", "", m.group(1))
            return _format_price(dollars_raw, m.group(2))

    # Search for number just before "Select Fares" button text
    sf_pos = flat.lower().find("select fares")
    if sf_pos > 0:
        before = flat[:sf_pos]
        m = re.search(r"(\d[\d,]*)\s*[.\s]\s*(\d{2})\s*$", before.strip())
        if m:
            return _format_price(re.sub(r"\s+", "", m.group(1)), m.group(2))
        m = re.search(r"\$\s*(\d[\d,]*)", before)
        if m:
            return _format_price(m.group(1))

    m = re.search(r"[Ff]rom\s*\$\s*(\d[\d,]*)", flat)
    if m:
        return _format_price(m.group(1))
    m = re.search(r"\$\s*(\d[\d,]*)", flat)
    if m:
        return _format_price(m.group(1))
    return "N/A"


def extract_all_times(text: str) -> list[str]:
    times = re.findall(r"\d{1,2}:\d{2}\s*[aApP][mM]", text)
    if times:
        return [t.strip() for t in times]
    return [t.strip() for t in re.findall(r"\b\d{1,2}:\d{2}\b", text)]


def extract_flights_from_body(body_text: str, origin: str, dest: str, date_str: str) -> list[dict]:
    """
    Extract ZL flight numbers and prices from page body text.
    PER→MJK is a direct non-stop flight — standard price extraction applies.
    """
    now = rex_now()
    ck_date = now.strftime("%d-%m-%Y")
    ck_time = now.strftime("%H:%M:%S")

    # Strip ribbon area — start from after "Select your departing flight"
    markers = [
        "Select your departing flight", "Departure Time",
        "departing flight", "Fly Economy", "Select Fares",
    ]
    ribbon_end = len(body_text) // 7  # fallback
    for marker in markers:
        pos = body_text.find(marker)
        if pos > 0:
            ribbon_end = pos
            break
    zl_match = re.search(r"ZL\s?\d{3,4}", body_text)
    if zl_match and zl_match.start() > 100:
        ribbon_end = min(ribbon_end, max(0, zl_match.start() - 50))

    print(f"   📍 Ribbon area ends at ~char {ribbon_end}")
    body_after = body_text[ribbon_end:]
    all_times  = extract_all_times(body_after)
    zl_matches = list(re.finditer(r"ZL\s?\d{3,4}", body_after))

    print(f"   🔍 ZL matches after ribbon: {[m.group() for m in zl_matches]}")

    flights = []
    seen    = set()

    for idx, m in enumerate(zl_matches):
        f_no = re.sub(r"\s", "", m.group())

        # Departure time: Nth flight → times[N*2] (0-indexed even positions)
        dep_idx = idx * 2
        if dep_idx < len(all_times):
            dep = all_times[dep_idx]
        elif idx < len(all_times):
            dep = all_times[idx]
        else:
            dep = "-"

        # Price: look in window after ZL match
        after_start = m.end()
        after_end   = min(len(body_after), after_start + 900)
        window      = body_after[after_start:after_end]
        price       = _extract_price_patterns(window)

        key = f"{f_no}-{dep}"
        if key in seen:
            continue
        seen.add(key)

        print(f"      ✈️  {f_no}  dep={dep}  price={price}")
        flights.append({
            "Date Checked":      ck_date,
            "Time Checked":      ck_time,
            "Airline":           f_no,
            "Date of Departure": date_str,
            "Time of Departure": dep,
            "Origin":            origin,
            "Destination":       dest,
            "Fare Price":        price,
            "Fare Class":        "Economy",
            "Source":            "Rex Website",
            "Run ID":            RUN_ID,
            "Status":            STATUS_SUCCESS,
            "Comment":           "Flight and fare data parsed from Rex availability page",
            "Retry Count":       0,
            "Debug Artifacts":   "",
        })

    return flights


def make_no_fare_row(date_str: str, origin: str, dest: str, comment: str, retry_count: int = 0) -> dict:
    now = rex_now()
    return {
        "Date Checked":      now.strftime("%d-%m-%Y"),
        "Time Checked":      now.strftime("%H:%M:%S"),
        "Airline":           "no flight",
        "Date of Departure": date_str,
        "Time of Departure": "-",
        "Origin":            origin,
        "Destination":       dest,
        "Fare Price":        "N/A",
        "Fare Class":        "",
        "Source":            "Rex Website",
        "Run ID":            RUN_ID,
        "Status":            STATUS_NO_FARE,
        "Comment":           comment,
        "Retry Count":       retry_count,
        "Debug Artifacts":   "",
    }


def make_failed_row(date_str: str, origin: str, dest: str, status: str,
                    comment: str, retry_count: int, debug: str = "") -> dict:
    now = rex_now()
    return {
        "Date Checked":      now.strftime("%d-%m-%Y"),
        "Time Checked":      now.strftime("%H:%M:%S"),
        "Airline":           "scrape failed",
        "Date of Departure": date_str,
        "Time of Departure": "-",
        "Origin":            origin,
        "Destination":       dest,
        "Fare Price":        "N/A",
        "Fare Class":        "",
        "Source":            "Rex Website - failed",
        "Run ID":            RUN_ID,
        "Status":            status,
        "Comment":           comment,
        "Retry Count":       retry_count,
        "Debug Artifacts":   debug,
    }


# ─────────────────────────────────────────────────────────────
#  EXCEL OUTPUT
# ─────────────────────────────────────────────────────────────

class OutputStore:
    def __init__(self, path: str):
        self.path = path

    def _load(self):
        if os.path.exists(self.path):
            wb = load_workbook(self.path)
            ws = wb.active
            first_has_values = any(
                ws.cell(row=1, column=c).value
                for c in range(1, len(CSV_FIELDS) + 1)
            )
            if not first_has_values:
                ws.append(CSV_FIELDS)
        else:
            wb = Workbook()
            ws = wb.active
            ws.title = "Rex PER MJK Data"
            ws.append(CSV_FIELDS)
        return wb, ws

    def _headers(self, ws) -> list[str]:
        return [cell.value or "" for cell in ws[1]]

    def _save(self, wb):
        out_dir = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(out_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=f".{Path(self.path).stem}_", suffix=".xlsx", dir=out_dir
        )
        os.close(fd)
        try:
            wb.save(tmp)
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    def _row_matches(self, ws, row_idx, origin, dest, date_str) -> bool:
        headers = self._headers(ws)
        vals = {h: ws.cell(row=row_idx, column=i + 1).value for i, h in enumerate(headers)}
        return (
            str(vals.get("Run ID") or "") == RUN_ID
            and str(vals.get("Origin") or "") == origin
            and str(vals.get("Destination") or "") == dest
            and str(vals.get("Date of Departure") or "") == date_str
        )

    def write_rows(self, origin: str, dest: str, date_str: str, rows: list[dict]):
        wb, ws = self._load()
        headers = self._headers(ws)
        for row_idx in range(ws.max_row, 1, -1):
            if self._row_matches(ws, row_idx, origin, dest, date_str):
                ws.delete_rows(row_idx, 1)
        for row in rows:
            ws.append([row.get(f, "") for f in headers])
        self._save(wb)
        print(f"   💾 Saved {len(rows)} row(s) → {self.path}")

    def job_completed(self, origin: str, dest: str, date_str: str) -> bool:
        if not os.path.exists(self.path):
            return False
        wb, ws = self._load()
        headers = self._headers(ws)
        for row_idx in range(2, ws.max_row + 1):
            if self._row_matches(ws, row_idx, origin, dest, date_str):
                vals = {h: ws.cell(row=row_idx, column=i + 1).value for i, h in enumerate(headers)}
                if str(vals.get("Status") or "") in COMPLETED_STATUSES:
                    return True
        return False


# ─────────────────────────────────────────────────────────────
#  DEBUG ARTIFACTS
# ─────────────────────────────────────────────────────────────

def save_debug(driver, label: str, meta: dict | None = None) -> str:
    """Save screenshot + page source + JSON metadata for failed jobs."""
    os.makedirs(DEBUG_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_") or "page"
    ts = rex_now().strftime("%Y%m%d_%H%M%S")
    artifacts = []

    png_path = os.path.join(DEBUG_DIR, f"{safe}_{ts}.png")
    try:
        driver.save_screenshot(png_path)
        artifacts.append(os.path.abspath(png_path))
        print(f"   📸 Screenshot: {png_path}")
    except Exception as e:
        print(f"   ⚠️  Screenshot failed: {e}")

    html_path = os.path.join(DEBUG_DIR, f"{safe}_{ts}.html")
    try:
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(driver.page_source)
        artifacts.append(os.path.abspath(html_path))
        print(f"   🧾 HTML: {html_path}")
    except Exception as e:
        print(f"   ⚠️  HTML dump failed: {e}")

    json_path = os.path.join(DEBUG_DIR, f"{safe}_{ts}.json")
    try:
        m = dict(meta or {})
        m.setdefault("url", driver.current_url)
        m.setdefault("saved_at", rex_now().isoformat(sep=" "))
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
            m["body_snippet"] = body[:1500]
        except Exception:
            pass
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(m, fh, ensure_ascii=False, indent=2)
        artifacts.append(os.path.abspath(json_path))
        print(f"   🧭 Metadata: {json_path}")
    except Exception as e:
        print(f"   ⚠️  Metadata dump failed: {e}")

    return " | ".join(artifacts)


# ─────────────────────────────────────────────────────────────
#  SELENIUM DRIVER FACTORY
# ─────────────────────────────────────────────────────────────

def make_driver(selenium_url: str, max_attempts: int = 4) -> webdriver.Remote:
    """
    Connect to Bright Data Selenium endpoint.
    Retries automatically on transient Internal Server Errors — these are
    Bright Data-side and usually resolve within a few seconds.
    """
    opts = Options()
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--lang=en-AU")
    # Enable Bright Data's built-in CAPTCHA/reCAPTCHA auto-solver
    try:
        opts.add_experimental_option("brd:options", {"captcha": {"solver": "ccc"}})
    except Exception:
        pass  # Older selenium versions may not support this — safe to ignore

    last_exc = None
    for attempt in range(1, max_attempts + 1):
        driver = None
        try:
            driver = webdriver.Remote(
                command_executor=selenium_url,
                options=opts,
            )
            driver.set_page_load_timeout(PAGE_TIMEOUT)
            driver.implicitly_wait(0)   # using explicit waits throughout
            return driver
        except Exception as exc:
            last_exc = exc
            msg = str(exc).lower()
            is_transient = "internal server error" in msg or "500" in msg or "connection" in msg
            # Always quit the partially-created driver if it exists
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
            if is_transient and attempt < max_attempts:
                wait = 8 * attempt
                print(f"   ⚠️  Bright Data connect error (attempt {attempt}/{max_attempts}), retrying in {wait}s: {exc}")
                time.sleep(wait)
                continue
            raise   # non-transient or out of retries → let caller handle it


# ─────────────────────────────────────────────────────────────
#  PAGE HELPERS
# ─────────────────────────────────────────────────────────────

def page_body_text(driver) -> str:
    try:
        return driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        return ""


def page_has_no_fare(driver) -> bool:
    text = page_body_text(driver).lower()
    patterns = [
        r"\bno\s+(?:available\s+)?flights?\b",
        r"\bno\s+(?:available\s+)?fares?\b",
        r"\bunable\s+to\s+find\s+flights?\b",
        r"\bflights?.{0,80}\bnot\s+available\b",
        r"\bfares?.{0,80}\bnot\s+available\b",
        r"\bsold\s+out\b",
    ]
    return any(re.search(p, text) for p in patterns)


def page_has_flights(driver) -> bool:
    try:
        text = page_body_text(driver)
        return bool(re.search(r"ZL\s?\d{3,4}", text))
    except Exception:
        return False


def page_has_verification(driver) -> bool:
    try:
        text = page_body_text(driver).lower()
        html = driver.page_source.lower()
        markers = ["please verify your details", "grecaptcha", "txtcaptcha",
                   "access denied", "blocked"]
        return any(m in text or m in html for m in markers)
    except Exception:
        return False


def click_avail_cont(driver, wait_secs: int = 90) -> bool:
    """
    Click the .availCont Continue button on Rex verification page.

    Two-phase approach:
      Phase 1 (wait_secs): Poll every 1.5s for reCAPTCHA to be solved
                           (Rex JS removes 'disabled' after solve).
      Phase 2 (fallback):  If still disabled, force-remove 'disabled' via JS
                           and click anyway — handles cases where Bright Data
                           solves reCAPTCHA but the callback fires late.
    """
    deadline = time.time() + wait_secs
    while time.time() < deadline:
        try:
            result = driver.execute_script("""
                const btn = document.querySelector('.availCont');
                if (!btn) return 'not_found';
                if (btn.disabled || btn.hasAttribute('disabled')) return 'disabled';
                btn.click();
                return 'clicked';
            """)
            if result == 'clicked':
                print("   ✅ Verification Continue button clicked (reCAPTCHA solved)")
                return True
            elif result == 'not_found':
                return False
            # still disabled — keep polling
        except Exception:
            pass
        time.sleep(1.5)

    # Phase 2: force-remove disabled and click
    # This works when Bright Data solved the reCAPTCHA but Rex callback was slow
    print("   ⚠️  reCAPTCHA taking long — force-clicking Continue button...")
    try:
        result = driver.execute_script("""
            const btn = document.querySelector('.availCont');
            if (!btn) return 'not_found';
            btn.removeAttribute('disabled');
            btn.disabled = false;
            // Also fire the reCAPTCHA callback manually to set txtcaptcha
            try {
                document.getElementById('txtcaptcha').value = 'captchad';
            } catch(e) {}
            btn.click();
            return 'force_clicked';
        """)
        if result == 'force_clicked':
            print("   ✅ Continue force-clicked (disabled removed)")
            return True
    except Exception as e:
        print(f"   ❌ Force-click also failed: {e}")
    return False


def page_is_rex_home(driver) -> bool:
    selectors = [
        "label[for*='rbTripType_oneway']",
        "input[id*='rbTripType_oneway']",
        "#ContentPlaceHolder1_BookingHomepageV21_OriginAirport",
        "#datefilter",
    ]
    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return True
        except Exception:
            pass
    return False


def wait_for_brightdata_captcha(driver, wait_secs: int = 60):
    """
    Wait for Bright Data to solve CAPTCHA on Selenium.
    CDP session is not available in Selenium, so simple polling is used instead.

    Exits early only when the page has reached a meaningful state:
      - Rex homepage booking form is visible, OR
      - Flight results are visible, OR
      - A no-fare message is visible.
    We no longer exit just because the word "captcha" disappeared from HTML,
    because that string can persist as a CSS class / element id even after
    Bright Data clears the overlay.
    """
    print(f"   ⏳ Bright Data CAPTCHA solver active — waiting up to {wait_secs}s...")
    deadline = time.time() + wait_secs
    while time.time() < deadline:
        if page_is_rex_home(driver):
            break
        if page_has_flights(driver):
            break
        if page_has_no_fare(driver):
            break
        time.sleep(2)
    print("   ✅ CAPTCHA wait complete")


def wait_for_rex_home(driver, timeout: int = 90) -> bool:
    """Wait for Rex homepage booking form to become ready."""
    print("   ⏳ Waiting for Rex homepage form...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if page_is_rex_home(driver):
            print("   ✅ Rex booking form ready")
            return True
        # Check for Continue button
        for pattern in ["Continue", "continiew"]:
            try:
                els = driver.find_elements(By.XPATH,
                    f"//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                    f"'abcdefghijklmnopqrstuvwxyz'), '{pattern.lower()}')]")
                for el in els:
                    try:
                        if el.is_displayed() and el.is_enabled():
                            el.click()
                            print("   ✅ Continue button clicked")
                            time.sleep(2)
                            break
                    except Exception:
                        pass
            except Exception:
                pass
        time.sleep(1.5)
    print("   ❌ Rex homepage form did not appear in time")
    return False


def open_rex_home(driver, label: str, attempts: int = 3) -> bool:
    for attempt in range(1, attempts + 1):
        ts = int(time.time())
        url = f"https://www.rex.com.au/?codex_retry={ts}_{attempt}"
        print(f"   🌐 Rex homepage load (attempt {attempt}/{attempts})...")
        try:
            driver.get(url)
        except Exception as e:
            print(f"   ⚠️  Page load exception: {e}")

        wait_for_brightdata_captcha(driver, wait_secs=30)

        if wait_for_rex_home(driver, timeout=60):
            return True

        if attempt < attempts:
            time.sleep(5 * attempt)

    return False


def click_one_way(driver) -> bool:
    try:
        result = driver.execute_script("""
            const one = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_rbTripType_oneway');
            const ret = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_rbTripType_return');
            if (!one) return false;
            one.checked = true;
            if (ret) ret.checked = false;
            one.dispatchEvent(new Event('change', { bubbles: true }));
            one.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
            return true;
        """)
        if result:
            print("   ✅ One-way selected")
            return True
    except Exception:
        pass

    for sel in ["label[for*='rbTripType_oneway']", "label:has-text('One way')",
                "input[id*='rbTripType_oneway']"]:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            el.click()
            print("   ✅ One-way selected (fallback)")
            return True
        except Exception:
            pass

    print("   ❌ Could not select one-way trip type")
    return False


def select_airport(driver, select_id: str, code: str, label: str,
                   wait_for_option: bool = False) -> bool:
    if wait_for_option:
        # Wait for destination options to load
        deadline = time.time() + 20
        while time.time() < deadline:
            result = driver.execute_script("""
                const [id, code] = arguments;
                const el = document.getElementById(id);
                if (!el) return false;
                return Array.from(el.options).some(opt =>
                    opt.value === code ||
                    (opt.textContent || '').toUpperCase().includes('(' + code + ')')
                );
            """, select_id, code)
            if result:
                break
            time.sleep(0.5)

    result = driver.execute_script("""
        const [id, code] = arguments;
        const el = document.getElementById(id);
        if (!el) return { ok: false, reason: 'selector missing' };
        const option = Array.from(el.options).find(opt => {
            const val  = (opt.value || '').trim();
            const text = (opt.textContent || '').trim().toUpperCase();
            return val === code || text.includes('(' + code + ')') || text === code;
        });
        if (!option) return { ok: false, reason: 'option missing', available:
            Array.from(el.options).map(o => ({ v: o.value, t: o.textContent.trim() }))
        };
        el.value = option.value;
        option.selected = true;
        el.dispatchEvent(new Event('input',  { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        if (window.jQuery) window.jQuery(el).trigger('change');
        return { ok: true };
    """, select_id, code)

    if not result.get("ok"):
        print(f"   ❌ Could not set {label} airport: {code} — {result.get('reason')}")
        avail = result.get("available", [])
        if avail:
            print(f"      Available: {avail[:8]}")
        return False

    time.sleep(0.5)
    print(f"   ✅ {label} airport selected: {code}")
    return True


def set_departure_date(driver, target_dt: datetime) -> bool:
    date_text = rex_form_date(target_dt)
    try:
        result = driver.execute_script("""
            const dateText = arguments[0];
            const dateInput = document.getElementById('datefilter');
            const dep = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_HDepartureDate');
            const ret = document.getElementById('ContentPlaceHolder1_BookingHomepageV21_HReturnDate');
            if (!dateInput || !dep) return false;
            dateInput.value = dateText;
            dep.value = dateText;
            if (ret) ret.value = '';
            for (const el of [dateInput, dep].filter(Boolean)) {
                el.dispatchEvent(new Event('input',  { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }
            return true;
        """, date_text)
        if result:
            print(f"   📅 Date set: {date_text}")
            return True
    except Exception as e:
        print(f"   ⚠️  Direct date set failed: {e}")

    # Fallback: open date picker and click the target day
    try:
        date_input = driver.find_element(By.ID, "datefilter")
        date_input.click()
        time.sleep(1)

        # Navigate to the target month
        target_month = datetime(target_dt.year, target_dt.month, 1)
        for _ in range(18):
            try:
                month_els = driver.find_elements(By.CSS_SELECTOR, ".daterangepicker .month")
                if not month_els:
                    break
                month_text = month_els[0].text.strip()
                try:
                    shown = datetime.strptime(month_text, "%b %Y")
                except ValueError:
                    try:
                        shown = datetime.strptime(month_text, "%B %Y")
                    except ValueError:
                        break
                if shown.year == target_dt.year and shown.month == target_dt.month:
                    break
                nav = "next" if target_month > shown else "prev"
                btn = driver.find_element(By.CSS_SELECTOR, f".daterangepicker .{nav}")
                btn.click()
                time.sleep(0.4)
            except Exception:
                break

        day_re = re.compile(rf"^\s*{target_dt.day}\s*$")
        cells = driver.find_elements(By.CSS_SELECTOR, "td.available:not(.off)")
        for cell in cells:
            if day_re.match(cell.text):
                cell.click()
                print(f"   📅 Date picked from calendar: {date_text}")
                return True
    except Exception as e:
        print(f"   ⚠️  Date picker fallback failed: {e}")

    return False


def dismiss_cookie_popup(driver):
    """
    Dismiss the EU/cookie consent popup if it is covering the page.
    Rex uses a .eupopup overlay — click its accept/close button so it no
    longer intercepts clicks on the booking form buttons.
    """
    try:
        result = driver.execute_script("""
            const selectors = [
                '.eupopup-button_1',
                '.eupopup-closebutton',
                '[class*="eupopup"] button',
                '[id*="cookieConsent"] button',
                '[class*="cookie"] button',
            ];
            for (const sel of selectors) {
                const el = document.querySelector(sel);
                if (el) { el.click(); return sel; }
            }
            const overlay = document.querySelector('.eupopup-container, [class*="eupopup"]');
            if (overlay) { overlay.remove(); return 'removed overlay'; }
            return null;
        """)
        if result:
            print(f"   🍪 Cookie popup dismissed ({result})")
            time.sleep(0.8)
    except Exception:
        pass


def submit_form(driver) -> bool:
    submit_id = "ContentPlaceHolder1_BookingHomepageV21_SubmitBooking"

    # Step 1: Dismiss cookie/privacy popup that may be blocking the button
    dismiss_cookie_popup(driver)

    # Step 2: Scroll button into view and try normal Selenium click
    try:
        btn = driver.find_element(By.ID, submit_id)
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.4)
        btn.click()
        print("   ✅ Form submitted")
        return True
    except Exception as e:
        print(f"   ⚠️  Submit click failed; trying JS direct click: {e}")

    # Step 3: JS direct .click() — avoids strict-mode __doPostBack / caller issue
    try:
        result = driver.execute_script("""
            const btn = document.getElementById(arguments[0]);
            if (!btn) return false;
            btn.scrollIntoView({block: 'center'});
            btn.click();
            return true;
        """, submit_id)
        if result:
            print("   ✅ Form submitted (JS direct click)")
            return True
    except Exception as e:
        print(f"   ❌ JS submit failed: {e}")

    return False


def wait_for_search_result(driver, target_dt: datetime, timeout: int = 90) -> tuple[str, str]:
    """
    Wait for Rex search result page to load.
    Returns: (status, reason)
      status: 'SUCCESS' | 'NO_FARE_AVAILABLE' | 'BLOCKED' | 'TIMEOUT'
    """
    deadline = time.time() + timeout
    verification_strikes = 0          # how many consecutive verification loops we've seen
    MAX_VERIFICATION_STRIKES = 4      # give up and return BLOCKED after this many

    while time.time() < deadline:
        if page_has_flights(driver):
            print("   ✅ Flights visible on page")
            return STATUS_SUCCESS, "Flight results loaded"

        if page_has_no_fare(driver):
            print("   ℹ️  No fare signal detected")
            return STATUS_NO_FARE, "Rex page indicates no flight/fare available"

        if page_has_verification(driver):
            verification_strikes += 1
            if verification_strikes > MAX_VERIFICATION_STRIKES:
                print("   ❌ Verification page persists — giving up")
                return STATUS_BLOCKED, "Verification page not cleared after repeated attempts"

            remaining = max(20, int(deadline - time.time()))
            wait_budget = min(90, remaining - 5)   # leave 5s for post-click settle
            print(f"   🧩 Rex verification page — waiting up to {wait_budget}s for reCAPTCHA solve + Continue click...")

            clicked = click_avail_cont(driver, wait_secs=wait_budget)
            time.sleep(3 if clicked else 1)
            continue

        # Not on results, no-fare, or verification — normal loading, wait a bit
        verification_strikes = 0      # reset strike counter if page is transitioning normally
        time.sleep(1.5)

    return STATUS_TIMEOUT, f"Timed out after {timeout}s waiting for Rex result"


# ─────────────────────────────────────────────────────────────
#  CORE SCRAPE — ONE DATE, ONE ATTEMPT
# ─────────────────────────────────────────────────────────────

def scrape_one_date(origin: str, dest: str, target_dt: datetime,
                    attempt: int, store: OutputStore,
                    selenium_url: str = "") -> tuple[str, list[dict], str]:
    """
    Create a fresh driver → open homepage → fill form → submit → extract → quit driver.

    Returns: (status, rows, debug_artifacts_str)
    """
    date_str = output_date(target_dt)
    label    = f"{RUN_ID}_{origin}_{dest}_{date_str}_attempt_{attempt}"
    driver   = None
    url      = selenium_url or ROUTE_SELENIUM_URL.get((origin, dest), BD_PER_MJK_SELENIUM_URL)

    try:
        print(f"   🔌 Connecting to Bright Data Selenium (zone: {url.split('@')[1] if '@' in url else url})...")
        driver = make_driver(url)
        print("   ✅ Driver connected")

        # ── Step 1: Homepage ──────────────────────────────────
        if not open_rex_home(driver, f"{label}_home"):
            debug = save_debug(driver, f"{label}_home_failed",
                               {"status": "HOME_FAILED", "origin": origin, "dest": dest})
            return STATUS_FAILED, [], debug

        # ── Step 2: One-way ───────────────────────────────────
        if not click_one_way(driver):
            debug = save_debug(driver, f"{label}_oneway_failed")
            return STATUS_STRUCT, [], debug

        # ── Step 3: Route ─────────────────────────────────────
        origin_id = "ContentPlaceHolder1_BookingHomepageV21_OriginAirport"
        dest_id   = "ContentPlaceHolder1_BookingHomepageV21_DestinationAirport"

        if not select_airport(driver, origin_id, origin, "Origin"):
            debug = save_debug(driver, f"{label}_origin_failed")
            return STATUS_STRUCT, [], debug

        if not select_airport(driver, dest_id, dest, "Destination", wait_for_option=True):
            debug = save_debug(driver, f"{label}_dest_failed")
            return STATUS_STRUCT, [], debug

        # ── Step 4: Date ──────────────────────────────────────
        if not set_departure_date(driver, target_dt):
            debug = save_debug(driver, f"{label}_date_failed")
            return STATUS_STRUCT, [], debug

        # ── Step 5: Submit ────────────────────────────────────
        if not submit_form(driver):
            debug = save_debug(driver, f"{label}_submit_failed")
            return STATUS_STRUCT, [], debug

        # Brief CAPTCHA wait after form submit
        wait_for_brightdata_captcha(driver, wait_secs=CAPTCHA_WAIT)

        # Click Continue button if verification page appears right after submit
        if page_has_verification(driver):
            print("   🧩 Verification page after submit — waiting for reCAPTCHA + clicking Continue...")
            click_avail_cont(driver, wait_secs=60)
            time.sleep(3)

        # ── Step 6: Wait for result ───────────────────────────
        status, reason = wait_for_search_result(driver, target_dt, timeout=PAGE_TIMEOUT)

        if status == STATUS_NO_FARE:
            row = make_no_fare_row(date_str, origin, dest, reason, attempt - 1)
            return STATUS_NO_FARE, [row], ""

        if status != STATUS_SUCCESS:
            debug = save_debug(driver, f"{label}_{status}",
                               {"status": status, "reason": reason})
            return status, [], debug

        # ── Step 7: Extract flights ───────────────────────────
        body_text = page_body_text(driver)
        flights = extract_flights_from_body(body_text, origin, dest, date_str)

        if flights:
            # Check missing prices
            missing = [f for f in flights if f.get("Fare Price") in {"N/A", "-", ""}]
            if missing:
                debug = save_debug(driver, f"{label}_missing_price",
                                   {"status": STATUS_STRUCT,
                                    "reason": "Flights found but price missing",
                                    "parsed": len(flights)})
                for f in flights:
                    f["Status"]  = STATUS_STRUCT
                    f["Comment"] = "Price extraction failed — check debug"
                return STATUS_STRUCT, flights, debug
            return STATUS_SUCCESS, flights, ""

        # ZL found on page but could not extract flight rows
        if page_has_no_fare(driver):
            row = make_no_fare_row(date_str, origin, dest,
                                   "No fare confirmed after flight extraction attempt", attempt - 1)
            return STATUS_NO_FARE, [row], ""

        debug = save_debug(driver, f"{label}_no_extract",
                           {"status": STATUS_STRUCT,
                            "reason": "Page loaded with ZL but no flights extracted"})
        return STATUS_STRUCT, [], debug

    except Exception as exc:
        comment = f"Unhandled exception: {exc}"
        print(f"   ❌ {comment}")
        traceback.print_exc()
        debug = ""
        if driver:
            try:
                debug = save_debug(driver, f"{label}_exception",
                                   {"status": STATUS_FAILED, "reason": comment,
                                    "traceback": traceback.format_exc()})
            except Exception:
                pass
        return STATUS_FAILED, [], debug

    finally:
        if driver:
            try:
                driver.quit()
                print("   🔌 Driver quit")
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
#  ROUTE RUNNER — LOOP OVER ALL DATES
# ─────────────────────────────────────────────────────────────

def run_route(origin: str, dest: str, store: OutputStore):
    dates = build_date_list()
    origin_name = AIRPORT_MAP.get(origin, origin)
    dest_name   = AIRPORT_MAP.get(dest, dest)
    selenium_url = ROUTE_SELENIUM_URL.get((origin, dest), BD_PER_MJK_SELENIUM_URL)
    zone_label   = selenium_url.split("zone-")[-1].split(":")[0] if "zone-" in selenium_url else "unknown"

    print(f"\n{'█'*60}")
    print(f"  ROUTE : {origin} ({origin_name}) → {dest} ({dest_name})")
    print(f"  Zone  : {zone_label}")
    print(f"  Dates : {dates[0].strftime('%d-%m-%Y')} → {dates[-1].strftime('%d-%m-%Y')}")
    print(f"  Output: {OUTPUT_EXCEL}")
    print(f"  Attempts/date: {MAX_ATTEMPTS}")
    print(f"{'█'*60}\n")

    for idx, target_dt in enumerate(dates, 1):
        date_str = output_date(target_dt)

        print(f"\n{'═'*60}")
        print(f"📅 [{idx}/{len(dates)}]  {target_dt.strftime('%A, %d %b %Y')}")
        print(f"{'─'*60}")

        # Resume check — only skip if --resume flag was passed
        if RESUME_MODE and store.job_completed(origin, dest, date_str):
            print(f"   ↩️  Already completed (run {RUN_ID}) — skipping")
            continue

        final_status = STATUS_FAILED
        final_rows: list[dict] = []
        final_debug = ""

        for attempt in range(1, MAX_ATTEMPTS + 1):
            if attempt > 1:
                backoff = min(90, 8 * (2 ** (attempt - 2)) + random.uniform(0, 3))
                print(f"   ⏳ Backing off {backoff:.1f}s before attempt {attempt}...")
                time.sleep(backoff)

            print(f"   🔁 Attempt {attempt}/{MAX_ATTEMPTS} — {origin}→{dest} {date_str}")
            status, rows, debug = scrape_one_date(origin, dest, target_dt, attempt, store, selenium_url)

            final_status = status
            final_rows   = rows
            final_debug  = debug

            if status in COMPLETED_STATUSES:
                break  # Done — no retry needed

            if status == STATUS_NO_FARE:
                break  # Genuine no fare — no retry needed

            print(f"   ⚠️  Attempt {attempt} ended: {status}")

        # If no rows produced — write a failed row
        if not final_rows:
            final_rows = [make_failed_row(
                date_str, origin, dest, final_status,
                f"All {MAX_ATTEMPTS} attempts failed",
                MAX_ATTEMPTS - 1,
                final_debug,
            )]

        store.write_rows(origin, dest, date_str, final_rows)

        if final_status == STATUS_SUCCESS:
            print(f"   ✅ {len(final_rows)} flight row(s) saved.")
        elif final_status == STATUS_NO_FARE:
            print(f"   ℹ️  No fare for {date_str}")
        else:
            print(f"   ❌ {final_status}")


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Rex PER↔MJK Selenium Scraper")
    p.add_argument("--days", type=int, default=TOTAL_DAYS)
    p.add_argument("--start-offset-days", type=int, default=0,
                   help="0 = start from today (default). 1 = start from tomorrow.")
    p.add_argument("--output", default=OUTPUT_EXCEL)
    p.add_argument("--debug-dir", default=DEBUG_DIR)
    p.add_argument("--log-dir", default=LOG_DIR)
    p.add_argument("--run-id", default=RUN_ID)
    p.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS)
    p.add_argument("--page-timeout", type=int, default=PAGE_TIMEOUT)
    p.add_argument("--captcha-wait", type=int, default=CAPTCHA_WAIT)
    p.add_argument("--resume", action="store_true",
                   help="Resume a previous run — skip dates already in the output file.")
    p.add_argument(
        "--routes",
        default="",
        help="Specify routes via CLI, e.g. PER-MJK,MJK-PER  (leave blank for interactive menu)",
    )
    return p.parse_args()


def interactive_route_selection() -> list[tuple[str, str]]:
    """
    Interactive menu — user selects which route(s) to run.
    """
    print("\n" + "═" * 60)
    print("  REX PER ↔ MJK SCRAPER — ROUTE SELECTION")
    print("═" * 60)
    print()
    print("  Available routes:")
    print()
    for i, (o, d) in enumerate(ROUTES, 1):
        print(f"    {i}.  {o} → {d}  ({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})")
    print()
    print("    0.  ✅ BOTH ROUTES (PER→MJK + MJK→PER)")
    print()

    while True:
        raw = input("  Enter route number(s) (0 or 1,2 or just 1): ").strip()
        if not raw:
            continue
        tokens = [t.strip() for t in raw.split(",") if t.strip()]
        try:
            selections = [int(t) for t in tokens]
        except ValueError:
            print("  ❌ Please enter numbers only.\n")
            continue

        if 0 in selections:
            print(f"\n  ✅ Both routes selected.\n")
            return list(ROUTES)

        bad = [s for s in selections if s < 1 or s > len(ROUTES)]
        if bad:
            print(f"  ❌ Invalid number(s): {bad}. Must be between 1 and {len(ROUTES)}.\n")
            continue

        chosen = [ROUTES[s - 1] for s in selections]
        print("\n  Selected routes:")
        for o, d in chosen:
            print(f"    ✈️  {o} → {d}  ({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})")
        print()
        confirm = input("  Confirm? (y/n): ").strip().lower()
        if confirm in {"y", "yes", ""}:
            return chosen
        print("  Please select again.\n")


if __name__ == "__main__":
    ns = parse_args()

    # Apply CLI config globally
    TOTAL_DAYS   = max(1, ns.days)
    START_OFFSET = max(0, ns.start_offset_days)  # 0 = start from today
    OUTPUT_EXCEL = ns.output
    DEBUG_DIR    = ns.debug_dir
    LOG_DIR      = ns.log_dir
    RUN_ID       = ns.run_id
    MAX_ATTEMPTS = max(1, ns.max_attempts)
    PAGE_TIMEOUT = max(30, ns.page_timeout)
    CAPTCHA_WAIT = max(10, ns.captcha_wait)
    RESUME_MODE  = ns.resume

    # Logging
    if LOG_DIR:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_path = os.path.join(LOG_DIR, f"rex_per_mjk_{RUN_ID}_{rex_now():%Y%m%d_%H%M%S}.log")
        log_fh   = open(log_path, "a", encoding="utf-8")
        print(f"🧾 Log: {log_path}")

    # ── ROUTE SELECTION ──────────────────────────────────────
    # If --routes given on CLI → use it; otherwise show interactive menu
    if ns.routes.strip():
        routes_to_run = []
        for token in ns.routes.split(","):
            parts = re.split(r"[-:>]", token.strip().upper())
            parts = [p for p in parts if p]
            if len(parts) == 2:
                routes_to_run.append((parts[0], parts[1]))
        if not routes_to_run:
            print("❌ Could not parse --routes; falling back to interactive menu.")
            routes_to_run = interactive_route_selection()
    else:
        routes_to_run = interactive_route_selection()

    store = OutputStore(OUTPUT_EXCEL)

    # Summary
    print("\n" + "═" * 60)
    print("  REX PER ↔ MJK SCRAPER — RUN SUMMARY")
    print("═" * 60)
    for o, d in routes_to_run:
        print(f"  ✈️   {o} → {d}  ({AIRPORT_MAP.get(o,'?')} → {AIRPORT_MAP.get(d,'?')})")
    print(f"  📆 Dates   : {TOTAL_DAYS} days  (from {output_date(build_date_list()[0])}  [today])")
    print(f"  🗂  Output  : {OUTPUT_EXCEL}")
    print(f"  🔁 Run ID  : {RUN_ID}")
    print(f"  ↩️  Resume  : {'Yes — skipping completed dates' if RESUME_MODE else 'No — fresh run (all dates)'}")
    print(f"  🌐 Engine  : Selenium (Bright Data Remote)")
    print("═" * 60 + "\n")

    route_status = {}
    try:
        for origin, dest in routes_to_run:
            try:
                run_route(origin, dest, store)
                route_status[(origin, dest)] = "ok"
            except KeyboardInterrupt:
                print(f"\n⛔ Stopped at {origin}→{dest}")
                route_status[(origin, dest)] = "interrupted"
                break
            except Exception as exc:
                route_status[(origin, dest)] = f"ERROR: {exc}"
                print(f"❌ Route {origin}→{dest} fatal error: {exc}")
    finally:
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
                print(f"  ❌  {o} → {d}  — {st}")
        print("═" * 60)
        print(f"\n📊 Output: {OUTPUT_EXCEL}\n")
