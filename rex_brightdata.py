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
import argparse
import requests
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from openpyxl import load_workbook, Workbook

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────
#  BRIGHT DATA CREDENTIALS
# ─────────────────────────────────────────────────────────────
BD_BROWSER_HOST = os.getenv("BD_BROWSER_HOST", "brd.superproxy.io")
BD_BROWSER_PORT = os.getenv("BD_BROWSER_PORT", "9222")
BD_BROWSER_USER = os.getenv(
    "BD_BROWSER_USER",
    "brd-customer-hl_fbc4a16a-zone-scraping_browser1",
)
BD_BROWSER_PASS = os.getenv("BD_BROWSER_PASS", "cdyjh3mz4oib")
BD_AUTH_TOKEN = os.getenv(
    "BD_AUTH_TOKEN",
    "7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7",
)
BD_WEB_UNLOCKER_ZONE = os.getenv("BD_WEB_UNLOCKER_ZONE", "unblocker1")
BD_UNLOCKER_ENDPOINT = os.getenv(
    "BD_UNLOCKER_ENDPOINT",
    "https://api.brightdata.com/request",
)
BD_BROWSER_WSS = os.getenv(
    "BD_BROWSER_WSS",
    f"wss://{BD_BROWSER_USER}:{BD_BROWSER_PASS}@{BD_BROWSER_HOST}:{BD_BROWSER_PORT}",
)

# Empty-result warning counter. Default is intentionally high so 84-day runs
# keep recording every date even when many dates have no flights.
MAX_EMPTY_STREAK = int(os.getenv("REX_MAX_EMPTY_STREAK", "999999"))


def web_unlocker_get(url: str) -> str:
    """Bright Data Web Unlocker API se HTML fetch karo."""
    resp = requests.post(
        BD_UNLOCKER_ENDPOINT,
        json={"zone": BD_WEB_UNLOCKER_ZONE, "url": url, "format": "raw"},
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
    ("PER", "MJK"), ("MJK", "PER"),
    ("CVQ", "MJK"), ("MJK", "CVQ"),
]

CONNECTING_ROUTES = {("CVQ", "MJK"), ("MJK", "CVQ")}

# Yeh routes Rex pe ribbon nahi dikhate — hamesha fresh search karenge
NO_RIBBON_ROUTES = {("PER", "MJK"), ("MJK", "PER")}

TOTAL_DAYS = int(os.getenv("REX_TOTAL_DAYS", "84"))
OUTPUT_EXCEL = os.getenv("REX_OUTPUT_EXCEL", "rex_results_all_routes.xlsx")

CSV_FIELDS = [
    "Date Checked", "Time Checked", "Airline",
    "Date of Departure", "Time of Departure",
    "Origin", "Destination",
    "Fare Price", "Fare Class", "Source",
]

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

def today_dt() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo('Australia/Perth')).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    except Exception:
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def build_date_list() -> list[datetime]:
    t = today_dt()
    return [t + timedelta(days=i) for i in range(TOTAL_DAYS)]


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
    """
    all_times = extract_all_times_from_text(card_text)
    zl_matches = list(re.finditer(r'ZL\s?\d{3,4}', card_text))

    result = {}
    for idx, m in enumerate(zl_matches):
        flight = re.sub(r'\s', '', m.group())
        dep_idx = idx * 2  # departure time = even index (0, 2, 4, ...)
        dep = all_times[dep_idx] if dep_idx < len(all_times) else "-"
        result[flight] = dep
    return result


def _ensure_cents(val: str) -> str:
    return val if '.' in val else f"{val}.00"


def append_rows(rows: list):
    if not rows:
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
    m = re.search(r'[Ff]rom\s*\$\s*([\d,]+\.\d{2})', card_text)
    if m:
        return f"${m.group(1)}"
    m = re.search(r'\$\s*([\d,]+\.\d{2})', card_text)
    if m:
        return f"${m.group(1)}"
    m = re.search(r'\$\s*(\d[\d,]*)', card_text)
    if m:
        return f"${_ensure_cents(m.group(1))}"
    return "N/A"


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
    return len(full_body) // 7


def extract_price_from_flight_window(full_body: str, flight_match,
                                     ribbon_end_pos: int):
    if flight_match.start() < ribbon_end_pos:
        return None

    after_start = flight_match.end()
    after_end   = min(len(full_body), after_start + 600)
    window      = full_body[after_start:after_end]

    m = re.search(r'[Ff]rom\s*\$\s*([\d,]+\.\d{2})', window)
    if m:
        return f"${m.group(1)}"

    m = re.search(r'[Ss]elect\s+[Ff]ares?\s*\$\s*([\d,]+\.\d{2})', window)
    if m:
        return f"${m.group(1)}"

    decimals = re.findall(r'\$\s*([\d,]+\.\d{2})', window)
    if decimals:
        return f"${decimals[0]}"

    m = re.search(r'\$\s*(\d[\d,]*)', window)
    if m:
        return f"${_ensure_cents(m.group(1))}"

    return "N/A"


# ─────────────────────────────────────────────────────────────
#  SCRAPER
# ─────────────────────────────────────────────────────────────

class RexScraper:

    def __init__(self, headless=False):
        self.headless = headless
        self._last_ribbon_price = ""

    async def save_debug_artifacts(self, page, label: str):
        os.makedirs("rex_debug", exist_ok=True)
        safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_") or "page"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        png_path = os.path.join("rex_debug", f"{safe_label}_{ts}.png")
        html_path = os.path.join("rex_debug", f"{safe_label}_{ts}.html")

        try:
            await page.screenshot(path=png_path, full_page=True)
            print(f"   📸 Debug screenshot: {os.path.abspath(png_path)}")
        except Exception as exc:
            print(f"   ⚠️  Screenshot failed: {exc}")

        try:
            html = await page.content()
            with open(html_path, "w", encoding="utf-8") as fh:
                fh.write(html)
            print(f"   🧾 Debug HTML: {os.path.abspath(html_path)}")
        except Exception as exc:
            print(f"   ⚠️  HTML dump failed: {exc}")

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
        ]
        return any(marker in text for marker in markers)

    async def open_rex_home(self, page, label: str, attempts: int = 4) -> bool:
        for attempt in range(1, attempts + 1):
            url = f"https://www.rex.com.au/?codex_retry={int(datetime.now().timestamp())}_{attempt}"
            print(f"   🌐 Loading Rex homepage (attempt {attempt}/{attempts})...")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except Exception as exc:
                print(f"   ⚠️  Homepage load failed: {exc}")

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

    async def wait_for_brightdata_captcha(self, page, detect_timeout: int = 60000):
        try:
            client = await page.context.new_cdp_session(page)
            await client.send("Captcha.waitForSolve", {"detectTimeout": detect_timeout})
            print("   ✅ Bright Data captcha solve step completed")
            return True
        except Exception:
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
        deadline = asyncio.get_event_loop().time() + timeout
        captcha_waited = False

        while asyncio.get_event_loop().time() < deadline:
            if await self.rex_home_ready(page):
                print("   ✅ Rex booking form is ready")
                return True

            if await self.click_continue_if_present(page):
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass

                form_deadline = asyncio.get_event_loop().time() + 18
                while asyncio.get_event_loop().time() < form_deadline:
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

    def parse_tab_date(self, text: str) -> datetime | None:
        m = re.search(
            r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\s+(\d{1,2})\s+'
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
            text, re.IGNORECASE
        )
        if not m:
            return None
        day, mon = int(m.group(1)), m.group(2).capitalize()
        now = datetime.now()
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
                    "$" not in raw,
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
                                       timeout: int = 15) -> bool:
        exp_day = str(target_dt.day)
        exp_mon = target_dt.strftime("%b")
        date_ok    = False
        flights_ok = False

        for tick in range(timeout * 2):
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
                    cards = await page.query_selector_all(sel)
                    if cards:
                        for card in cards[:3]:
                            txt = (await card.inner_text()).strip()
                            if re.search(r'ZL\s?\d{3,4}', txt):
                                flights_ok = True
                                break
                    if flights_ok:
                        break

                if not flights_ok:
                    body_snippet = await page.inner_text("body")
                    if re.search(r'ZL\s?\d{3,4}', body_snippet[:5000]):
                        flights_ok = True

            if date_ok and flights_ok:
                print(f"   ✅ Page loaded — date synced + flights visible")
                return True

            await asyncio.sleep(0.5)

        if flights_ok:
            print(f"   ⚠️  Flights visible but date header uncertain — proceeding")
            return True

        print(f"   ⚠️  Load timeout ({timeout}s) — extracting anyway")
        return False

    # ── Extract flights ──────────────────────────────────────
    async def extract_flights(self, page, date_str, origin, dest) -> list:
        now     = datetime.now()
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
                text  = (await row.inner_text()).strip()
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
        full_body = await page.inner_text("body")

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
             dep_time, orig, dest, price):
        return {
            "Date Checked":      ck_date,
            "Time Checked":      ck_time,
            "Airline":           airline,
            "Date of Departure": dep_date,
            "Time of Departure": dep_time,   # Raw as-is from website
            "Origin":            orig,
            "Destination":       dest,
            "Fare Price":        price,
            "Fare Class":        "Economy",
            "Source":            "Rex Website",
        }

    def _no_flight(self, date_str, orig, dest):
        now = datetime.now()
        return self._row(now.strftime("%d-%m-%Y"), now.strftime("%H:%M:%S"),
                         "no flight", date_str, "-", orig, dest, "-")

    def _site_unavailable(self, date_str, orig, dest):
        now = datetime.now()
        row = self._row(now.strftime("%d-%m-%Y"), now.strftime("%H:%M:%S"),
                        "site unavailable", date_str, "-", orig, dest, "-")
        row["Source"] = "Rex Website - unavailable"
        return row

    def write_unavailable_route_rows(self, origin_code, dest_code, reason: str):
        print(f"   🧾 Writing unavailable rows: {reason}")
        rows = [
            self._site_unavailable(dt.strftime("%d-%m-%Y"), origin_code, dest_code)
            for dt in build_date_list()
        ]
        append_rows(rows)

    # ─────────────────────────────────────────────────────────
    #  FRESH SEARCH (jab ribbon mein date nahi milti)
    # ─────────────────────────────────────────────────────────
    async def do_fresh_search(self, page, origin_name: str, dest_name: str,
                               target_dt: datetime) -> bool:
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

        Returns True agar page successfully load ho gayi, False agar error.
        """
        print(f"   🔄 Fresh search: {origin_name} → {dest_name} "
              f"on {target_dt.strftime('%d %b %Y')}")
        try:
            if not await self.open_rex_home(page, "fresh_search_home"):
                return False

            if not await self.click_one_way(page, "fresh_search_one_way_failed"):
                return False

            # Origin
            await page.locator(
                "#ContentPlaceHolder1_BookingHomepageV21_OriginAirport + .select2-container"
            ).click()
            await page.locator(".select2-search__field").fill(origin_name)
            await page.locator(".select2-results__option").filter(
                has_text=origin_name
            ).first.click()

            # Destination
            await page.locator(
                "#ContentPlaceHolder1_BookingHomepageV21_DestinationAirport + .select2-container"
            ).click()
            await page.locator(".select2-search__field").fill(dest_name)
            await page.locator(".select2-results__option").filter(
                has_text=dest_name
            ).first.click()

            # Date picker — target date select karo
            await page.locator("#datefilter").click()
            await asyncio.sleep(1)

            # Correct month tak navigate karo
            for _ in range(12):  # max 12 months aage
                # Current visible month check karo
                try:
                    month_txt = await page.locator(
                        ".daterangepicker .month"
                    ).first.inner_text(timeout=1000)
                    visible_month = datetime.strptime(month_txt.strip(), "%b %Y")
                    if (visible_month.year == target_dt.year and
                            visible_month.month == target_dt.month):
                        break
                    # Aage jao
                    await page.locator(
                        ".daterangepicker .next"
                    ).first.click()
                    await asyncio.sleep(0.5)
                except:
                    break

            # Target date ka day click karo
            await page.locator(
                ".daterangepicker td.available:not(.off)"
            ).filter(has_text=str(target_dt.day)).first.click()
            await asyncio.sleep(0.5)

            # Submit
            await page.locator(
                "#ContentPlaceHolder1_BookingHomepageV21_SubmitBooking"
            ).click()

            print("   ⏳ Bright Data auto-solving Google Captcha...")
            await self.wait_for_brightdata_captcha(page, detect_timeout=60000)
            await self.click_continue_if_present(page)

            # Flight list load hone ka wait
            loaded = await self.wait_for_flights_loaded(page, target_dt, timeout=20)
            await asyncio.sleep(1)
            print(f"   ✅ Fresh search complete — loaded={loaded}")
            return True

        except Exception as e:
            print(f"   ❌ Fresh search failed: {e}")
            return False

    # ─────────────────────────────────────────────────────────
    #  RIBBON NAVIGATION
    # ─────────────────────────────────────────────────────────
    async def run_ribbon_route(self, page, origin_code, dest_code):
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
                ok = await self.do_fresh_search(
                    page, origin_name, dest_name, target_dt
                )
                if not ok:
                    print("   ❌ Fresh search bhi fail — 'no flight'")
                    append_rows([self._no_flight(date_str, origin_code, dest_code)])
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
    #  FRESH SEARCH ROUTE (PER↔MJK — ribbon nahi aata)
    #  Baki sab same, sirf ribbon skip karke har date ke liye
    #  directly fresh search karta hai
    # ─────────────────────────────────────────────────────────
    async def run_fresh_search_route(self, page, origin_code, dest_code):
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
            ok = await self.do_fresh_search(
                page, origin_name, dest_name, target_dt
            )
            if not ok:
                print("   ❌ Fresh search fail — 'no flight'")
                append_rows([self._no_flight(date_str, origin_code, dest_code)])
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
    async def run_route(self, origin_code, dest_code):
        origin_name = AIRPORT_MAP.get(origin_code, origin_code)
        dest_name   = AIRPORT_MAP.get(dest_code, dest_code)
        dates       = build_date_list()

        print(f"\n{'█'*60}")
        print(f"  ROUTE : {origin_code} ({origin_name}) → {dest_code} ({dest_name})")
        rtype = "Connecting" if (origin_code, dest_code) in CONNECTING_ROUTES else "Independent"
        print(f"  Type  : {rtype}")
        print(f"  Window: {dates[0].strftime('%d-%m-%Y')} → {dates[-1].strftime('%d-%m-%Y')}")
        print(f"  Output: {OUTPUT_EXCEL}")
        print(f"{'█'*60}")

        async with async_playwright() as p:
            print("🔌 Connecting to Bright Data Browser API...")
            browser = await p.chromium.connect_over_cdp(BD_BROWSER_WSS)
            context = await browser.new_context(viewport={"width": 1366, "height": 900})
            page    = await context.new_page()
            print("✅ Browser API connected.")

            try:
                if not await self.open_rex_home(page, "initial_home"):
                    self.write_unavailable_route_rows(
                        origin_code, dest_code,
                        "Rex homepage did not load after retries",
                    )
                    return

                if not await self.click_one_way(page, "initial_one_way_failed"):
                    self.write_unavailable_route_rows(
                        origin_code, dest_code,
                        "Could not select one-way trip type",
                    )
                    return

                await page.locator(
                    "#ContentPlaceHolder1_BookingHomepageV21_OriginAirport + .select2-container"
                ).click()
                await page.locator(".select2-search__field").fill(origin_name)
                await page.locator(".select2-results__option").filter(
                    has_text=origin_name
                ).first.click()

                await page.locator(
                    "#ContentPlaceHolder1_BookingHomepageV21_DestinationAirport + .select2-container"
                ).click()
                await page.locator(".select2-search__field").fill(dest_name)
                await page.locator(".select2-results__option").filter(
                    has_text=dest_name
                ).first.click()

                today = datetime.now()
                await page.locator("#datefilter").click()
                await page.locator(
                    ".daterangepicker td.available:not(.off)"
                ).filter(has_text=str(today.day)).first.click()

                await page.locator(
                    "#ContentPlaceHolder1_BookingHomepageV21_SubmitBooking"
                ).click()

                print("⏳ Bright Data CAPTCHA solver active — auto-solving Google Captcha...")
                await self.wait_for_brightdata_captcha(page, detect_timeout=60000)
                await self.click_continue_if_present(page)

                print("⏳ Auto-waiting for flights to load...")
                await self.wait_for_flights_loaded(page, dates[0], timeout=30)
                await asyncio.sleep(2)
                print("✅ Page ready — proceeding automatically.")

                # PER↔MJK pe ribbon nahi aata — fresh search loop use karo
                if (origin_code, dest_code) in {("PER", "MJK"), ("MJK", "PER")}:
                    await self.run_fresh_search_route(page, origin_code, dest_code)
                else:
                    await self.run_ribbon_route(page, origin_code, dest_code)

            except KeyboardInterrupt:
                print("\n⛔ Interrupted.")
            except Exception as e:
                print(f"\n❌ Fatal error: {e}")
                import traceback; traceback.print_exc()
            finally:
                await context.close()
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
    parser.add_argument("--output", default=OUTPUT_EXCEL, help="Excel output file")
    parser.add_argument("--list", action="store_true", help="Show supported routes")
    parser.add_argument("--skip-unblocker-check", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    ns = parse_args()

    if ns.list:
        print_usage()
        sys.exit(0)

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

    TOTAL_DAYS = max(1, ns.days)
    OUTPUT_EXCEL = ns.output

    if not ns.skip_unblocker_check:
        check_web_unlocker()

    scraper = RexScraper(headless=False)

    print(f"\n🗓️  TODAY = {today_dt().strftime('%A, %d-%m-%Y')}  (Day 0)")
    print(f"📋 Routes: {len(routes_to_run)}")
    print(f"📆 Dates per route: {TOTAL_DAYS}")
    print("🌐 Bright Data Browser API: ENABLED")
    print("🌐 Bright Data Web Unblocker: integrated")
    print(f"📊 Output: {OUTPUT_EXCEL}\n")

    for origin, dest in routes_to_run:
        try:
            asyncio.run(scraper.run_route(origin, dest))
        except KeyboardInterrupt:
            print(f"\n⛔ Stopped at {origin}→{dest}.")
            break
