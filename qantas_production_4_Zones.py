"""
Qantas Fare Tracker v12 — Multi-Zone Bright Data Edition
=========================================================
WHAT'S NEW vs v11:
  - 4 dedicated BD zones — one per route, zero conflict
  - Output filename: Qantas_BME-KNX_20260512.xlsx (client-ready format)
  - --workers N flag: run 1, 2, 3, or 4 routes in parallel
  - --route N flag: run a single specific route
  - Cleaner credential management via ROUTE_CREDENTIALS dict
  - All existing reliability fixes preserved (retry, heartbeat, gap-fill, etc.)

Routes:
  1. BME → KNX  (zone: scraping_browser2)
  2. BME → DRW  (zone: qantas_1)   ← Special: Direct Only + Shadow DOM
  3. DRW → KNX  (zone: qantas_2)
  4. KNX → BME  (zone: qantas_3)

Usage:
  python qantas_production_final.py                  # Interactive menu
  python qantas_production_final.py --workers 4      # All 4 routes parallel
  python qantas_production_final.py --workers 2      # 2 routes parallel
  python qantas_production_final.py --route 1        # Only BME→KNX
  python qantas_production_final.py --route 2        # Only BME→DRW
"""

import time
import sys
import re
import random
import traceback
import argparse
import threading
import os
from datetime import datetime, timedelta, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from selenium.webdriver import Remote, ChromeOptions as Options
from selenium.webdriver.chromium.remote_connection import ChromiumRemoteConnection as Connection
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ══════════════════════════════════════════════════════════════════
#  ROUTES & CREDENTIALS — Edit only this section if things change
# ══════════════════════════════════════════════════════════════════

def _env_zone(env_name, default, legacy_env_name=None):
    """Read a Bright Data zone from env and normalize full usernames to zone names."""
    value = os.getenv(env_name)
    if not value and legacy_env_name:
        value = os.getenv(legacy_env_name)
    value = (value or default).strip()
    if "-zone-" in value:
        value = value.split("-zone-", 1)[1]
    return value


ROUTES = [
    ("BME", "KNX"),   # Route 1 — Standard
    ("BME", "DRW"),   # Route 2 — Special: Direct Only
    ("DRW", "KNX"),   # Route 3 — Special: Direct Only
    ("KNX", "BME"),   # Route 4 — Standard
]

# Each route gets its OWN zone — zero conflict guaranteed
ROUTE_CREDENTIALS = {
    ("BME", "KNX"): {
        "zone": _env_zone("QANTAS_BME_KNX_ZONE", "scraping_browser2", "BRIGHTDATA_ZONE"),
        "password": os.getenv("QANTAS_BME_KNX_PASS", os.getenv("BRIGHTDATA_PASS", "nymmsv0ffs60")),
    },
    ("BME", "DRW"): {
        "zone": _env_zone("QANTAS_BME_DRW_ZONE", "qantas_1"),
        "password": os.getenv("QANTAS_BME_DRW_PASS", "x9ck9dpthpsg"),
    },
    ("DRW", "KNX"): {
        "zone": _env_zone("QANTAS_DRW_KNX_ZONE", "qantas_2"),
        "password": os.getenv("QANTAS_DRW_KNX_PASS", "kgu154ajo3d9"),
    },
    ("KNX", "BME"): {
        "zone": _env_zone("QANTAS_KNX_BME_ZONE", "qantas_3"),
        "password": os.getenv("QANTAS_KNX_BME_PASS", "n748kj03bomt"),
    },
}

BRIGHTDATA_HOST     = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")
BRIGHTDATA_PORT     = int(os.getenv("BRIGHTDATA_PORT", "9515"))
CUSTOMER_ID         = os.getenv("BRIGHTDATA_CUSTOMER_ID", "hl_fbc4a16a")

AIRPORT_NAMES = {"BME": "Broome", "KNX": "Kununurra", "DRW": "Darwin"}
AIRLINE       = "Qantas"
SOURCE        = "qantas.com"
DAYS_OUT      = 84
OUTPUT_DIR    = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Tuning constants ──────────────────────────────────────────────
MAX_SEARCH_RETRIES   = 5
NO_NEW_STREAK_LIMIT  = 3
NEXT_ARROW_RETRIES   = 2
TAB_SLEEP            = 4
NEXT_SLEEP           = 4
DATES_PER_SESSION    = 10
MIN_DELAY            = 2.5
MAX_DELAY            = 5.5

# Thread-safe print lock
_print_lock = threading.Lock()

def tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs)

# ══════════════════════════════════════════════════════════════════
#  DRIVER MANAGEMENT
# ══════════════════════════════════════════════════════════════════

def _make_user(zone, country="au"):
    sid = random.randint(1000000, 9999999)
    return f"brd-customer-{CUSTOMER_ID}-zone-{zone}-country-{country}-session-{sid}"


def make_driver(route_key, country="au"):
    """Connect to Bright Data using route-specific zone — fresh session/IP every call."""
    from selenium.webdriver.remote.client_config import ClientConfig

    creds = ROUTE_CREDENTIALS[route_key]
    zone  = creds["zone"]
    pwd   = creds["password"]
    user  = _make_user(zone, country)

    tprint(f"  🌐 [{route_key[0]}→{route_key[1]}] Connecting BD zone={zone} session={user.split('-session-')[-1]}")
    server_url = f"https://{BRIGHTDATA_HOST}:{BRIGHTDATA_PORT}"
    config     = ClientConfig(remote_server_addr=server_url, username=user, password=pwd)
    connection = Connection(server_url, "goog", "chrome", client_config=config)

    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--lang=en-AU")
    try:
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
    except Exception:
        pass

    driver = Remote(connection, options=opts)
    driver.set_page_load_timeout(120)
    driver.set_script_timeout(120)
    driver.implicitly_wait(10)

    try:
        driver.execute_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
            "Object.defineProperty(navigator,'languages',{get:()=>['en-AU','en']});"
            "window.chrome={runtime:{}};"
        )
    except Exception:
        pass

    tprint(f"  ✅ [{route_key[0]}→{route_key[1]}] Connected to Bright Data!")
    return driver


def safe_quit(driver):
    """Always use this — gives BD ~12s to free the session slot."""
    try:
        driver.quit()
    except Exception:
        pass
    time.sleep(12)

# ══════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════

def random_delay(a=MIN_DELAY, b=MAX_DELAY):
    time.sleep(random.uniform(a, b))


def human_type(element, text):
    for ch in text:
        element.send_keys(ch)
        time.sleep(random.uniform(0.08, 0.22))


def driver_heartbeat(driver):
    try:
        driver.current_url
        return True
    except Exception:
        return False


def capture_debug(driver, name="debug"):
    try:
        debug_dir = Path("debug")
        debug_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        driver.save_screenshot(str(debug_dir / f"{name}_{ts}.png"))
        with open(debug_dir / f"{name}_{ts}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception:
        pass


def parse_date(text, ref_year):
    """Parse '2 May', 'Sat 2 May', 'Wed 6 May' → date."""
    try:
        clean = re.sub(r'^[A-Za-z]{3,}\s+', '', text.strip())
        clean = re.sub(r'^[^0-9]+', '', clean).strip()
        m = re.match(r'(\d+\s+[A-Za-z]+)', clean)
        if m:
            clean = m.group(1)
        dt     = datetime.strptime(f"{clean} {ref_year}", "%d %b %Y")
        result = dt.date()
        today  = date.today()
        if result < today - timedelta(days=30):
            dt     = datetime.strptime(f"{clean} {ref_year + 1}", "%d %b %Y")
            result = dt.date()
        return result
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════
#  SCRAPING HELPERS
# ══════════════════════════════════════════════════════════════════

def extract_ribbon_tabs(driver, today):
    raw = driver.execute_script("""
        let selectors = [
            '.cal-tab-body',
            '[id*="tab-date"]',
            '.date-ribbon__tab',
            '.flex-linear-calendar button',
            '[role="tab"]'
        ];
        let tabs = [];
        for (let sel of selectors) {
            let found = Array.from(document.querySelectorAll(sel)).filter(t => {
                let txt = (t.innerText || '').trim();
                return /\\d/.test(txt) && txt.length < 300 && !txt.includes('Privacy') && !txt.includes('Cookie');
            });
            if (found.length > 0) { tabs = found; break; }
        }
        return tabs.map((t, i) => ({
            index : i,
            text  : (t.innerText || '').trim(),
        }));
    """)

    results = []
    for item in (raw or []):
        text      = item["text"]
        date_part = re.split(r'\$|No flights|LOWEST|This is|Next|Price|Economy', text, flags=re.IGNORECASE)[0].strip()
        date_obj  = parse_date(date_part, today.year)
        if not date_obj or date_obj < today:
            continue
        no_flight = "no flights" in text.lower()
        results.append({
            "date_obj"  : date_obj,
            "date_str"  : str(date_obj),
            "no_flight" : no_flight,
            "tab_index" : item["index"],
            "raw_text"  : text
        })
    return results


def click_tab(driver, tab_index):
    driver.execute_script("""
        let selectors = ['.cal-tab-body', '[id*="tab-date"]', '.date-ribbon__tab', '.flex-linear-calendar button', '[role="tab"]'];
        let tabs = [];
        for (let sel of selectors) {
            let found = Array.from(document.querySelectorAll(sel)).filter(t => {
                let txt = (t.innerText || '').trim();
                return /\\d/.test(txt) && !txt.includes('Privacy') && !txt.includes('Cookie');
            });
            if (found.length > 0) { tabs = found; break; }
        }
        if (tabs[arguments[0]]) {
            tabs[arguments[0]].scrollIntoView({block: 'center', inline: 'center'});
            tabs[arguments[0]].click();
        }
    """, tab_index)
    time.sleep(TAB_SLEEP)


def scrape_flight_cards_standard(driver):
    results = []
    try:
        data = driver.execute_script("""
            let rows = [];
            let rowEls = Array.from(document.querySelectorAll('grouped-avail-flight-row, [class*="flightRow"], [class*="flight-card"], .flight-card'))
                         .filter(r => r.offsetParent !== null);
            for (let row of rowEls) {
                let depTime = '';
                let timeEl = row.querySelector('[class*="depTime"], [class*="departureTime"], .departure-time, time');
                if (timeEl) {
                    let tm = timeEl.innerText.match(/\\d{1,2}:\\d{2}/);
                    depTime = tm ? tm[0] : '';
                }
                let ecoPrice = null, bizPrice = null;
                let cells = row.querySelectorAll('td, .upsell-cell, [class*="cell"]');
                for (let cell of cells) {
                    let cTxt = cell.innerText.toLowerCase();
                    if (cTxt.includes('no seats')) continue;
                    let m = cell.innerText.match(/\\$([0-9,]+)/);
                    if (m) {
                        let val = parseFloat(m[1].replace(',',''));
                        if (cTxt.includes('business')) bizPrice = val;
                        else ecoPrice = val;
                    }
                }
                if (ecoPrice || bizPrice) rows.push({ depTime, ecoPrice, bizPrice });
            }
            return rows;
        """)
        for item in (data or []):
            if item.get("ecoPrice"):
                results.append({"fare_class": "Economy",  "fare_price": item["ecoPrice"], "departure_time": item["depTime"]})
            if item.get("bizPrice"):
                results.append({"fare_class": "Business", "fare_price": item["bizPrice"], "departure_time": item["depTime"]})
    except Exception as _e:
        tprint(f'[WARN] {type(_e).__name__}: {_e}')
    return results


def scrape_flight_cards_shadow(driver, origin, dest):
    results = []
    time.sleep(7)
    origin_name = AIRPORT_NAMES.get(origin, origin).lower()
    dest_name   = AIRPORT_NAMES.get(dest, dest).lower()
    origin_code = origin.lower()
    dest_code   = dest.lower()

    try:
        data = driver.execute_script(f"""
            let results = [];
            let originName = "{origin_name}";
            let destName   = "{dest_name}";
            let originCode = "{origin_code}";
            let destCode   = "{dest_code}";

            function getDeepText(node) {{
                let text = node.innerText || '';
                if (node.shadowRoot) text += ' ' + getDeepText(node.shadowRoot);
                for (let child of node.children || []) text += ' ' + getDeepText(child);
                return text;
            }}
            function findFlightRows(root) {{
                let found = [];
                let candidates = root.querySelectorAll('grouped-avail-flight-row, .flight-card, .upsell-row, [class*="FlightCard"], [class*="flight-row"]');
                candidates.forEach(c => found.push(c));
                let all = root.querySelectorAll('*');
                all.forEach(el => {{ if (el.shadowRoot) found = found.concat(findFlightRows(el.shadowRoot)); }});
                return found;
            }}
            let rows = findFlightRows(document);
            if (rows.length === 0) rows = Array.from(document.querySelectorAll('div')).filter(d => d.innerText.length > 50 && d.innerText.length < 1500);

            for (let row of rows) {{
                if (row.closest && row.closest('.flex-linear-calendar, .date-ribbon, .search-bar')) continue;
                let txt = getDeepText(row);
                let lowerTxt = txt.toLowerCase();

                let hasRoute = (lowerTxt.includes(originName) && lowerTxt.includes(destName)) ||
                               (lowerTxt.includes(originCode) && lowerTxt.includes(destCode));

                if (originCode === 'bme' && destCode === 'drw') {{
                    if (lowerTxt.includes('airnorth')) hasRoute = true;
                }}

                if (hasRoute) {{
                    let times = txt.match(/(\\d{{1,2}}:\\d{{2}})/g);
                    if (!times || times.length < 1) continue;

                    let ecoPrice = null, bizPrice = null;
                    let priceMatches = txt.match(/\\$([0-9,]+)/g);

                    if (priceMatches) {{
                        let numericPrices = [];
                        priceMatches.forEach(p => {{
                            let val = parseFloat(p.replace(/[^0-9.]/g, ''));
                            if (!numericPrices.includes(val)) numericPrices.push(val);
                        }});

                        if (numericPrices.length === 1) {{
                            let priceStr = priceMatches[0];
                            let parts = txt.split(priceStr);
                            let prefix = parts[0].toLowerCase();
                            let suffix = (parts[1] || '').toLowerCase();
                            if (prefix.includes('no seats')) {{
                                bizPrice = numericPrices[0];
                            }} else {{
                                ecoPrice = numericPrices[0];
                                if (suffix.includes('no seats')) bizPrice = null;
                            }}
                        }} else if (numericPrices.length >= 2) {{
                            ecoPrice = numericPrices[0];
                            bizPrice = numericPrices[1];
                        }}
                    }}

                    let isDirect = !lowerTxt.includes('1 stop') && !lowerTxt.includes('2 stop') && !lowerTxt.includes('via') && !lowerTxt.includes('connect');
                    if (row.classList && row.classList.contains('e2e-direct-flight')) isDirect = true;

                    let specialRoutes = [['bme','drw'], ['drw','knx']];
                    let isSpecialRoute = specialRoutes.some(r => r[0] === originCode && r[1] === destCode);

                    if (isSpecialRoute) {{
                        let hubs = ['perth', 'sydney', 'melbourne', 'brisbane', 'adelaide', 'alice springs', 'cairns'];
                        let hubsToExclude = hubs.filter(h => h !== originName && h !== destName);
                        let containsHub = hubsToExclude.some(h => lowerTxt.includes(h));
                        if (lowerTxt.includes('airnorth') && !containsHub) {{
                            isDirect = true;
                        }} else {{
                            isDirect = false;
                        }}
                    }}

                    if (ecoPrice || bizPrice) {{
                        let key = times[0] + (ecoPrice || bizPrice);
                        if (!results.some(r => r.key === key)) {{
                            results.push({{ key: key, depTime: times[0], ecoPrice: ecoPrice, bizPrice: bizPrice, isDirect: isDirect }});
                        }}
                    }}
                }}
            }}
            return results;
        """)
        for item in (data or []):
            if item["isDirect"]:
                if item["ecoPrice"]:
                    results.append({"fare_class": "Economy",  "fare_price": item["ecoPrice"], "departure_time": item["depTime"]})
                if item["bizPrice"]:
                    results.append({"fare_class": "Business", "fare_price": item["bizPrice"], "departure_time": item["depTime"]})
    except Exception as _e:
        tprint(f'[WARN] {type(_e).__name__}: {_e}')
    return results


def click_next_arrow(driver):
    clicked = driver.execute_script("""
        let btn = Array.from(document.querySelectorAll('a, button')).find(b => {
            let txt = (b.innerText || '').toLowerCase().trim();
            return txt.includes('next') && (txt.includes('day') || txt.includes('14'));
        });
        if (!btn) {
            btn = Array.from(document.querySelectorAll('button')).find(b => {
                let lbl = (b.getAttribute('aria-label') || '').toLowerCase();
                let cls = (b.className || '').toLowerCase();
                return lbl.includes('next') || cls.includes('next-btn')
                    || cls.includes('nextbutton') || cls.includes('next-button');
            });
        }
        if (!btn) {
            let ribbon = document.querySelector('.flex-linear-calendar, .date-ribbon, [class*="linearCalendar"], [class*="dateRibbon"]');
            if (ribbon) {
                let btns = Array.from(ribbon.querySelectorAll('button, a')).filter(b => b.offsetParent !== null);
                if (btns.length) btn = btns[btns.length - 1];
            }
        }
        if (btn) {
            btn.scrollIntoView({block: 'center'});
            btn.click();
            return true;
        }
        return false;
    """)
    if clicked:
        time.sleep(NEXT_SLEEP)
    return bool(clicked)

# ══════════════════════════════════════════════════════════════════
#  SEARCH
# ══════════════════════════════════════════════════════════════════

def do_search(driver, wait, origin, dest, start_date, attempt=1):
    tag = f"[{origin}→{dest}]"
    try:
        if attempt > 1:
            driver.get("https://www.google.com")
            time.sleep(5)
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
    except Exception as _e:
        tprint(f'[WARN] {type(_e).__name__}: {_e}')

    tprint(f"    🔍 {tag} Search attempt {attempt}: {origin}→{dest} from {start_date}")
    try:
        driver.get("https://www.qantas.com/en-au")
        tprint(f"    ⏳ {tag} Waiting for search form...")

        form_ready = False
        for _ in range(20):
            time.sleep(3)
            found = driver.execute_script("""
                let checks = [
                    document.getElementById('trip-type-toggle-button'),
                    document.getElementById('departurePort-input'),
                    document.querySelector('[data-testid="trip-type-toggle"]'),
                    document.querySelector('input[id*="departure"]'),
                    document.querySelector('input[id*="Departure"]'),
                    document.querySelector('[class*="tripType"] button'),
                    document.querySelector('[class*="TripType"] button'),
                    document.querySelector('button[id*="trip"]'),
                    Array.from(document.querySelectorAll('input[type="text"], input:not([type])')).find(i => {
                        let r = i.getBoundingClientRect();
                        return r.width > 100 && r.height > 0 && i.offsetParent !== null;
                    })
                ];
                let hit = checks.find(c => c != null);
                if (hit) return hit.id || hit.className || 'form-found';
                return null;
            """)
            if found:
                tprint(f"    ✅ {tag} Search form ready ({found})")
                form_ready = True
                break
            tprint(f"    ⏳ {tag} Form not yet visible...")

        if not form_ready:
            tprint(f"    ⚠️  {tag} Form never appeared — proceeding anyway")

        tprint(f"    📄 {tag} Page title: {driver.title}")

        # Dismiss overlays
        dismissed = driver.execute_script("""
            let closed = [];
            document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
            document.dispatchEvent(new KeyboardEvent('keyup',   {key:'Escape', keyCode:27, bubbles:true}));
            let closeSelectors = [
                'button[aria-label*="Close"]', 'button[aria-label*="close"]',
                '[class*="closeButton"]', '[class*="close-button"]', '[class*="CloseBtn"]',
                'button[aria-label*="Go back"]', 'button[aria-label*="Back"]',
            ];
            for (let sel of closeSelectors) {
                let btns = Array.from(document.querySelectorAll(sel)).filter(b => b.offsetParent !== null);
                btns.forEach(b => { b.click(); closed.push(sel); });
            }
            let menuClose = Array.from(document.querySelectorAll('button')).find(b => {
                let txt = (b.innerText || b.getAttribute('aria-label') || '').toLowerCase();
                return (txt.includes('close menu') || txt.includes('close nav')) && b.offsetParent !== null;
            });
            if (menuClose) { menuClose.click(); closed.push('close-menu-btn'); }
            document.body.click();
            return closed;
        """)
        if dismissed:
            time.sleep(2)

        # One Way Toggle
        toggle = None
        toggle_selectors = [
            (By.ID, "trip-type-toggle-button"),
            (By.CSS_SELECTOR, "[data-testid='trip-type-toggle']"),
            (By.CSS_SELECTOR, "button[aria-label*='One way'], button[aria-label*='one way']"),
            (By.XPATH, "//button[contains(translate(text(),'OW','ow'),'one way') or contains(translate(text(),'RR','rr'),'return')]"),
            (By.CSS_SELECTOR, "[class*='tripType'] button, [class*='trip-type'] button"),
            (By.XPATH, "//button[contains(.,'Return') or contains(.,'return')]"),
            (By.XPATH, "//button[contains(.,'One way') or contains(.,'one way')]"),
        ]
        for by, sel in toggle_selectors:
            try:
                toggle = WebDriverWait(driver, 12).until(EC.element_to_be_clickable((by, sel)))
                tprint(f"    ✅ {tag} Toggle found: {toggle.text[:40]}")
                break
            except Exception:
                pass

        if toggle is None:
            driver.execute_script("""
                document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
                document.body.click();
                let main = document.querySelector('main, article, [role="main"], #main-content');
                if (main) main.click();
            """)
            time.sleep(10)
            for by, sel in toggle_selectors:
                try:
                    toggle = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((by, sel)))
                    tprint(f"    ✅ {tag} Toggle found (delayed): {toggle.text[:40]}")
                    break
                except Exception:
                    pass
            if toggle is None:
                raise Exception("Page form did not load — toggle not found")
        elif "One way" not in toggle.text and "one way" not in toggle.text.lower():
            driver.execute_script("arguments[0].click();", toggle)
            for ow_xpath in ["//li[contains(.,'One way')]", "//button[contains(.,'One way')]", "//*[contains(@class,'one-way')]"]:
                try:
                    ow = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, ow_xpath)))
                    driver.execute_script("arguments[0].click();", ow)
                    tprint(f"    ✅ {tag} One way selected")
                    break
                except Exception:
                    pass

        # Airport inputs
        port_pairs = [
            (origin, "departurePort-input"),
            (dest,   "arrivalPort-input"),
        ]
        for port, input_id in port_pairs:
            airport_name = AIRPORT_NAMES.get(port, port)
            port_lower   = port.lower()
            name_lower   = airport_name.lower()
            is_origin    = (input_id == "departurePort-input")

            driver.execute_script("""
                document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
                document.body.click();
            """)
            time.sleep(1)

            f_in = None
            inp_selectors = [
                (By.ID, input_id),
                (By.XPATH, f"//input[@id='{input_id}' and not(@type='hidden')]"),
                (By.CSS_SELECTOR, f"input[id*='{'departure' if is_origin else 'arrival'}']"),
                (By.CSS_SELECTOR, f"input[name*='{'origin' if is_origin else 'destination'}'], input[name*='{'departure' if is_origin else 'arrival'}']"),
                (By.XPATH, f"//input[contains(@placeholder,'{'rom' if is_origin else 'o'}')]"),
                (By.XPATH, "//input[contains(@placeholder,'airport') or contains(@placeholder,'Airport')]"),
            ]
            for inp_sel in inp_selectors:
                try:
                    candidate = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(inp_sel))
                    visible = driver.execute_script(
                        "let r = arguments[0].getBoundingClientRect();"
                        "return r.width > 0 && r.height > 0 && r.top >= 0 && r.top < window.innerHeight;",
                        candidate
                    )
                    if visible:
                        f_in = candidate
                        break
                except Exception:
                    pass

            if f_in is None:
                found_via_js = driver.execute_script(f"""
                    let inputs = Array.from(document.querySelectorAll('input[type="text"], input:not([type])'))
                        .filter(i => {{
                            let r = i.getBoundingClientRect();
                            return r.width > 0 && r.height > 0 && r.top >= 0 && r.top < window.innerHeight
                                && !i.readOnly && i.offsetParent !== null;
                        }});
                    let idx = {'0' if is_origin else '1'};
                    let target = inputs[idx] || inputs[0];
                    if (target) {{
                        target.scrollIntoView({{block:'center'}});
                        target.click();
                        target.focus();
                        return target.id || target.name || 'found-via-js-idx-' + idx;
                    }}
                    return null;
                """)
                if found_via_js:
                    time.sleep(1)
                    try:
                        f_in = driver.switch_to.active_element
                    except Exception:
                        pass

            if f_in is None:
                raise Exception(f"Airport input not found for {port}")

            driver.execute_script("arguments[0].value = '';", f_in)
            f_in.click()
            time.sleep(1)
            human_type(f_in, airport_name)
            random_delay(5, 8)

            matched = False
            for _ in range(15):
                good = driver.execute_script(f"""
                    let opts = Array.from(document.querySelectorAll('[id^="departurePort-item"], [id^="arrivalPort-item"]'));
                    if (!opts.length) opts = Array.from(document.querySelectorAll('[role="listbox"] [role="option"], [class*="menuItem"], [class*="menu-item"]'));
                    let hit = opts.find(o => {{
                        let t = (o.innerText || o.textContent || '').toLowerCase();
                        return t.includes('{port_lower}') || t.includes('{name_lower}');
                    }});
                    if (hit) {{ hit.click(); return (hit.innerText || hit.textContent || '').trim().slice(0,80); }}
                    return null;
                """)
                if good:
                    tprint(f"    ✅ {tag} Airport selected ({port}): {good}")
                    matched = True
                    break
                time.sleep(1)

            if not matched:
                tprint(f"    ⚠️  {tag} Retrying type for {port}...")
                try:
                    driver.execute_script("arguments[0].value = '';", f_in)
                    f_in.click()
                    time.sleep(1)
                    human_type(f_in, airport_name)
                    time.sleep(5)
                    good = driver.execute_script(f"""
                        let opts = Array.from(document.querySelectorAll('[id^="departurePort-item"], [id^="arrivalPort-item"]'));
                        if (!opts.length) opts = Array.from(document.querySelectorAll('[role="listbox"] [role="option"], [class*="menuItem"], [class*="menu-item"]'));
                        let hit = opts.find(o => {{
                            let t = (o.innerText || o.textContent || '').toLowerCase();
                            return t.includes('{port_lower}') || t.includes('{name_lower}');
                        }});
                        if (hit) {{ hit.click(); return (hit.innerText || hit.textContent || '').trim().slice(0,80); }}
                        return null;
                    """)
                    if good:
                        tprint(f"    ✅ {tag} Airport selected on retype ({port}): {good}")
                        matched = True
                except Exception:
                    pass

            if not matched:
                tprint(f"    ⚠️  {tag} Could not match suggestion for {port} — pressing Enter")
                f_in.send_keys(Keys.RETURN)
            time.sleep(2)

        # Date picker
        d_btn = None
        date_selectors = [
            (By.ID, "daypicker-button"),
            (By.CSS_SELECTOR, "[data-testid='daypicker-button']"),
            (By.CSS_SELECTOR, "button[aria-label*='date'], button[aria-label*='Date']"),
            (By.CSS_SELECTOR, "[class*='datepicker'] button, [class*='date-picker'] button"),
            (By.XPATH, "//button[contains(@class,'date') or contains(@id,'date')]"),
        ]
        for by, sel in date_selectors:
            try:
                d_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((by, sel)))
                break
            except Exception:
                pass
        if d_btn is None:
            raise Exception("Could not find date picker button")
        driver.execute_script("arguments[0].click();", d_btn)
        random_delay()

        target_day = str(start_date.day)
        target_iso = start_date.strftime("%Y-%m-%d")

        date_clicked = driver.execute_script(f"""
            let tds = Array.from(document.querySelectorAll('td, [role="gridcell"], [class*="day"]'));
            let hit = tds.find(el => {{
                let txt = (el.innerText || el.textContent || '').trim();
                return txt === '{target_day}' && el.offsetParent !== null;
            }});
            if (hit) {{ hit.click(); return 'clicked td day: {target_day}'; }}
            let formats = [
                "{start_date.strftime('%A, %d %B %Y')}",
                "{start_date.strftime('%d %B %Y')}",
                "{target_iso}",
            ];
            for (let fmt of formats) {{
                let el = document.querySelector('[aria-label="' + fmt + '"]');
                if (el) {{ el.click(); return 'aria-label: ' + fmt; }}
            }}
            return 'no click needed - date pre-selected';
        """)
        tprint(f"    📅 {tag} Date step: {date_clicked}")
        time.sleep(2)

        cont_clicked = driver.execute_script("""
            let btns = Array.from(document.querySelectorAll('button'));
            let cont = btns.find(b => (b.innerText||'').trim().toLowerCase() === 'continue');
            if (cont) { cont.click(); return true; }
            return false;
        """)
        if cont_clicked:
            time.sleep(3)

        # Search button
        search_selectors = [
            "button[type='submit']",
            "[data-testid='search-flights-btn'] button",
            "[data-testid='search-flights-btn']",
            "button[aria-label*='Search']",
            "button[aria-label*='search']",
            "[class*='searchButton'] button",
            "[class*='search-button']",
            "[class*='SearchButton']",
            "form button[type='submit']",
        ]
        sb = None
        for sel in search_selectors:
            try:
                sb = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                tprint(f"    ✅ {tag} Search button found: {sel}")
                break
            except Exception:
                pass

        if sb is None:
            sb_found = driver.execute_script("""
                let btns = Array.from(document.querySelectorAll('button'));
                let hit = btns.find(b => {
                    let txt = (b.innerText || '').trim().toLowerCase();
                    return txt === 'search' || txt === 'search flights' || txt === 'find flights';
                });
                if (hit) { hit.scrollIntoView({block:'center'}); hit.click(); return true; }
                return false;
            """)
            if sb_found:
                tprint(f"    ✅ {tag} Search button clicked via JS")
                time.sleep(5)
            else:
                raise Exception("Could not find search/submit button")
        else:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sb)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", sb)

        # Wait for results
        result_selectors = [
            ".cal-tab-body", ".flight-card", "[class*='flightCard']",
            "[class*='flight-row']", "grouped-avail-flight-row",
            ".flex-linear-calendar", "[class*='availResults']",
            "[class*='results-container']", "[data-testid*='flight']",
        ]
        results_found = False
        deadline = time.time() + 90
        tprint(f"    ⏳ {tag} Waiting for results...", end="", flush=True)

        while time.time() < deadline:
            cur_url = driver.current_url
            if cur_url != "https://www.qantas.com/en-au" and (
                "booking" in cur_url or "select" in cur_url or
                "results" in cur_url or "flights" in cur_url.lower() or
                "en-au/flight" in cur_url or "tripflow" in cur_url
            ):
                tprint(f" ✅ {tag} URL changed: {cur_url}")
                for _ in range(15):
                    time.sleep(3)
                    new_url = driver.current_url
                    if new_url == cur_url:
                        break
                    cur_url = new_url

                page_title    = driver.title.lower()
                is_redirect   = "tripflow" in cur_url or "redirect" in cur_url

                if not is_redirect:
                    if "access denied" in page_title or "403" in page_title or "blocked" in page_title:
                        body_text = driver.execute_script(
                            "return (document.body && document.body.innerText || '').toLowerCase().slice(0, 300);"
                        )
                        if "access denied" in body_text or "403 forbidden" in body_text:
                            raise Exception(f"Access Denied at {cur_url}")

                if is_redirect:
                    confirmed_block = 0
                    for i in range(30):
                        time.sleep(3)
                        cur_url    = driver.current_url
                        page_title = driver.title.lower()
                        if "tripflow" not in cur_url and "redirect" not in cur_url:
                            break
                        if "access denied" in page_title or "403" in page_title:
                            body_text = driver.execute_script(
                                "return (document.body && document.body.innerText || '').toLowerCase().slice(0, 500);"
                            )
                            if "access denied" in body_text or "403 forbidden" in body_text or "you have been blocked" in body_text:
                                confirmed_block += 1
                                if confirmed_block >= 3:
                                    raise Exception(f"Access Denied at {cur_url}")
                            else:
                                confirmed_block = 0
                        else:
                            confirmed_block = 0

                results_found = True
                time.sleep(3)
                break

            found_sel = driver.execute_script("""
                let sels = arguments[0];
                for (let s of sels) {
                    try {
                        let el = document.querySelector(s);
                        if (el && el.offsetParent !== null) return s;
                    } catch(e) {}
                }
                return null;
            """, result_selectors)

            if found_sel:
                tprint(f" ✅ {tag} DOM element: {found_sel}")
                results_found = True
                break

            elapsed = int(time.time() - (deadline - 90))
            tprint(f" {elapsed}s..", end="", flush=True)
            time.sleep(3)

        if not results_found:
            tprint()
            raise Exception(f"Results page not loaded after 90s. URL: {driver.current_url}")

        tprint(f"    🔗 {tag} Results URL: {driver.current_url}")
        return True

    except Exception as e:
        err_str = str(e)
        tprint(f"    ⚠️  {tag} Search failed: {err_str}")
        if "internal server error" in err_str.lower() or "500" in err_str:
            return "bd_error"
        return False


def do_search_with_retry(driver, wait, origin, dest, target_date, route_key):
    current_driver = driver
    current_wait   = wait

    for attempt in range(1, MAX_SEARCH_RETRIES + 1):
        result = do_search(current_driver, current_wait, origin, dest, target_date, attempt=attempt)
        if result is True:
            return True, current_driver, current_wait

        if attempt < MAX_SEARCH_RETRIES:
            if result == "bd_error":
                wait_secs = 15 * attempt
                tprint(f"    🚫 [{origin}→{dest}] BD error — waiting {wait_secs}s...")
                try:
                    safe_quit(current_driver)
                except Exception:
                    pass
                time.sleep(wait_secs)
                current_driver = make_driver(route_key)
                current_wait   = WebDriverWait(current_driver, 60)
                continue

            needs_new_driver = False
            try:
                title     = current_driver.title.lower()
                body_text = current_driver.execute_script(
                    "return (document.body && document.body.innerText || '').toLowerCase().slice(0, 500);"
                )
                if "access denied" in title or "denied" in title or "403" in title or \
                   "access denied" in body_text or "403 forbidden" in body_text:
                    needs_new_driver = True
            except Exception:
                needs_new_driver = True

            if needs_new_driver:
                tprint(f"    🚫 [{origin}→{dest}] Spawning fresh session for retry {attempt + 1}...")
                try:
                    safe_quit(current_driver)
                except Exception:
                    pass
                current_driver = make_driver(route_key)
                current_wait   = WebDriverWait(current_driver, 60)
            else:
                time.sleep(8)

    tprint(f"    ❌ [{origin}→{dest}] All {MAX_SEARCH_RETRIES} attempts failed for {target_date}")
    return False, current_driver, current_wait

# ══════════════════════════════════════════════════════════════════
#  OUTPUT — CLIENT-READY FILENAME
# ══════════════════════════════════════════════════════════════════

def make_filename(origin, dest, run_date):
    """Returns: Qantas_BME-KNX_20260512"""
    route_str = f"{origin}-{dest}"
    date_str  = run_date.strftime("%Y%m%d")
    return f"Qantas_{route_str}_{date_str}"


def save_route(rows, origin, dest, run_date):
    """Save a single route's data — filename: Qantas_BME-KNX_20260512.xlsx"""
    if not rows:
        return
    df   = pd.DataFrame(rows)
    base = make_filename(origin, dest, run_date)
    xlsx = OUTPUT_DIR / f"{base}.xlsx"
    csv  = OUTPUT_DIR / f"{base}.csv"

    cols = ["Date Checked", "Time Checked", "Airline", "Date of Departure",
            "Time of Departure", "Origin", "Destination", "Fare Price", "Fare Class", "Source"]

    df[cols].to_csv(csv, index=False)

    with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
        df[cols].to_excel(w, index=False, sheet_name="Fare Tracker")
        ok = df[df["Fare Price"].notna()].copy()
        if not ok.empty:
            ok["Route"] = ok["Origin"] + "→" + ok["Destination"]
            ok.pivot_table(
                index="Date of Departure", columns="Route",
                values="Fare Price", aggfunc="min"
            ).round(2).to_excel(w, sheet_name="Cheapest By Route")

    tprint(f"  💾 [{origin}→{dest}] Saved → {xlsx.name}")

# ══════════════════════════════════════════════════════════════════
#  RECORD ROW
# ══════════════════════════════════════════════════════════════════

def record_row(all_rows, origin, dest, date_str, departure_time, fare_price, fare_class):
    all_rows.append({
        "Date Checked":      datetime.now().strftime("%d/%m/%Y"),
        "Time Checked":      datetime.now().strftime("%H:%M"),
        "Airline":           AIRLINE,
        "Date of Departure": date_str,
        "Time of Departure": departure_time,
        "Origin":            origin,
        "Destination":       dest,
        "Fare Price":        fare_price,
        "Fare Class":        fare_class,
        "Source":            SOURCE,
    })

# ══════════════════════════════════════════════════════════════════
#  SCRAPE ONE ROUTE
# ══════════════════════════════════════════════════════════════════

def scrape_route(origin, dest, today):
    """
    Scrape a single route on its own zone.
    Returns list of row dicts.
    """
    route_key  = (origin, dest)
    is_special = (origin == "BME" and dest == "DRW") or (origin == "DRW" and dest == "KNX")
    limit      = DAYS_OUT
    all_rows   = []

    tprint(f"\n{'─'*60}")
    tprint(f"  📍 {origin}→{dest} | Zone: {ROUTE_CREDENTIALS[route_key]['zone']}")

    driver = make_driver(route_key)
    wait   = WebDriverWait(driver, 60)

    try:
        expected_date  = today
        ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date, route_key)

        if not ok:
            tprint(f"  💥 [{origin}→{dest}] Could not load. Filling {limit} dates as NO DATA.")
            for i in range(limit):
                d = today + timedelta(days=i)
                record_row(all_rows, origin, dest, str(d), "", None, "NO DATA")
            save_route(all_rows, origin, dest, today)
            return all_rows

        collected     = 0
        seen_dates    = set()
        no_new_streak = 0

        while collected < limit:

            if not driver_heartbeat(driver):
                tprint(f"    💀 [{origin}→{dest}] Heartbeat failed — recreating session...")
                try:
                    safe_quit(driver)
                except Exception:
                    pass
                random_delay(5, 10)
                driver = make_driver(route_key)
                wait   = WebDriverWait(driver, 60)
                ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date, route_key)
                if not ok:
                    raise Exception("Heartbeat recovery failed")

            tabs     = extract_ribbon_tabs(driver, today)
            tabs.sort(key=lambda t: t["date_obj"])
            new_tabs = [t for t in tabs if t["date_obj"] not in seen_dates and t["date_obj"] >= expected_date]

            if not new_tabs:
                no_new_streak += 1
                tprint(f"    ⚠️  [{origin}→{dest}] No new tabs (streak {no_new_streak}/{NO_NEW_STREAK_LIMIT})")

                if no_new_streak >= NO_NEW_STREAK_LIMIT:
                    tprint(f"    🔄 [{origin}→{dest}] Re-searching at {expected_date}...")
                    ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date, route_key)
                    if not ok:
                        while collected < limit:
                            record_row(all_rows, origin, dest, str(expected_date), "", None, "NO DATA")
                            seen_dates.add(expected_date)
                            collected     += 1
                            expected_date += timedelta(days=1)
                        break
                    no_new_streak = 0
                else:
                    arrow_clicked = False
                    for _ in range(NEXT_ARROW_RETRIES):
                        if click_next_arrow(driver):
                            arrow_clicked = True
                            break
                        time.sleep(2)
                    if not arrow_clicked:
                        ok, driver, wait = do_search_with_retry(driver, wait, origin, dest, expected_date, route_key)
                        if not ok:
                            while collected < limit:
                                record_row(all_rows, origin, dest, str(expected_date), "", None, "NO DATA")
                                seen_dates.add(expected_date)
                                collected     += 1
                                expected_date += timedelta(days=1)
                            break
                        no_new_streak = 0
                continue

            no_new_streak = 0

            for tab in new_tabs:
                if collected >= limit:
                    break

                date_obj = tab["date_obj"]
                date_str = tab["date_str"]

                # Gap fill
                while expected_date < date_obj and collected < limit:
                    gap_str = str(expected_date)
                    fc      = "No Direct Flight" if is_special else "NO FLIGHTS"
                    record_row(all_rows, origin, dest, gap_str, "", None, fc)
                    seen_dates.add(expected_date)
                    collected     += 1
                    expected_date += timedelta(days=1)
                    tprint(f"    [{collected}/{limit}] {gap_str}  ⬛ Gap-filled ({fc})")

                if collected >= limit:
                    break

                tprint(f"    [{collected+1}/{limit}] {date_str}", end="  ")
                click_tab(driver, tab["tab_index"])

                if tab["no_flight"]:
                    fc = "No Direct Flight" if is_special else "NO FLIGHTS"
                    record_row(all_rows, origin, dest, date_str, "", None, fc)
                    tprint(f"🛑 No flights")
                else:
                    cards = (scrape_flight_cards_shadow(driver, origin, dest)
                             if is_special else
                             scrape_flight_cards_standard(driver))

                    if cards:
                        for c in cards:
                            record_row(all_rows, origin, dest, date_str,
                                       c["departure_time"], c["fare_price"], c["fare_class"])
                        tprint(f"✅ {len(cards)} fares")
                    else:
                        fc = "No Direct Flight" if is_special else "SOLD OUT"
                        record_row(all_rows, origin, dest, date_str, "", None, fc)
                        tprint(f"🛑 {'No Direct Flight' if is_special else 'No flights found'}")

                seen_dates.add(date_obj)
                collected     += 1
                expected_date  = date_obj + timedelta(days=1)

                # Periodic save every 7 dates
                if collected % 7 == 0:
                    save_route(all_rows, origin, dest, today)

            if collected < limit:
                click_next_arrow(driver)

        tprint(f"\n  ✅ [{origin}→{dest}] COMPLETE: {collected}/{limit} dates")
        save_route(all_rows, origin, dest, today)

    except Exception as route_err:
        tprint(f"\n  💥 [{origin}→{dest}] Crashed: {route_err}")
        traceback.print_exc()
        capture_debug(driver, f"crash_{origin}_{dest}")
        existing = {r["Date of Departure"] for r in all_rows}
        for i in range(DAYS_OUT):
            d = today + timedelta(days=i)
            if str(d) not in existing:
                record_row(all_rows, origin, dest, str(d), "", None, "NO DATA")
        try:
            save_route(all_rows, origin, dest, today)
        except Exception:
            pass

    finally:
        try:
            safe_quit(driver)
        except Exception:
            pass

    return all_rows

# ══════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(description="Qantas Fare Tracker v12")
    parser.add_argument(
        "--workers", type=int, default=1, choices=[1, 2, 3, 4],
        help="Number of parallel routes to run (1-4). Default: 1"
    )
    parser.add_argument(
        "--route", type=int, default=None, choices=[1, 2, 3, 4],
        help="Run only a specific route: 1=BME→KNX, 2=BME→DRW, 3=DRW→KNX, 4=KNX→BME"
    )
    return parser.parse_args()


def interactive_menu():
    """Show menu and return selected routes list."""
    print(f"\n{'═'*60}")
    print(f"  🛫 Qantas Fare Tracker v12")
    print(f"  Select route(s):\n")
    for i, (o, d) in enumerate(ROUTES, 1):
        zone = ROUTE_CREDENTIALS[(o, d)]["zone"]
        print(f"    {i}. {o} → {d}  (zone: {zone})")
    print(f"    {len(ROUTES) + 1}. All routes\n")

    while True:
        try:
            choice = int(input(f"  Enter choice (1-{len(ROUTES)+1}): ").strip())
            if 1 <= choice <= len(ROUTES):
                return [ROUTES[choice - 1]]
            elif choice == len(ROUTES) + 1:
                return list(ROUTES)
        except Exception:
            pass

    workers_input = input("  Workers (parallel routes) [1]: ").strip()
    try:
        return int(workers_input) if workers_input else 1
    except Exception:
        return 1


def main():
    args  = parse_args()
    today = date.today()

    # Determine which routes to run
    if args.route is not None:
        routes = [ROUTES[args.route - 1]]
    elif len(sys.argv) == 1:
        # No args — show interactive menu
        routes = interactive_menu()
        args.workers = 1 if len(routes) == 1 else min(len(routes), 4)
    else:
        routes = list(ROUTES)

    workers = min(args.workers, len(routes))

    print(f"\n{'═'*60}")
    print(f"  🛫 Qantas Fare Tracker v12 — {today}")
    print(f"  Routes   : {len(routes)}")
    print(f"  Workers  : {workers} (parallel)")
    print(f"  Output   : Qantas_ROUTE_{today.strftime('%Y%m%d')}.xlsx")
    print(f"{'═'*60}\n")

    if workers == 1:
        # Sequential — simple, safe
        for origin, dest in routes:
            scrape_route(origin, dest, today)
            if len(routes) > 1:
                tprint(f"\n  ⏳ Waiting 20s before next route...")
                time.sleep(20)
    else:
        # Parallel — each route on its own thread & zone
        tprint(f"  🚀 Starting {workers} parallel workers...\n")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(scrape_route, origin, dest, today): (origin, dest)
                for origin, dest in routes
            }
            for future in as_completed(futures):
                origin, dest = futures[future]
                try:
                    rows = future.result()
                    tprint(f"\n  ✅ [{origin}→{dest}] Thread done — {len(rows)} rows")
                except Exception as exc:
                    tprint(f"\n  💥 [{origin}→{dest}] Thread failed: {exc}")

    print(f"\n{'═'*60}")
    print(f"  🏁 All done! Check output/ folder for files.")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
