import asyncio
import json
import random
import re
import sys
import argparse
import traceback
from datetime import datetime, timedelta, date
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
import pandas as pd
from pathlib import Path

# ══════════════════════════════════════════════════════════════════
#  CONFIGURATION & BRIGHT DATA CREDENTIALS
# ══════════════════════════════════════════════════════════════════
ROUTES = [
    ("BME", "KNX"),
    ("BME", "DRW"),
    ("DRW", "KNX"),
    ("KNX", "BME"),
    ("PER", "GET"),
    ("GET", "PER"),
    ("DRW", "BME"),
    ("KNX", "DRW"),
]
DAYS_OUT = 84
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

CUSTOMER_ID = "hl_fbc4a16a"
ROUTE_CREDENTIALS = {
    ("BME", "KNX"): {"zone": "scraping_browser2",  "password": "nymmsv0ffs60"},
    ("BME", "DRW"): {"zone": "qantas_1",            "password": "x9ck9dpthpsg"},
    ("DRW", "KNX"): {"zone": "qantas_2",            "password": "kgu154ajo3d9"},
    ("KNX", "BME"): {"zone": "qantas_3",            "password": "n748kj03bomt"},
    ("PER", "GET"): {"zone": "qantas_browser_4",    "password": "q7b458ikgj87"},
    ("GET", "PER"): {"zone": "qantas_browser_5",    "password": "64u5qm13pevg"},
    ("DRW", "BME"): {"zone": "qantas_browser_6",    "password": "aah84ml95h00"},
    ("KNX", "DRW"): {"zone": "qantas_browser_7",    "password": "kyp0m7odmdw9"},
}

AIRPORT_NAMES = {"BME": "Broome", "KNX": "Kununurra", "DRW": "Darwin", "PER": "Perth", "GET": "Geraldton"}
AIRLINE = "Qantas"
SOURCE = "qantas.com"

# ══════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════

def make_filename(origin, dest, run_date):
    """Returns standard client-ready filename."""
    route_str = f"{origin}-{dest}"
    date_str = run_date.strftime("%Y%m%d")
    return f"Qantas_{route_str}_{date_str}"

def save_route(rows, origin, dest, run_date):
    """Save scraped data to client-ready Excel and CSV files without crashing."""
    if not rows:
        return
    try:
        df = pd.DataFrame(rows)
        base = make_filename(origin, dest, run_date)
        xlsx = OUTPUT_DIR / f"{base}.xlsx"
        csv = OUTPUT_DIR / f"{base}.csv"

        cols = ["Date Checked", "Time Checked", "Airline", "Date of Departure",
                "Time of Departure", "Origin", "Destination", "Fare Price", "Fare Class", "Source"]

        # Ensure all columns exist (guard against partial rows)
        for col in cols:
            if col not in df.columns:
                df[col] = None

        try:
            df[cols].to_csv(csv, index=False)
        except Exception as csv_err:
            print(f"  [WARN] [{origin}->{dest}] CSV save failed: {csv_err}")

        try:
            with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
                df[cols].to_excel(w, index=False, sheet_name="Fare Tracker")
                ok = df[df["Fare Price"].notna()].copy()
                if not ok.empty:
                    ok["Route"] = ok["Origin"] + "->" + ok["Destination"]
                    ok.pivot_table(
                        index="Date of Departure", columns="Route",
                        values="Fare Price", aggfunc="min"
                    ).round(2).to_excel(w, sheet_name="Cheapest By Route")
            print(f"  [SAVE] [{origin}->{dest}] Saved -> {xlsx.name}")
        except Exception as xlsx_err:
            print(f"  [WARN] [{origin}->{dest}] XLSX save failed: {xlsx_err} - CSV only")

    except Exception as save_err:
        print(f"  [ERR] [{origin}->{dest}] save_route completely failed: {save_err}")

def record_row(all_rows, origin, dest, date_str, departure_time, fare_price, fare_class):
    """Append a scraped fare row to the results collection."""
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

def parse_date_text(text, ref_year, today):
    """Parse '2 May', 'Sat 2 May', 'Wed 6 May' -> date."""
    try:
        clean = re.sub(r'^[A-Za-z]{3,}\s+', '', text.strip())
        clean = re.sub(r'^[^0-9]+', '', clean).strip()
        m = re.match(r'(\d+\s+[A-Za-z]+)', clean)
        if m:
            clean = m.group(1)
        dt = datetime.strptime(f"{clean} {ref_year}", "%d %b %Y")
        result = dt.date()
        if result < today - timedelta(days=30):
            dt = datetime.strptime(f"{clean} {ref_year + 1}", "%d %b %Y")
            result = dt.date()
        return result
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════
#  NAVIGATION & FORM FILLING HELPERS
# ══════════════════════════════════════════════════════════════════

async def handle_cookies(page):
    """Dismiss cookie modal if present."""
    try:
        accept_button = page.get_by_role("button", name="Accept All")
        if await accept_button.is_visible(timeout=5000):
            await accept_button.click()
            print("Accepted cookies.")
    except:
        pass

async def select_airport(page, input_id, iata_code, field_idx):
    """Select an airport from Qantas autocomplete with multi-strategy lookup and retries."""
    tag = f"[{iata_code}]"
    airport_name = AIRPORT_NAMES.get(iata_code, iata_code)
    print(f"    Searching {tag} airport...")
    
    # Close any open dropdowns before each field
    await page.keyboard.press("Escape")
    await asyncio.sleep(1)
    
    input_element = None
    
    # Strategy 1: Find input by ID
    try:
        el = page.locator(f"#{input_id}")
        if await el.is_visible(timeout=5000):
            input_element = el
            print(f"    [OK] Input found by ID: {input_id}")
    except Exception:
        pass
        
    # Strategy 2: Find input by various generic CSS selectors
    if not input_element:
        is_origin = (field_idx == 0)
        dep_or_arr   = "departure" if is_origin else "arrival"
        orig_or_dest = "origin"    if is_origin else "destination"
        from_or_to   = "From"      if is_origin else "To"
        from_or_to_l = "from"      if is_origin else "to"
        css_list = [
            f"input[id*='{dep_or_arr}']",
            f"input[name*='{orig_or_dest}']",
            f"input[name*='{dep_or_arr}']",
            f"input[placeholder*='{from_or_to}']",
            f"input[placeholder*='{from_or_to_l}']",
            "input[placeholder*='airport']",
            "input[placeholder*='Airport']",
        ]
        for css in css_list:
            try:
                el = page.locator(css).first
                if await el.is_visible(timeout=2000):
                    input_element = el
                    print(f"    [OK] Input found by CSS: {css}")
                    break
            except Exception:
                pass
                
    # Strategy 3: Find input via JS nth visible input element
    if not input_element:
        try:
            target_id = await page.evaluate(f"""() => {{
                let inputs = Array.from(document.querySelectorAll(
                    'input[type="text"], input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"]):not([type="submit"])'
                )).filter(i => {{
                    let r = i.getBoundingClientRect();
                    return r.width > 50 && r.height > 0 && r.top >= 0
                        && r.top < window.innerHeight && !i.readOnly
                        && i.offsetParent !== null;
                }});
                let target = inputs[{field_idx}] || inputs[0];
                if (target) {{
                    target.scrollIntoView({{block:'center'}});
                    return target.id || target.name || 'js-input';
                }}
                return null;
            }}""")
            if target_id:
                if target_id.startswith('js-input'):
                    await page.evaluate(f"""() => {{
                        let inputs = Array.from(document.querySelectorAll(
                            'input[type="text"], input:not([type="hidden"]):not([type="checkbox"]):not([type="radio"]):not([type="submit"])'
                        )).filter(i => i.offsetParent !== null);
                        if (inputs[{field_idx}]) inputs[{field_idx}].focus();
                    }}""")
                else:
                    input_element = page.locator(f"#{target_id}").first
                print(f"    [OK] Input focused/found via JS: {target_id}")
        except Exception as e:
            print(f"    JS fallback error: {e}")
            
    # Focus and click the field
    if input_element:
        await input_element.click()
    else:
        # Strategy 4: Click the label and focus active element
        label_text = "From" if field_idx == 0 else "To"
        await page.evaluate(f"""() => {{
            let labels = Array.from(document.querySelectorAll('label, [class*="label"], [class*="Label"]'));
            let lbl = labels.find(l => (l.innerText||'').trim().toLowerCase().startsWith('{label_text.lower()}'));
            if (lbl) lbl.click();
        }}""")
        await asyncio.sleep(1)
        
    # Autocomplete autocomplete
    terms = [airport_name, iata_code]
    matched = False
    
    for term in terms:
        print(f"      Trying autocomplete with term: '{term}'")
        try:
            # Clear field
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)
            
            # Type sequentially to emulate real human keyboard events
            await page.keyboard.type(term, delay=150)
            await asyncio.sleep(5)  # Wait for autocomplete dropdown suggestions
            
            # Find and click the suggestion
            good = await page.evaluate(f"""() => {{
                let opts = Array.from(document.querySelectorAll(
                    '[id^="departurePort-item"], [id^="arrivalPort-item"],'
                    + '[role="listbox"] [role="option"], [role="option"],'
                    + '[class*="menuItem"], [class*="menu-item"],'
                    + '[class*="suggestion"], [class*="autocomplete"] li'
                ));
                let hit = opts.find(o => {{
                    let t = (o.innerText || o.textContent || '').toLowerCase();
                    return t.includes('{iata_code.lower()}') || t.includes('{airport_name.lower()}');
                }});
                if (hit) {{
                    hit.scrollIntoView({{block:'center'}});
                    hit.click();
                    return (hit.innerText || hit.textContent || '').trim().slice(0,80);
                }}
                return null;
            }}""")
            
            if good:
                print(f"    [OK] Airport selected: {good.splitlines()[0]}")
                matched = True
                break
        except Exception as e:
            print(f"    Autocomplete option matching error: {e}")
            
    if not matched:
        print(f"    [WARN] No suggestion match found. Pressing Enter as fallback...")
        await page.keyboard.press("Enter")
        await asyncio.sleep(1.5)
        await page.keyboard.press("Enter")
        await asyncio.sleep(1)

async def dismiss_disclaimer(page):
    """Check for commercial pricing disclaimer modal and close it if present using multiple strategies."""
    try:
        # Strategy 1: Standard Playwright selectors for the Close button
        for selector in [
            "button:has-text('Close')",
            "div.react-responsive-modal-modal button:has-text('Close')",
            "div[class*='Disclaimer'] button:has-text('Close')",
            "div[role='alertdialog'] button:has-text('Close')"
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=500):
                    await btn.click(timeout=2000)
                    print(f"    [DISCLAIMER] Closed pricing disclaimer using selector: {selector}")
                    await asyncio.sleep(1)
                    return True
            except Exception:
                pass

        # Strategy 2: Client-side JS search & click for maximum resilience
        dismissed = await page.evaluate("""() => {
            let btns = Array.from(document.querySelectorAll('button, [role="button"]'));
            let closeBtn = btns.find(b => {
                let txt = (b.innerText || b.textContent || '').trim().toLowerCase();
                let parentDisclaimer = b.closest('[class*="Disclaimer"], [id*="disclaimer"], [role="alertdialog"], .react-responsive-modal-modal');
                return (txt === 'close' || txt === 'done') && parentDisclaimer !== null;
            });
            if (!closeBtn) {
                let disclaimerModal = document.querySelector('[class*="Disclaimer"], .react-responsive-modal-modal, [role="alertdialog"]');
                if (disclaimerModal) {
                    closeBtn = Array.from(disclaimerModal.querySelectorAll('button')).find(b => {
                        let txt = (b.innerText || b.textContent || '').trim().toLowerCase();
                        return txt.includes('close') || txt.includes('done');
                    });
                }
            }
            if (closeBtn) {
                closeBtn.scrollIntoView({block: 'center'});
                closeBtn.click();
                return true;
            }
            return false;
        }""")
        if dismissed:
            print("    [DISCLAIMER] Pricing disclaimer detected and successfully closed via JS.")
            await asyncio.sleep(1)
            return True
    except Exception as e:
        print(f"    [DISCLAIMER] Error checking/dismissing disclaimer: {e}")
    return False

async def fill_search_form(page, origin, dest, start_date):
    """Fill the Qantas flight search form with robust settling loops."""
    print(f"Filling search form: {origin} to {dest}")
    
    # Dismiss initial overlays and banners to ensure focus
    await page.evaluate("""() => {
        document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', keyCode:27, bubbles:true}));
        let closeSelectors = [
            'button[aria-label*="Close"]', 'button[aria-label*="close"]',
            '[class*="closeButton"]', '[class*="close-button"]', '[class*="CloseBtn"]',
            'button[aria-label*="Go back"]', 'button[aria-label*="Back"]',
        ];
        for (let sel of closeSelectors) {
            let btns = Array.from(document.querySelectorAll(sel)).filter(b => b.offsetParent !== null);
            btns.forEach(b => b.click());
        }
        document.body.click();
    }""")
    await asyncio.sleep(2)
    
    # 1. Select One Way and Wait for trip-type-toggle to actually update
    try:
        toggle = page.locator("#trip-type-toggle-button")
        await toggle.wait_for(state="visible", timeout=30000)
        text = await toggle.inner_text()
        if "One way" not in text:
            print("Toggling to One way...")
            await toggle.click()
            await asyncio.sleep(1.5)
            
            # Click the One way option via JavaScript for maximum reliability
            clicked = await page.evaluate("""() => {
                let options = Array.from(document.querySelectorAll('li, button, [role="option"], a'));
                let ow = options.find(o => {
                    let txt = (o.innerText || '').trim().toLowerCase();
                    return txt === 'one way' || o.id === 'trip-type-item-0' || o.getAttribute('aria-label') === 'One way';
                });
                if (ow) {
                    ow.scrollIntoView({block: 'center'});
                    ow.click();
                    return true;
                }
                return false;
            }""")
            if clicked:
                print("Clicked 'One way' option.")
            else:
                print("Warning: 'One way' option not found in DOM via JS search. Trying fallback locator click.")
                await page.locator("#trip-type-item-0").click()
            
            # React state sync check loop
            for _ in range(15):
                await asyncio.sleep(1)
                text = await toggle.inner_text()
                if "One way" in text:
                    print("Confirmed trip type toggle successfully updated to 'One way'.")
                    break
            else:
                raise Exception("Trip type toggle did not update to 'One way' text in time.")
    except Exception as e:
        print(f"One way selection error: {e}")
        # If we failed to confirm 'One way', let's raise it so we don't proceed to a guaranteed crash
        raise Exception(f"Failed to toggle trip type to 'One way': {e}")

    # React unmounts/remounts inputs after type toggle. Wait for new input to be stable.
    print("    Waiting for airport input to settle after toggle...")
    input_loc = page.locator("#departurePort-input")
    await input_loc.wait_for(state="visible", timeout=20000)
    await asyncio.sleep(2)

    # 2. Origin
    await select_airport(page, "departurePort-input", origin, 0)

    # 3. Destination
    await select_airport(page, "arrivalPort-input", dest, 1)
    
    # 4. Date Selection
    print("Selecting Date...")
    try:
        date_btn = page.locator("#daypicker-button")
        await date_btn.click()
        
        # Check and dismiss pricing disclaimer modal up to 5 times over 2.5 seconds
        for _ in range(5):
            await asyncio.sleep(0.5)
            await dismiss_disclaimer(page)
        
        target_date_str = start_date.strftime("%Y-%m-%d")
        print(f"Targeting date: {target_date_str}")
        
        date_cell = page.locator(f"[data-testid='{target_date_str}']")
        
        # Click the date cell, retrying with disclaimer dismissals if blocked
        success = False
        for attempt in range(3):
            try:
                await dismiss_disclaimer(page)
                if await date_cell.is_visible(timeout=3000):
                    await date_cell.click(timeout=4000)
                    success = True
                    print(f"    Successfully selected date {target_date_str} on attempt {attempt+1}")
                    break
                else:
                    day = str(start_date.day)
                    fallback_cell = page.locator("td, [role='gridcell']").filter(has_text=re.compile(f"^{day}$")).first
                    if await fallback_cell.is_visible(timeout=2000):
                        await fallback_cell.click(timeout=4000)
                        success = True
                        print(f"    Successfully selected date {day} (fallback) on attempt {attempt+1}")
                        break
            except Exception as click_err:
                print(f"    [WARN] Date click attempt {attempt+1} intercepted or failed: {click_err}. Retrying disclaimer dismiss...")
                await dismiss_disclaimer(page)
                await asyncio.sleep(1)
        
        # Fallback to direct client-side JS click if standard Playwright click fails
        if not success:
            print("    [WARN] Standard click on date cell failed. Trying client-side JS click fallback...")
            await dismiss_disclaimer(page)
            js_clicked = await page.evaluate(f"""() => {{
                let cell = document.querySelector("[data-testid='{target_date_str}']");
                if (cell) {{
                    cell.scrollIntoView({{block: 'center'}});
                    cell.click();
                    return true;
                }}
                let day = "{start_date.day}";
                let cells = Array.from(document.querySelectorAll('td, [role="gridcell"]'));
                let fallback = cells.find(c => (c.innerText || '').trim() === day);
                if (fallback) {{
                    fallback.scrollIntoView({{block: 'center'}});
                    fallback.click();
                    return true;
                }}
                return false;
            }}""")
            if js_clicked:
                print("    [OK] Date selected successfully via JS click fallback.")
                success = True
            else:
                raise Exception("Could not find or click the date cell using either standard or JS fallback.")
            
        await asyncio.sleep(1)
        await dismiss_disclaimer(page)
        
        # Continue Button
        continue_btn = None
        for selector in ["[data-testid='dialogConfirmation'] button", "[data-testid='dialogConfirmation']", "button:has-text('Continue')", "button:has-text('Done')"]:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=2000):
                continue_btn = btn
                break
        
        if not continue_btn:
            continue_btn = page.get_by_role("button", name="Continue", exact=True).first
            
        if continue_btn and await continue_btn.is_visible(timeout=2000):
            try:
                await dismiss_disclaimer(page)
                await continue_btn.click(timeout=4000)
                print("Clicked Continue.")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"    [WARN] Continue button click failed: {e}. Attempting JS click fallback...")
                await dismiss_disclaimer(page)
                js_continue = await page.evaluate("""(btn) => {
                    if (btn) { btn.scrollIntoView({block: 'center'}); btn.click(); return true; }
                    return false;
                }""", await continue_btn.element_handle())
                if js_continue:
                    print("    [OK] Clicked Continue via JS fallback.")
                    await asyncio.sleep(2)
        
        # Backup: Always press Escape to ensure the date picker modal is closed
        await page.keyboard.press("Escape")
        await asyncio.sleep(1)
            
    except Exception as e:
        print(f"Date selection error: {e}")

    # 5. Flexible with dates
    print("Selecting Flexible dates...")
    try:
        flex_label = page.get_by_text("Flexible with dates")
        if await flex_label.is_visible(timeout=3000):
            try:
                await dismiss_disclaimer(page)
                await flex_label.click(timeout=4000)
                print("Checked 'Flexible with dates'.")
            except Exception as flex_err:
                print(f"    [WARN] Flexible click failed: {flex_err}. Trying JS fallback...")
                await page.evaluate("""() => {
                    let labels = Array.from(document.querySelectorAll('label, span, div'));
                    let flex = labels.find(l => (l.innerText || '').toLowerCase().includes('flexible with dates'));
                    if (flex) { flex.click(); return true; }
                    return false;
                }""")
    except Exception as e:
        print(f"Flexible selection warning: {e}")

    await page.screenshot(path="02_form_filled.png")
    
    # Check and dismiss pricing disclaimer before clicking search
    await dismiss_disclaimer(page)
    
    # 6. Click Search
    print("Clicking Search flights...")
    search_btn = None
    for selector in [
        "[data-testid='search-flights-btn'] button",
        "[data-testid='search-flights-btn']",
        "button[type='submit']",
        "button:has-text('Search flights')",
        "button:has-text('Find flights')"
    ]:
        btn = page.locator(selector).first
        if await btn.is_visible(timeout=2000):
            search_btn = btn
            break
            
    if not search_btn:
        search_btn = page.get_by_role("button", name="Search flights").first

    try:
        await dismiss_disclaimer(page)
        await search_btn.click(timeout=5000)
        print("Clicked Search flights.")
    except Exception as e:
        print(f"Click error: {e}. Trying to dismiss disclaimer and retry...")
        await dismiss_disclaimer(page)
        try:
            await search_btn.click(timeout=5000)
            print("Clicked Search flights on retry.")
        except Exception as retry_err:
            print(f"Retry search click failed: {retry_err}. Attempting JS click fallback...")
            try:
                await page.evaluate("""(btn) => {
                    if (btn) { btn.scrollIntoView({block:'center'}); btn.click(); return true; }
                    return false;
                }""", await search_btn.element_handle())
                print("JS click fallback on search button succeeded.")
            except Exception as js_err:
                print(f"JS search click failed: {js_err}")

async def wait_for_results_page(page, origin, dest):
    """Monitor navigation and wait until results page dates ribbon settled successfully."""
    tag = f"[{origin}->{dest}]"
    homepages = {
        "https://www.qantas.com/en-au",
        "https://www.qantas.com/en-au/",
        "https://www.qantas.com/",
        "https://www.qantas.com",
    }
    
    print(f"    [WAIT] {tag} Waiting for results page...")
    await asyncio.sleep(4)
    
    deadline = datetime.now() + timedelta(seconds=120)
    results_found = False
    
    while datetime.now() < deadline:
        try:
            cur_url = page.url.split("?")[0].rstrip("/")
        except Exception:
            await asyncio.sleep(3)
            continue
            
        on_homepage = cur_url in {u.rstrip("/") for u in homepages}
        
        if not on_homepage:
            print(f"    [MOVE] {tag} URL moved -> {cur_url}")
            
            # Wait for URL to fully settle
            for _ in range(20):
                await asyncio.sleep(3)
                try:
                    new_url = page.url.split("?")[0].rstrip("/")
                except Exception:
                    break
                if new_url == cur_url:
                    break
                cur_url = new_url
                print(f"    [SETTLE] {tag} Settling -> {cur_url}")
                
            page_title = ""
            try:
                page_title = (await page.title()).lower()
            except Exception:
                pass
                
            # Check for block/captcha
            if any(kw in page_title for kw in ("access denied", "403", "blocked", "captcha")):
                body = ""
                try:
                    body = (await page.content()).lower()
                except Exception:
                    pass
                if any(kw in body for kw in ("access denied", "403 forbidden", "you have been blocked", "captcha")):
                    raise Exception(f"Access Denied / Blocked at {cur_url}")
                    
            # Tripflow / intermediate redirect — wait for final destination
            redirect_wait = 0
            while ("tripflow" in cur_url or "redirect" in cur_url) and redirect_wait < 90:
                await asyncio.sleep(3)
                redirect_wait += 3
                try:
                    cur_url = page.url.split("?")[0].rstrip("/")
                    page_title = (await page.title()).lower()
                except Exception:
                    break
                if "tripflow" not in cur_url and "redirect" not in cur_url:
                    print(f"    [OK] {tag} Redirect resolved -> {cur_url}")
                    break
                if any(kw in page_title for kw in ("access denied", "403", "blocked")):
                    raise Exception(f"Access Denied after redirect at {cur_url}")
                    
            # Final check: verify date ribbon exists
            await asyncio.sleep(5)  # let page fully render
            
            ribbon_ok = await page.evaluate("""() => {
                let sels = ['.cal-tab-body','[id*="tab-date"]','.date-ribbon__tab',
                            '.flex-linear-calendar button','[role="tab"]'];
                for (let s of sels) {
                    let els = Array.from(document.querySelectorAll(s)).filter(e => {
                        let txt = (e.innerText||'').trim();
                        return /\\d/.test(txt) && txt.length < 300;
                    });
                    if (els.length >= 3) return els.length;
                }
                return 0;
            }""")
            
            if not ribbon_ok:
                print(f"    [WARN] {tag} URL moved but NO date ribbon found ({cur_url}) - waiting more...")
                for _ in range(4):
                    await asyncio.sleep(5)
                    ribbon_ok = await page.evaluate("""() => {
                        let sels = ['.cal-tab-body','[id*="tab-date"]','.date-ribbon__tab',
                                    '.flex-linear-calendar button','[role="tab"]'];
                        for (let s of sels) {
                            let els = Array.from(document.querySelectorAll(s)).filter(e => {
                                let txt = (e.innerText||'').trim();
                                return /\\d/.test(txt) && txt.length < 300;
                            });
                            if (els.length >= 3) return els.length;
                        }
                        return 0;
                    }""")
                    if ribbon_ok:
                        break
                        
                if not ribbon_ok:
                    print(f"    [WARN] {tag} Ribbon still not found - forcing return to retry search")
                    return False
                    
            print(f"    [OK] {tag} Date ribbon confirmed ({ribbon_ok} tabs)")
            results_found = True
            await asyncio.sleep(2)
            break
            
        elapsed = 120 - int((deadline - datetime.now()).total_seconds())
        print(f"    [WAIT] {tag} {elapsed}s - on homepage, waiting for navigation...")
        await asyncio.sleep(5)
        
    if not results_found:
        raise Exception(f"Search never left homepage after 120s. URL={page.url}")
        
    print(f"    [URL] {tag} Results URL: {page.url}")
    return True

# ══════════════════════════════════════════════════════════════════
#  SCRAPING & RIBBON PARSING HELPERS
# ══════════════════════════════════════════════════════════════════

async def extract_ribbon_tabs(page, today):
    """Extract ribbon tabs currently visible in the dates ribbon."""
    return await page.evaluate("""() => {
        let selectors = ['.cal-tab-body', '[id*="tab-date"]', '.date-ribbon__tab', '.flex-linear-calendar button', '[role="tab"]'];
        let tabs = [];
        for (let sel of selectors) {
            let found = Array.from(document.querySelectorAll(sel)).filter(t => {
                let txt = (t.innerText || '').trim();
                return /\\d/.test(txt) && txt.length < 300 && !txt.includes('Privacy') && !txt.includes('Cookie');
            });
            if (found.length > 0) { 
                return found.map((t, i) => ({
                    index: i,
                    text: t.innerText.trim(),
                    id: t.id
                }));
            }
        }
        return [];
    }""")

async def click_tab(page, tab_index):
    """Click on a specific dates ribbon tab by its index."""
    try:
        clicked = await page.evaluate("""(idx) => {
            let selectors = ['.cal-tab-body', '[id*="tab-date"]', '.date-ribbon__tab', '.flex-linear-calendar button', '[role="tab"]'];
            let tabs = [];
            for (let sel of selectors) {
                let found = Array.from(document.querySelectorAll(sel)).filter(t => {
                    let txt = (t.innerText || '').trim();
                    return /\\d/.test(txt) && !txt.includes('Privacy') && !txt.includes('Cookie');
                });
                if (found.length > 0) { tabs = found; break; }
            }
            if (tabs[idx]) {
                tabs[idx].scrollIntoView({block: 'center', inline: 'center'});
                tabs[idx].click();
                return true;
            }
            return false;
        }""", tab_index)
        return clicked
    except Exception as e:
        print(f"Error clicking tab {tab_index}: {e}")
        return False

async def click_next_arrow(page):
    """Click on the calendar next button to load the next block of dates."""
    try:
        clicked = await page.evaluate("""() => {
            let btn = null;

            // Strategy 1: text includes 'next' + ('day' or '14')
            btn = Array.from(document.querySelectorAll('a, button')).find(b => {
                let txt = (b.innerText || '').toLowerCase().trim();
                return txt.includes('next') && (txt.includes('day') || txt.includes('14'));
            });

            // Strategy 2: aria-label or class name contains 'next'
            if (!btn) {
                btn = Array.from(document.querySelectorAll('button, a')).find(b => {
                    let lbl = (b.getAttribute('aria-label') || '').toLowerCase();
                    let cls = (b.className || '').toLowerCase();
                    return lbl.includes('next') || cls.includes('next-btn')
                        || cls.includes('nextbutton') || cls.includes('next-button');
                });
            }

            // Strategy 3: last visible button inside any known ribbon/calendar container
            if (!btn) {
                let ribbon = document.querySelector(
                    '.flex-linear-calendar, .date-ribbon, [class*="linearCalendar"], [class*="dateRibbon"], [class*="calendarNav"]'
                );
                if (ribbon) {
                    let btns = Array.from(ribbon.querySelectorAll('button, a')).filter(b => b.offsetParent !== null);
                    if (btns.length) btn = btns[btns.length - 1];
                }
            }

            // Strategy 4: any visible button in the right 30% of the viewport (navigation arrow)
            if (!btn) {
                let candidates = Array.from(document.querySelectorAll('button, a[role="button"]')).filter(b => {
                    let r = b.getBoundingClientRect();
                    return r.width > 0 && r.height > 0 && r.right > window.innerWidth * 0.7 && r.top < window.innerHeight;
                });
                if (candidates.length) btn = candidates[candidates.length - 1];
            }

            if (btn) {
                btn.scrollIntoView({block: 'center', inline: 'nearest'});
                btn.click();
                return true;
            }
            return false;
        }""")
        return clicked
    except Exception as e:
        print(f"Error clicking next arrow: {e}")
        return False

async def scrape_flight_cards(page, origin, dest):
    """Scrape flight cards from results page based on standard or shadow DOM structure."""
    is_special = (
        (origin == "BME" and dest == "DRW") or
        (origin == "DRW" and dest == "KNX") or
        (origin == "DRW" and dest == "BME") or
        (origin == "KNX" and dest == "DRW")
    )
    if is_special:
        return await scrape_flight_cards_shadow(page, origin, dest)
    else:
        return await scrape_flight_cards_standard(page)

async def scrape_flight_cards_standard(page):
    """Scrape standard flight row layouts."""
    try:
        data = await page.evaluate("""() => {
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
        }""")
        
        results = []
        for item in (data or []):
            if item.get("ecoPrice"):
                results.append({"fare_class": "Economy",  "fare_price": item["ecoPrice"], "departure_time": item["depTime"]})
            if item.get("bizPrice"):
                results.append({"fare_class": "Business", "fare_price": item["bizPrice"], "departure_time": item["depTime"]})
        return results
    except Exception as e:
        print(f"Error scraping standard cards: {e}")
        return []

async def scrape_flight_cards_shadow(page, origin, dest):
    """Scrape Airnorth / direct flight special shadow DOM flight row structures."""
    origin_name = AIRPORT_NAMES.get(origin, origin).lower()
    dest_name   = AIRPORT_NAMES.get(dest, dest).lower()
    origin_code = origin.lower()
    dest_code   = dest.lower()
    
    try:
        data = await page.evaluate("""({ originName, destName, originCode, destCode }) => {
            let results = [];
            function getDeepText(node) {
                let text = node.innerText || '';
                if (node.shadowRoot) text += ' ' + getDeepText(node.shadowRoot);
                for (let child of node.children || []) text += ' ' + getDeepText(child);
                return text;
            }
            function findFlightRows(root) {
                let found = [];
                let candidates = root.querySelectorAll('grouped-avail-flight-row, .flight-card, .upsell-row, [class*="FlightCard"], [class*="flight-row"]');
                candidates.forEach(c => found.push(c));
                let all = root.querySelectorAll('*');
                all.forEach(el => { if (el.shadowRoot) found = found.concat(findFlightRows(el.shadowRoot)); });
                return found;
            }
            let rows = findFlightRows(document);
            if (rows.length === 0) rows = Array.from(document.querySelectorAll('div')).filter(d => d.innerText.length > 50 && d.innerText.length < 1500);

            for (let row of rows) {
                if (row.closest && row.closest('.flex-linear-calendar, .date-ribbon, .search-bar')) continue;
                let txt = getDeepText(row);
                let lowerTxt = txt.toLowerCase();

                let hasRoute = (lowerTxt.includes(originName) && lowerTxt.includes(destName)) ||
                               (lowerTxt.includes(originCode) && lowerTxt.includes(destCode));

                if (originCode === 'bme' && destCode === 'drw') {
                    if (lowerTxt.includes('airnorth')) hasRoute = true;
                }
                if (originCode === 'drw' && destCode === 'bme') {
                    if (lowerTxt.includes('airnorth')) hasRoute = true;
                }

                if (hasRoute) {
                    let times = txt.match(/(\\d{1,2}:\\d{2})/g);
                    if (!times || times.length < 1) continue;

                    let ecoPrice = null, bizPrice = null;
                    let priceMatches = txt.match(/\\$([0-9,]+)/g);

                    if (priceMatches) {
                        let numericPrices = [];
                        priceMatches.forEach(p => {
                            let val = parseFloat(p.replace(/[^0-9.]/g, ''));
                            if (!numericPrices.includes(val)) numericPrices.push(val);
                        });

                        if (numericPrices.length === 1) {
                            let priceStr = priceMatches[0];
                            let parts = txt.split(priceStr);
                            let prefix = parts[0].toLowerCase();
                            let suffix = (parts[1] || '').toLowerCase();
                            if (prefix.includes('no seats')) {
                                bizPrice = numericPrices[0];
                            } else {
                                ecoPrice = numericPrices[0];
                                if (suffix.includes('no seats')) bizPrice = null;
                            }
                        } else if (numericPrices.length >= 2) {
                            ecoPrice = numericPrices[0];
                            bizPrice = numericPrices[1];
                        }
                    }

                    let isDirect = !lowerTxt.includes('1 stop') && !lowerTxt.includes('2 stop') && !lowerTxt.includes('via') && !lowerTxt.includes('connect');
                    if (row.classList && row.classList.contains('e2e-direct-flight')) isDirect = true;

                    let specialRoutes = [['bme','drw'], ['drw','knx'], ['drw','bme'], ['knx','drw']];
                    let isSpecialRoute = specialRoutes.some(r => r[0] === originCode && r[1] === destCode);

                    if (isSpecialRoute) {
                        let hubs = ['perth', 'sydney', 'melbourne', 'brisbane', 'adelaide', 'alice springs', 'cairns'];
                        let hubsToExclude = hubs.filter(h => h !== originName && h !== destName);
                        let containsHub = hubsToExclude.some(h => lowerTxt.includes(h));
                        if (lowerTxt.includes('airnorth') && !containsHub) {
                            isDirect = true;
                        } else {
                            isDirect = false;
                        }
                    }

                    if (ecoPrice || bizPrice) {
                        let key = times[0] + (ecoPrice || bizPrice);
                        if (!results.some(r => r.key === key)) {
                            results.push({ key: key, depTime: times[0], ecoPrice: ecoPrice, bizPrice: bizPrice, isDirect: isDirect });
                        }
                    }
                }
            }
            return results;
        }""", {"originName": origin_name, "destName": dest_name, "originCode": origin_code, "destCode": dest_code})
        
        results = []
        for item in (data or []):
            if item["isDirect"]:
                if item["ecoPrice"]:
                    results.append({"fare_class": "Economy",  "fare_price": item["ecoPrice"], "departure_time": item["depTime"]})
                if item["bizPrice"]:
                    results.append({"fare_class": "Business", "fare_price": item["bizPrice"], "departure_time": item["depTime"]})
        return results
    except Exception as e:
        print(f"Error scraping shadow cards: {e}")
        return []

# ══════════════════════════════════════════════════════════════════
#  MAIN SCRAPING WORKFLOW
# ══════════════════════════════════════════════════════════════════

async def scrape_route(origin, dest, today):
    """Run Qantas fare scraper for a single route from start to finish."""
    all_rows = []
    
    # ── Bright Data Scraping Browser Setup ──
    route_key = (origin, dest)
    creds = ROUTE_CREDENTIALS.get(route_key, {"zone": "scraping_browser2", "password": "nymmsv0ffs60"})
    zone = creds["zone"]
    password = creds["password"]
    
    sid = random.randint(1000000, 9999999)
    username = f"brd-customer-{CUSTOMER_ID}-zone-{zone}-country-au-session-{sid}"
    browser_wss = f"wss://{username}:{password}@brd.superproxy.io:9222"
    
    print(f"\n[START] Starting Qantas Playwright Scraper - {today}")
    print(f"Route   : {origin} -> {dest} | Zone: {zone}")
    print(f"CDP WSS : wss://{username.split('-session-')[0]}-session-{sid}@brd.superproxy.io:9222")
    
    async with async_playwright() as p:
        print("Connecting to Bright Data Scraping Browser...")
        browser = await p.chromium.connect_over_cdp(browser_wss)
        
        try:
            # Create isolated browser context matching standard viewport
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080}
            )
            page = await context.new_page()
            
            print("Navigating to Qantas home page...")
            await page.goto("https://www.qantas.com/en-au", wait_until="domcontentloaded", timeout=90000)
            
            try:
                # Dismiss region popup if present
                stay_btn = page.get_by_role("button", name="Stay on Qantas AU")
                if await stay_btn.is_visible(timeout=5000):
                    await stay_btn.click()
                    print("Clicked 'Stay on Qantas AU'")
            except:
                pass

            await handle_cookies(page)
            
            # Fill the search form and click search button
            await fill_search_form(page, origin, dest, today)
            
            # Monitor and wait for navigation to complete
            nav_ok = await wait_for_results_page(page, origin, dest)
            if not nav_ok:
                print(f"[FAIL] [{origin}->{dest}] Failed to reach results page.")
                raise Exception("Failed to reach results page.")

            collected_dates = set()
            limit_date = today + timedelta(days=DAYS_OUT)
            is_special = (
                (origin == "BME" and dest == "DRW") or
                (origin == "DRW" and dest == "KNX") or
                (origin == "DRW" and dest == "BME") or
                (origin == "KNX" and dest == "DRW")
            )
            expected_date = today
            no_new_streak = 0
            
            while len(collected_dates) < DAYS_OUT:
                raw_tabs = await extract_ribbon_tabs(page, today)
                tabs = []
                for rt in raw_tabs:
                    d_obj = parse_date_text(rt["text"], today.year, today)
                    if d_obj and d_obj >= today and d_obj <= limit_date:
                        tabs.append({**rt, "date_obj": d_obj, "date_str": str(d_obj)})
                
                tabs.sort(key=lambda t: t["date_obj"])
                new_tabs = [t for t in tabs if t["date_obj"] not in collected_dates and t["date_obj"] >= expected_date]
                
                if not new_tabs:
                    no_new_streak += 1
                    print(f"    [WARN] No new tabs in view (streak {no_new_streak})")
                    if no_new_streak >= 3:
                        print("Reached maximum tab streak without progress. Finishing.")
                        break

                    arrow_clicked = await click_next_arrow(page)
                    if arrow_clicked:
                        print("    Clicked Next Calendar Arrow...")
                        await asyncio.sleep(6)
                    else:
                        print(f"    [WARN] Could not click next arrow (streak {no_new_streak}), will retry...")
                        await asyncio.sleep(4)
                    continue
                
                no_new_streak = 0
                
                for tab in new_tabs:
                    if len(collected_dates) >= DAYS_OUT:
                        break
                        
                    date_obj = tab["date_obj"]
                    date_str = tab["date_str"]
                    
                    # Gap fill missing dates
                    while expected_date < date_obj and len(collected_dates) < DAYS_OUT:
                        gap_str = str(expected_date)
                        fc = "No Direct Flight" if is_special else "NO FLIGHTS"
                        record_row(all_rows, origin, dest, gap_str, "", None, fc)
                        collected_dates.add(expected_date)
                        expected_date += timedelta(days=1)
                        print(f"    [{len(collected_dates)}/{DAYS_OUT}] {gap_str}  [GAP] Gap-filled ({fc})")

                    if len(collected_dates) >= DAYS_OUT:
                        break
                        
                    print(f"    [{len(collected_dates)+1}/{DAYS_OUT}] Scraping {date_str}...", end=" ")
                    
                    try:
                        await click_tab(page, tab["index"])
                        await asyncio.sleep(3)
                        
                        if "no flights" in tab["text"].lower():
                            fc = "No Direct Flight" if is_special else "NO FLIGHTS"
                            record_row(all_rows, origin, dest, date_str, "", None, fc)
                            print(f"[STOP] {fc}")
                        else:
                            fares = await scrape_flight_cards(page, origin, dest)
                            if fares:
                                for f in fares:
                                    record_row(all_rows, origin, dest, date_str,
                                               f["departure_time"], f["fare_price"], f["fare_class"])
                                print(f"[OK] {len(fares)} fares")
                            else:
                                fc = "No Direct Flight" if is_special else "SOLD OUT"
                                record_row(all_rows, origin, dest, date_str, "", None, fc)
                                print(f"[STOP] {fc}")
                                
                    except Exception as tab_err:
                        print(f"[WARN] Error scraping {date_str}: {tab_err}")
                        record_row(all_rows, origin, dest, date_str, "", None, "NO DATA")
                        
                    collected_dates.add(date_obj)
                    expected_date = date_obj + timedelta(days=1)
                    
                    if len(collected_dates) % 7 == 0:
                        save_route(all_rows, origin, dest, today)
                
                if len(collected_dates) < DAYS_OUT:
                    await click_next_arrow(page)
                    await asyncio.sleep(6)

            # Final Save
            save_route(all_rows, origin, dest, today)
            print(f"\n[FINISH] Scrape complete for {origin}->{dest}! {len(collected_dates)} dates processed successfully.")
            
        except (Exception, asyncio.CancelledError) as e:
            is_cancelled = isinstance(e, asyncio.CancelledError)
            err_lbl = "Cancelled/Timeout" if is_cancelled else "crashed"
            print(f"\n[CRASH] Route {origin}->{dest} {err_lbl}: {e}")
            if not is_cancelled:
                traceback.print_exc()
            try:
                await page.screenshot(path=f"crash_{origin}_{dest}.png")
            except:
                pass
            
            # Ensure we save any partially gathered data
            existing = {r["Date of Departure"] for r in all_rows}
            for i in range(DAYS_OUT):
                d = today + timedelta(days=i)
                if str(d) not in existing:
                    record_row(all_rows, origin, dest, str(d), "", None, "NO DATA")
            save_route(all_rows, origin, dest, today)
            raise e
            
        finally:
            await browser.close()

def get_australia_today():
    """Dynamically determine today's date in Australia (AEST/AEDT) timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Australia/Sydney")).date()
    except Exception:
        # Fallback manual calculation of UTC offset (AEST/AEDT)
        from datetime import timezone
        utc_now = datetime.now(timezone.utc)
        year = utc_now.year
        # DST end: First Sunday of April
        apr_1 = datetime(year, 4, 1, tzinfo=timezone.utc)
        apr_dst_end = apr_1 + timedelta(days=((6 - apr_1.weekday()) % 7))
        # DST start: First Sunday of October
        oct_1 = datetime(year, 10, 1, tzinfo=timezone.utc)
        oct_dst_start = oct_1 + timedelta(days=((6 - oct_1.weekday()) % 7))
        
        is_dst = utc_now < apr_dst_end or utc_now >= oct_dst_start
        offset = 11 if is_dst else 10
        return (utc_now + timedelta(hours=offset)).date()

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Qantas Playwright Fare Tracker")
    parser.add_argument(
        "--route", type=int, default=None, choices=[1, 2, 3, 4, 5, 6, 7, 8, 9],
        help=(
            "Run specific route: 1=BME->KNX, 2=BME->DRW, 3=DRW->KNX, 4=KNX->BME, "
            "5=PER->GET, 6=GET->PER, 7=DRW->BME, 8=KNX->DRW, 9=All routes sequentially"
        )
    )
    return parser.parse_args()

def interactive_menu():
    """Display interactive route selection menu."""
    print(f"\n{'='*60}")
    print(f"  [FLY] Qantas Playwright Scraper")
    print(f"  Select route(s) to scrape:\n")
    for i, (o, d) in enumerate(ROUTES, 1):
        zone = ROUTE_CREDENTIALS[(o, d)]["zone"]
        print(f"    {i}. {o} -> {d}  (zone: {zone})")
    print(f"    9. All 8 routes sequentially\n")

    while True:
        try:
            choice = int(input(f"  Enter choice (1-9): ").strip())
            if 1 <= choice <= 8:
                return [ROUTES[choice - 1]]
            elif choice == 9:
                return list(ROUTES)
        except Exception:
            pass

async def main():
    args = parse_args()
    today = get_australia_today()

    # Determine which routes to run
    if args.route is not None:
        if args.route == 9:
            routes = list(ROUTES)
        else:
            routes = [ROUTES[args.route - 1]]
    elif len(sys.argv) == 1 and sys.stdin.isatty():
        # No args and interactive session - show menu
        routes = interactive_menu()
    else:
        # Non-interactive session or fallback - run all 8 routes sequentially
        print("Non-interactive session detected (Cron/CI). Defaulting to running all 8 routes sequentially.")
        routes = list(ROUTES)

    print(f"\n{'='*60}")
    print(f"  [FLY] Qantas Playwright Scraper -- {today} (Australia Time)")
    print(f"  Selected Routes : {len(routes)}")
    for o, d in routes:
        print(f"    - {o} -> {d}")
    print(f"{'='*60}\n")

    failed_routes = []
    for origin, dest in routes:
        try:
            print(f"\n[RUN] Starting scrape for {origin} -> {dest} (Max limit: 20 minutes)...")
            # 20-minute timeout per route: 84 dates × ~6s each ≈ 9 min, plus navigation/save overhead
            await asyncio.wait_for(scrape_route(origin, dest, today), timeout=1200.0)
            
            if len(routes) > 1:
                print("\n[WAIT] Waiting 20 seconds before starting next route to avoid IP block/rate limits...")
                await asyncio.sleep(20)
        except asyncio.TimeoutError:
            print(f"\n[TIMEOUT] Route {origin} -> {dest} exceeded the 20-minute execution limit. Skipping to the next route...")
            failed_routes.append(f"{origin}->{dest} (Timeout)")
        except Exception as err:
            print(f"\n[FAIL] Route {origin} -> {dest} failed execution: {err}")
            failed_routes.append(f"{origin}->{dest}")
            # Continue running subsequent routes!

    print(f"\n{'='*60}")
    if failed_routes:
        print(f"  [DONE] Execution completed with failures.")
        print(f"  Failed Routes : {', '.join(failed_routes)}")
        print(f"  Note: Other successful routes' CSV and Excel files are fully saved in output/ directory.")
    else:
        print(f"  [DONE] All selected routes executed successfully! Check output/ folder.")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    asyncio.run(main())
