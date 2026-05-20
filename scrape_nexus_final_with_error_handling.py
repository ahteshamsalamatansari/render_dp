import asyncio
import csv
import logging
import os
import sys
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from playwright.async_api import async_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

ROUTES = [
    ("PER", "GET"),
    ("GET", "PER"),
    ("PER", "BME"),
    ("BME", "PER"),
    ("KTA", "BME"),
    ("BME", "KTA"),
    ("PHE", "BME"),
    ("BME", "PHE"),
    ("GET", "BME"),
    ("BME", "GET"),
]

MAX_RETRIES = 3
RETRY_BASE_DELAY = 5       # seconds; doubles each attempt: 5 → 10 → 20
MAX_CONSECUTIVE_ERRORS = 8  # skip remaining dates in route after this many back-to-back failures

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


class NexusScraper:
    def __init__(self, headless=True, progress_callback=None, stop_requested=None):
        self.headless = headless
        self.results = []
        self.captured_by_date = {}
        self.progress_callback = progress_callback
        self.stop_requested = stop_requested
        self._browser = None
        self._context = None
        self._bd_browser_ws = None
        self._p = None

    @property
    def captured_json(self):
        if not hasattr(self, "current_target_date") or not self.current_target_date:
            return None
        return self.captured_by_date.get(self.current_target_date)

    @captured_json.setter
    def captured_json(self, value):
        if not hasattr(self, "captured_by_date"):
            self.captured_by_date = {}
        if value is None:
            if hasattr(self, "current_target_date") and self.current_target_date:
                self.captured_by_date.pop(self.current_target_date, None)
        else:
            if hasattr(self, "current_target_date") and self.current_target_date:
                self.captured_by_date[self.current_target_date] = value

    def should_stop(self):
        if not self.stop_requested:
            return False
        try:
            return bool(self.stop_requested())
        except Exception:
            return False

    async def handle_response(self, response):
        if "Ajax/Search/Flights/" in response.url:
            try:
                post_data = response.request.post_data or ""
                params = urllib.parse.parse_qs(post_data)
                date_list = params.get("departDate") or params.get("departdate")

                dep_date = None
                if date_list:
                    dep_date = date_list[0]
                    self.captured_by_date[dep_date] = await response.json()
                else:
                    # Fallback: derive date from the flight payload itself
                    js = await response.json()
                    outgoing = js.get("Outgoing", [])
                    if outgoing:
                        dep_time_iso = outgoing[0].get("DepartsLocalISO8601") or ""
                        if "T" in dep_time_iso:
                            dep_date = dep_time_iso.split("T")[0]
                    if dep_date:
                        self.captured_by_date[dep_date] = js
            except Exception:
                pass

    def get_proxy_config(self):
        bd_proxy_host = os.environ.get("BRIGHTDATA_PROXY_HOST")
        bd_proxy_user = os.environ.get("BRIGHTDATA_PROXY_USER")
        bd_proxy_pass = os.environ.get("BRIGHTDATA_PROXY_PASS")

        if bd_proxy_host and bd_proxy_user and bd_proxy_pass:
            logger.info("Routing traffic via Bright Data Residential Proxy...")
            return {
                "server": f"http://{bd_proxy_host}",
                "username": bd_proxy_user,
                "password": bd_proxy_pass,
            }

        proxy_url = os.environ.get("PROXY_URL")
        if proxy_url:
            logger.info("Routing traffic via standard PROXY_URL...")
            return {"server": proxy_url}

        api_key = os.environ.get("SCRAPING_API_KEY")
        provider = os.environ.get("SCRAPING_API_PROVIDER", "zenrows").lower()
        if not api_key:
            return None

        logger.info(f"Routing traffic via Managed Scraping Smart Proxy ({provider})...")
        if provider == "zenrows":
            return {
                "server": "http://proxy.zenrows.com:8001",
                "username": api_key,
                "password": "js_render=true&premium_proxy=true",
            }
        elif provider == "scrapfly":
            return {
                "server": "http://proxy.scrapfly.io:80",
                "username": api_key,
                "password": "asp=true&render_js=true",
            }
        elif provider == "scrapeops":
            return {
                "server": "http://proxy.scrapeops.io:80",
                "username": api_key,
                "password": "bypass=cloudflare",
            }
        return None

    # ------------------------------------------------------------------
    # Browser lifecycle helpers
    # ------------------------------------------------------------------

    async def _init_browser_and_context(self):
        """Connect (or reconnect) browser and open a fresh context."""
        if self._bd_browser_ws:
            logger.info("Connecting to Bright Data Scraping Browser via CDP...")
            self._browser = await self._p.chromium.connect_over_cdp(self._bd_browser_ws)
            self._context = await self._browser.new_context()
        else:
            proxy_config = self.get_proxy_config()
            launch_kwargs = {
                "headless": self.headless,
                "args": [
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            }
            if proxy_config:
                launch_kwargs["proxy"] = proxy_config
            self._browser = await self._p.chromium.launch(**launch_kwargs)
            context_kwargs = {"proxy": proxy_config} if proxy_config else {}
            self._context = await self._browser.new_context(**context_kwargs)

    async def _new_page(self):
        """Open a new page and attach the response listener."""
        page = await self._context.new_page()
        page.on("response", self.handle_response)
        return page

    async def _warm_session(self, page, timeout=20000):
        """Hit the homepage to establish cookies/session."""
        await page.goto(
            "https://nexusairlines.com.au/",
            wait_until="domcontentloaded",
            timeout=timeout,
        )
        await asyncio.sleep(2)

    async def _recreate_page(self, page):
        """
        Close the broken page and return a healthy replacement.
        Attempts context reuse first; falls back to full browser reconnect.
        """
        try:
            await page.close()
        except Exception:
            pass

        # Try within the existing context
        try:
            new_page = await self._new_page()
            await self._warm_session(new_page)
            return new_page
        except Exception as ctx_err:
            logger.warning(f"    Context unhealthy ({ctx_err}); performing full browser reconnect...")

        # Full reconnect
        try:
            await self._browser.close()
        except Exception:
            pass
        await self._init_browser_and_context()
        new_page = await self._new_page()
        await self._warm_session(new_page)
        logger.info("    Browser reconnected successfully.")
        return new_page

    # ------------------------------------------------------------------
    # Core scraping logic
    # ------------------------------------------------------------------

    async def _scrape_single_date(self, page, origin, dest, target_date):
        """
        Navigate and capture JSON for one date.
        Returns the page. Raises on timeout or unrecoverable failure.
        """
        date_str = target_date.strftime("%d/%m/%Y")
        encoded_date = quote(date_str, safe="")
        search_url = (
            f"https://secure.nexusairlines.com.au/Booking/Search"
            f"?From={origin}&To={dest}&Depart={encoded_date}"
            f"&Adults=1&Children=0&Infants=0"
        )

        self.current_target_date = target_date.strftime("%Y-%m-%d")
        self.captured_json = None

        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)

        # Wait for Cloudflare/Turnstile challenge + redirect
        for _ in range(30):
            if self.should_stop():
                return page
            await asyncio.sleep(1)
            if self.captured_json:
                break
            title = await page.title()
            if "Just a moment..." not in title and "Loading" not in title and title != "":
                break

        await page.wait_for_load_state("domcontentloaded")

        # Dismiss resident-fare modal if present
        has_modal = await page.evaluate("() => !!document.querySelector('#nonResidentFare')")
        if has_modal:
            logger.info("    Resident modal detected — clicking Non-Resident Fare...")
            self.captured_json = None
            await page.evaluate("document.querySelector('#nonResidentFare').click();")
            for _ in range(15):
                if self.should_stop():
                    return page
                if self.captured_json:
                    break
                await asyncio.sleep(1)

        # Click submit if still on search form without data
        if "Booking/Search" in page.url and not self.captured_json:
            submit_btn = page.locator("#submit")
            if await submit_btn.is_visible():
                await submit_btn.click(force=True)
                for _ in range(10):
                    if self.should_stop():
                        return page
                    if self.captured_json:
                        break
                    await asyncio.sleep(1)
        else:
            for _ in range(10):
                if self.should_stop():
                    return page
                if self.captured_json:
                    break
                await asyncio.sleep(1)

        return page

    async def scrape_all(self, routes, days=84):
        total = max(1, len(routes) * days)
        completed = 0

        self._bd_browser_ws = os.environ.get(
            "BRIGHTDATA_BROWSER_WS",
            "wss://brd-customer-hl_fbc4a16a-zone-cron_nexus:0td7q08n8f70@brd.superproxy.io:9222",
        )

        async with async_playwright() as p:
            self._p = p
            await self._init_browser_and_context()

            page = await self._new_page()
            logger.info("Establishing session via homepage...")
            await self._warm_session(page)

            start_date = datetime.now() + timedelta(days=1)

            for origin, dest in routes:
                if self.should_stop():
                    break

                logger.info(f"Scraping route: {origin} -> {dest}")
                consecutive_errors = 0
                route_captured = 0

                for i in range(days):
                    if self.should_stop():
                        break

                    # Circuit breaker: too many consecutive failures on this route
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        remaining = days - i
                        logger.error(
                            f"  {consecutive_errors} consecutive failures on {origin}->{dest}. "
                            f"Skipping remaining {remaining} date(s) for this route."
                        )
                        completed += remaining
                        break

                    target_date = start_date + timedelta(days=i)
                    date_str = target_date.strftime("%d/%m/%Y")
                    logger.info(f"  - Date: {date_str}")

                    if self.progress_callback:
                        self.progress_callback(
                            completed,
                            total,
                            f"Nexus {origin}->{dest} {target_date.strftime('%Y-%m-%d')}",
                        )

                    success = False
                    for attempt in range(MAX_RETRIES):
                        if self.should_stop():
                            break
                        try:
                            page = await self._scrape_single_date(page, origin, dest, target_date)
                            if self.captured_json:
                                self.parse_json(self.captured_json, target_date, origin, dest)
                                route_captured += 1
                            consecutive_errors = 0
                            success = True
                            break
                        except Exception as e:
                            if attempt < MAX_RETRIES - 1:
                                wait = RETRY_BASE_DELAY * (2 ** attempt)
                                logger.warning(
                                    f"    Attempt {attempt + 1}/{MAX_RETRIES} failed for {date_str}: {e}. "
                                    f"Retrying in {wait}s..."
                                )
                                await asyncio.sleep(wait)
                                try:
                                    page = await self._recreate_page(page)
                                except Exception as rec_err:
                                    logger.error(f"    Page recreation failed: {rec_err}. Skipping date.")
                                    break
                            else:
                                logger.error(
                                    f"    All {MAX_RETRIES} attempts exhausted for {date_str}: {e}"
                                )

                    if not success:
                        consecutive_errors += 1

                    completed += 1
                    if self.progress_callback:
                        self.progress_callback(
                            completed,
                            total,
                            f"Nexus {origin}->{dest} {target_date.strftime('%Y-%m-%d')} complete",
                        )

                logger.info(
                    f"Route {origin}->{dest} complete: {route_captured}/{days} dates captured."
                )
                # Checkpoint after every route so a mid-run crash loses at most one route's worth of data
                self.save_to_csv("nexus_partial_latest.csv")

            try:
                await self._browser.close()
            except Exception:
                pass

    def parse_json(self, data, date, origin, dest):
        outgoing = data.get("Outgoing", [])
        if not outgoing:
            return

        for flight in outgoing:
            dep_time_iso = flight.get("DepartsLocalISO8601") or ""
            time_str = dep_time_iso.split("T")[1][:5] if "T" in dep_time_iso else ""

            flight_date_str = dep_time_iso.split("T")[0] if "T" in dep_time_iso else ""
            if flight_date_str and flight_date_str != date.strftime("%Y-%m-%d"):
                continue  # skip flights from other dates in the 7-day window

            fares = flight.get("AdvancedFares", []) or flight.get("Fares", [])
            for fare in fares:
                price = fare.get("Adult", 0.0)
                fees = fare.get("Taxes", 0.0)
                total = fare.get("Total", price + fees)
                fare_class = fare.get("FareClass", "")
                fare_name = fare.get("DisplayName", "")
                seats_available = fare.get("SeatsAvailable", 0)
                inactive_only = fare.get("InactiveOnly", False)
                availability = "Sold Out" if inactive_only or seats_available <= 0 else "Available"

                self.results.append({
                    "Date Checked": datetime.now().strftime("%d/%m/%Y"),
                    "Time Checked": datetime.now().strftime("%H:%M"),
                    "Airline": "Nexus Airlines",
                    "Date of Departure": date.strftime("%Y-%m-%d"),
                    "Time of Departure": time_str,
                    "Origin": origin,
                    "Destination": dest,
                    "Fare Price": price,
                    "Fees & Charges": fees,
                    "Base+fees charges": total,
                    "Fare Class": f"{fare_name} ({fare_class})",
                    "Seats Available": max(0, seats_available) if availability == "Available" else 0,
                    "Availability": availability,
                    "Source": "https://nexusairlines.com.au/",
                })

    def save_to_csv(self, filename):
        if not self.results:
            logger.warning("No results to save.")
            return
        keys = [
            "Date Checked", "Time Checked", "Airline",
            "Date of Departure", "Time of Departure",
            "Origin", "Destination",
            "Fare Price", "Fees & Charges", "Base+fees charges",
            "Fare Class", "Seats Available", "Availability", "Source",
        ]
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.results)
        logger.info(f"Saved {len(self.results)} rows to {filename}")


async def scrape_nexus(
    selected_routes=None,
    days_out=84,
    headless=True,
    progress_callback=None,
    output_dir=OUTPUT_DIR,
    stop_requested=None,
) -> dict:
    routes = selected_routes or list(ROUTES)
    scraper = NexusScraper(
        headless=headless,
        progress_callback=progress_callback,
        stop_requested=stop_requested,
    )
    await scraper.scrape_all(routes, days=days_out)

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%d-%m-%Y_%I-%M%p")
    csv_path = output_dir / f"Nexus_Fare_Tracker_{stamp}.csv"
    scraper.save_to_csv(csv_path)

    return {
        "rows": scraper.results,
        "csv_path": str(csv_path),
        "xlsx_path": None,
    }


async def main():
    routes = list(ROUTES)
    scraper = NexusScraper(headless=True)
    logger.info(f"Starting full scrape: {len(routes)} routes × 84 days")

    try:
        await scraper.scrape_all(routes, days=84)
    except Exception as e:
        logger.critical(f"Scrape aborted with unhandled error: {e}", exc_info=True)
        sys.exit(1)

    stamp = datetime.now().strftime("%d-%m-%Y_%I-%M%p")
    csv_path = OUTPUT_DIR / f"Nexus_Fare_Tracker_{stamp}.csv"
    scraper.save_to_csv(str(csv_path))
    logger.info("Scrape complete.")


if __name__ == "__main__":
    asyncio.run(main())
