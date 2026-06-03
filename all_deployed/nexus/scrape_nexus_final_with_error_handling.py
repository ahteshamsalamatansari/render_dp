"""
Nexus Airlines fare scraper — direct-API edition.

Architecture
------------
Discovered from HAR analysis: the booking site has NO Cloudflare Turnstile,
just standard CDN. The actual data comes from a single POST:

    POST https://secure.nexusairlines.com.au/Ajax/Search/Flights/
    Body: From=Perth&to=Geraldton&departDate=YYYY-MM-DD&adults=1&...
    Response: JSON with all fares

So instead of navigating a full page per date (which suffered random 45s
DOM-load timeouts), we open ONE browser via Bright Data, warm the session
cookies once, then make 840 direct POSTs. Each call is sub-second and
returns JSON directly — no page rendering, no DOM events to stall on.

Reliability layers for unattended cron use
-------------------------------------------
1. Per-date retries (3) with exponential backoff
2. Automatic session rewarm if server returns HTML/302 (cookies expired)
3. Proactive browser reconnect at 55 min (avoids Bright Data's 60-min kill)
4. Full backfill pass at end with a fresh session
5. Final report listing any permanently-missed dates (should be zero)
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from playwright.async_api import async_playwright

# ---------------------------------------------------------------------------
# Logging — timestamped lines for cron log readability
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Routes & airport->city mapping
# Discovered from HAR: the Ajax endpoint requires FULL city names, not IATA.
# ---------------------------------------------------------------------------
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

CITY_MAP = {
    "PER": "Perth",
    "GET": "Geraldton",
    "BME": "Broome",
    "KTA": "Karratha",
    "PHE": "Port Hedland",
}

# ---------------------------------------------------------------------------
# Tuning
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2          # backoff: 2s -> 4s -> 8s
SESSION_MAX_SECONDS = 55 * 60 # reconnect before Bright Data's 60-min kill
API_TIMEOUT_MS = 30000        # 30s for in-page fetch (normally <1s)
WARMUP_TIMEOUT_MS = 90000     # 90s — gives Bright Data plenty of time to solve CF challenge
INTER_CALL_DELAY_S = 0.4      # small gap between calls to look human
CONSECUTIVE_FAIL_RECONNECT = 3  # do full IP rotation after this many in a row
CONSECUTIVE_FAIL_ABANDON = 7    # give up on route after this many in a row

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)


class SessionExpiredError(Exception):
    """API returned non-JSON — cookies expired, IP blocked, or backend down."""


class NexusScraper:
    def __init__(self, headless=True, progress_callback=None, stop_requested=None):
        self.headless = headless
        self.results: list[dict] = []
        self.missed: list[tuple] = []     # (origin, dest, target_date)
        self.progress_callback = progress_callback
        self.stop_requested = stop_requested

        self._browser = None
        self._context = None
        self._warmup_page = None
        self._bd_browser_ws: str | None = None
        self._p = None
        self._session_start_time: float = 0.0
        self._last_warmed: tuple[str, str] | None = None

    def should_stop(self) -> bool:
        if not self.stop_requested:
            return False
        try:
            return bool(self.stop_requested())
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Proxy config (unchanged from before)
    # ------------------------------------------------------------------
    def get_proxy_config(self):
        bd_proxy_host = os.environ.get("BRIGHTDATA_PROXY_HOST")
        bd_proxy_user = os.environ.get("BRIGHTDATA_PROXY_USER")
        bd_proxy_pass = os.environ.get("BRIGHTDATA_PROXY_PASS")
        if bd_proxy_host and bd_proxy_user and bd_proxy_pass:
            logger.info("Routing traffic via Bright Data Residential Proxy...")
            return {"server": f"http://{bd_proxy_host}", "username": bd_proxy_user, "password": bd_proxy_pass}

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
            return {"server": "http://proxy.zenrows.com:8001", "username": api_key, "password": "js_render=true&premium_proxy=true"}
        elif provider == "scrapfly":
            return {"server": "http://proxy.scrapfly.io:80", "username": api_key, "password": "asp=true&render_js=true"}
        elif provider == "scrapeops":
            return {"server": "http://proxy.scrapeops.io:80", "username": api_key, "password": "bypass=cloudflare"}
        return None

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------
    async def _connect_browser(self):
        """Connect to Bright Data (or launch local). Resets session timer."""
        if self._bd_browser_ws:
            logger.info("Connecting to Bright Data Scraping Browser via CDP...")
            self._browser = await self._p.chromium.connect_over_cdp(self._bd_browser_ws)
            self._context = await self._browser.new_context()
        else:
            proxy_config = self.get_proxy_config()
            launch_kwargs = {
                "headless": self.headless,
                "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            }
            if proxy_config:
                launch_kwargs["proxy"] = proxy_config
            self._browser = await self._p.chromium.launch(**launch_kwargs)
            ctx_kwargs = {"proxy": proxy_config} if proxy_config else {}
            self._context = await self._browser.new_context(**ctx_kwargs)

        self._session_start_time = time.monotonic()
        self._warmup_page = None

    async def _warmup_session(self, origin: str, dest: str) -> bool:
        """
        Warm up for a SPECIFIC route. ASP.NET session state is bound to the
        route from the /Booking/Search redirect — calls for any other route
        will return 403. So we call this once per route.

        Returns True if the page's own initial AJAX call completed with 200,
        which proves the full session (CF + ASP.NET + cookies) is alive.
        """
        logger.info(f"Warming up session for {origin}->{dest}...")
        try:
            # Close any existing warmup page
            if self._warmup_page is not None:
                try:
                    if not self._warmup_page.is_closed():
                        await self._warmup_page.close()
                except Exception:
                    pass
            self._warmup_page = await self._context.new_page()

            # Listen for the page's own auto-fired AJAX call. When it succeeds,
            # we know the server-side session is live for THIS route.
            initial_ajax_status: list = [None]
            initial_ajax_event = asyncio.Event()

            def on_response(response):
                if "Ajax/Search/Flights" in response.url:
                    initial_ajax_status[0] = response.status
                    initial_ajax_event.set()

            self._warmup_page.on("response", on_response)

            # Build the route-specific search URL with a date in the next week
            sample_date = (datetime.now() + timedelta(days=7)).strftime("%d/%m/%Y")
            encoded = quote(sample_date, safe="")
            search_url = (
                f"https://secure.nexusairlines.com.au/Booking/Search"
                f"?From={origin}&To={dest}&Depart={encoded}"
                f"&Adults=1&Children=0&Infants=0"
            )

            try:
                await self._warmup_page.goto(
                    search_url,
                    wait_until="domcontentloaded",
                    timeout=WARMUP_TIMEOUT_MS,
                )
            except Exception as nav_err:
                logger.warning(f"  Warmup nav timed out (will still verify): {nav_err}")

            # Wait up to 30s for the page's initial AJAX to complete with 200.
            try:
                await asyncio.wait_for(initial_ajax_event.wait(), timeout=30)
                status = initial_ajax_status[0]
                if status == 200:
                    logger.info(f"  ✓ Session live for {origin}->{dest} (initial AJAX = 200)")
                    self._last_warmed = (origin, dest)
                    return True
                else:
                    logger.warning(f"  ✗ Initial AJAX returned {status} for {origin}->{dest}")
                    return False
            except asyncio.TimeoutError:
                logger.warning(f"  ✗ Page never auto-fired AJAX for {origin}->{dest}")
                return False
        except Exception as e:
            logger.warning(f"  Warmup error: {type(e).__name__}: {e}")
            return False

    async def _full_reconnect(self):
        """
        Tear down browser entirely and reconnect — gets a new Bright Data
        session (and usually a different residential IP). Use this when the
        current IP has been soft-blocked by IIS (repeated 403s).
        """
        logger.info("Forcing FULL browser reconnect (new Bright Data IP)...")
        try:
            await self._browser.close()
        except Exception:
            pass
        await self._connect_browser()

    async def _maybe_proactive_reconnect(self):
        """Reconnect before Bright Data's 60-minute hard session kill."""
        elapsed = time.monotonic() - self._session_start_time
        if elapsed >= SESSION_MAX_SECONDS:
            logger.info(f"Proactive session refresh (elapsed {elapsed/60:.1f} min)...")
            try:
                await self._browser.close()
            except Exception:
                pass
            try:
                await self._connect_browser()
                if self._last_warmed is not None:
                    o, d = self._last_warmed
                    await self._warmup_session(o, d)
                logger.info("  Session refreshed.")
            except Exception as e:
                logger.error(f"  Proactive reconnect failed: {e}. Continuing with stale session.")

    # ------------------------------------------------------------------
    # The actual API call — replaces page.goto entirely
    # ------------------------------------------------------------------
    async def _api_call(self, origin: str, dest: str, target_date: datetime) -> dict:
        """
        Run the POST FROM INSIDE the warmed page via fetch().  This is the
        critical bit:
          - `context.request.post()` is a raw HTTP call with a non-browser
            TLS fingerprint → Cloudflare challenges it (HTTP 403 "Just a
            moment...").
          - `page.evaluate(fetch...)` runs inside the real browser page,
            with the real browser's TLS, real cookies (including
            cf_clearance), and same-origin context → Cloudflare passes it.
        """
        if self._warmup_page is None or self._warmup_page.is_closed():
            raise SessionExpiredError("Warmup page is not available")

        from_city = CITY_MAP[origin]
        to_city = CITY_MAP[dest]
        date_iso = target_date.strftime("%Y-%m-%d")

        # JS payload runs inside the page.  The browser fills in cookies,
        # referer, origin, sec-* headers automatically.
        js = """
        async ({fromCity, toCity, dateIso, timeoutMs}) => {
            const params = new URLSearchParams();
            params.append('From', fromCity);
            params.append('to', toCity);
            params.append('departDate', dateIso);
            params.append('adults', '1');
            params.append('children', '0');
            params.append('infants', '0');
            params.append('CustomPassengers', '0');
            params.append('packageID', '0');
            params.append('AgentID', '');
            params.append('Coupon', '');
            params.append('GiftVoucher', '');
            params.append('AircraftType', '');
            params.append('FareClassValidationEligibility', 'false');

            const ctrl = new AbortController();
            const timer = setTimeout(() => ctrl.abort(), timeoutMs);
            try {
                const r = await fetch('/Ajax/Search/Flights/', {
                    method: 'POST',
                    body: params,
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Accept': '*/*',
                    },
                    credentials: 'include',
                    signal: ctrl.signal,
                });
                const text = await r.text();
                return { status: r.status, body: text };
            } finally {
                clearTimeout(timer);
            }
        }
        """
        try:
            result = await asyncio.wait_for(
                self._warmup_page.evaluate(
                    js,
                    {
                        "fromCity": from_city,
                        "toCity": to_city,
                        "dateIso": date_iso,
                        "timeoutMs": API_TIMEOUT_MS,
                    },
                ),
                timeout=(API_TIMEOUT_MS / 1000) + 5,
            )
        except asyncio.TimeoutError:
            raise SessionExpiredError("fetch() hard timeout")

        status = result.get("status")
        body_text = result.get("body", "")
        body_stripped = body_text.lstrip()

        if status != 200 or not body_stripped.startswith("{"):
            preview = body_text[:120].replace("\n", " ")
            raise SessionExpiredError(f"HTTP {status}, body preview: {preview!r}")

        try:
            return json.loads(body_text)
        except json.JSONDecodeError as e:
            raise SessionExpiredError(f"JSON decode failed: {e}")

    # ------------------------------------------------------------------
    # Per-date retry wrapper
    # ------------------------------------------------------------------
    async def _scrape_date(self, origin: str, dest: str, target_date: datetime) -> tuple[bool, int]:
        """
        Try one date with up to MAX_RETRIES quick retries on the warmed page.
        Returns (api_ok, rows_added). api_ok is True if the API answered with
        valid JSON; rows_added is the number of fare rows extracted (can be 0
        even when api_ok is True — e.g. empty Outgoing or empty Fares arrays).
        """
        date_str = target_date.strftime("%d/%m/%Y")

        for attempt in range(MAX_RETRIES):
            if self.should_stop():
                return False, 0
            try:
                data = await self._api_call(origin, dest, target_date)
                rows_added, prices = self.parse_json(data, target_date, origin, dest)
                if rows_added > 0:
                    lo, hi = min(prices), max(prices)
                    if lo == hi:
                        logger.info(f"      {origin}->{dest} {date_str}: {rows_added} fare(s) @ ${lo:.0f}")
                    else:
                        logger.info(f"      {origin}->{dest} {date_str}: {rows_added} fare(s), ${lo:.0f}-${hi:.0f}")
                else:
                    # API said 200/JSON but no rows — surface it so silent empties stop slipping through
                    keys = list(data.keys()) if isinstance(data, dict) else []
                    outgoing = data.get("Outgoing", []) if isinstance(data, dict) else []
                    logger.warning(
                        f"      {origin}->{dest} {date_str}: API ok but 0 rows "
                        f"(Outgoing={len(outgoing)}, top-level keys={keys})"
                    )
                return True, rows_added
            except Exception as e:
                kind = type(e).__name__
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning(
                        f"    Attempt {attempt + 1}/{MAX_RETRIES} for {date_str}: {kind}: {e}. "
                        f"Retrying in {wait}s..."
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.warning(
                        f"    Attempt {attempt + 1}/{MAX_RETRIES} for {date_str}: {kind}: {e}"
                    )

        return False, 0

    # ------------------------------------------------------------------
    # Main scrape orchestrator
    # ------------------------------------------------------------------
    async def scrape_all(self, routes, days: int = 84):
        total = max(1, len(routes) * days)
        completed = 0

        self._bd_browser_ws = os.environ.get(
            "BRIGHTDATA_BROWSER_WS",
            "wss://brd-customer-hl_fbc4a16a-zone-cron_nexus:0td7q08n8f70@brd.superproxy.io:9222",
        )

        async with async_playwright() as p:
            self._p = p
            await self._connect_browser()

            start_date = datetime.now() + timedelta(days=1)

            # =========================================================
            # MAIN PASS — for each route: 1 warmup, then ~840 fetches
            # =========================================================
            for origin, dest in routes:
                if self.should_stop():
                    break

                logger.info(f"=== Scraping route: {origin} -> {dest} ===")
                await self._maybe_proactive_reconnect()

                # Warm up for THIS route. Try up to 3 times (with a full
                # reconnect between attempts if the warmup itself fails).
                warmup_ok = False
                for warm_attempt in range(3):
                    warmup_ok = await self._warmup_session(origin, dest)
                    if warmup_ok:
                        break
                    if warm_attempt < 2:
                        logger.warning(
                            f"  Warmup attempt {warm_attempt + 1}/3 failed for {origin}->{dest}; "
                            f"reconnecting and retrying..."
                        )
                        await self._full_reconnect()
                        await asyncio.sleep(3)

                if not warmup_ok:
                    logger.error(
                        f"  Could not warm session for {origin}->{dest} after 3 tries. "
                        f"Queuing all {days} dates for backfill."
                    )
                    for i in range(days):
                        self.missed.append((origin, dest, start_date + timedelta(days=i)))
                    completed += days
                    continue

                # ---------- Date loop for this route ----------
                route_captured = 0
                route_rows_added = 0
                route_empty_dates = 0
                consecutive_fails = 0

                for i in range(days):
                    if self.should_stop():
                        break

                    # Abandon route after too many consecutive failures
                    if consecutive_fails >= CONSECUTIVE_FAIL_ABANDON:
                        remaining = days - i
                        logger.error(
                            f"  {consecutive_fails} consecutive failures on {origin}->{dest}. "
                            f"Queuing remaining {remaining} dates for backfill."
                        )
                        for j in range(i, days):
                            self.missed.append((origin, dest, start_date + timedelta(days=j)))
                        completed += remaining
                        break

                    await self._maybe_proactive_reconnect()

                    target_date = start_date + timedelta(days=i)
                    date_str = target_date.strftime("%d/%m/%Y")
                    logger.info(f"  - Date: {date_str}")

                    if self.progress_callback:
                        self.progress_callback(
                            completed, total,
                            f"Nexus {origin}->{dest} {target_date.strftime('%Y-%m-%d')}",
                        )

                    success, rows_added = await self._scrape_date(origin, dest, target_date)
                    if success:
                        route_captured += 1
                        route_rows_added += rows_added
                        if rows_added == 0:
                            route_empty_dates += 1
                        consecutive_fails = 0
                    else:
                        consecutive_fails += 1
                        self.missed.append((origin, dest, target_date))
                        logger.error(f"    ✗ Date failed — queued for backfill")

                        # Mid-route recovery: after N consecutive fails, do a
                        # full IP rotation + re-warm. This handles soft-blocks
                        # from the IIS layer / IP-level rate limiting.
                        if consecutive_fails >= CONSECUTIVE_FAIL_RECONNECT:
                            logger.warning(
                                f"  {consecutive_fails} consecutive failures — "
                                f"rotating IP and re-warming..."
                            )
                            await self._full_reconnect()
                            await asyncio.sleep(5)
                            ok = await self._warmup_session(origin, dest)
                            if not ok:
                                logger.warning("  Re-warm after IP rotation failed; will keep trying anyway.")

                    # Tiny gap between calls — looks more human, eases load
                    await asyncio.sleep(INTER_CALL_DELAY_S)

                    completed += 1
                    if self.progress_callback:
                        self.progress_callback(
                            completed, total,
                            f"Nexus {origin}->{dest} {target_date.strftime('%Y-%m-%d')} complete",
                        )

                logger.info(
                    f"Route {origin}->{dest} complete: {route_captured}/{days} dates ok, "
                    f"{route_rows_added} fare rows extracted, {route_empty_dates} date(s) had 0 rows "
                    f"({len(self.missed)} in backfill queue so far)"
                )
                if route_rows_added == 0:
                    logger.error(
                        f"  ⚠  Route {origin}->{dest} produced ZERO fare rows even though "
                        f"{route_captured} API call(s) succeeded — check warmup / response shape."
                    )
                self.save_to_csv("nexus_partial_latest.csv")

            # =========================================================
            # BACKFILL PASS — fresh IP per route, re-warm per route
            # =========================================================
            if self.missed:
                logger.info("=" * 60)
                logger.info(f"BACKFILL PASS — {len(self.missed)} dates to retry")
                logger.info("=" * 60)

                # Fresh browser session for backfill
                await self._full_reconnect()

                # Group missed dates by route so we warm up ONCE per route
                from collections import defaultdict
                by_route: dict = defaultdict(list)
                for origin, dest, td in self.missed:
                    by_route[(origin, dest)].append(td)
                self.missed.clear()
                recovered = 0
                total_to_retry = sum(len(d) for d in by_route.values())
                done = 0

                for (origin, dest), dates in by_route.items():
                    if self.should_stop():
                        break

                    logger.info(f"  Backfill route {origin}->{dest} ({len(dates)} dates)")

                    # Warm up for this route with up to 2 attempts
                    ok = await self._warmup_session(origin, dest)
                    if not ok:
                        await self._full_reconnect()
                        await asyncio.sleep(3)
                        ok = await self._warmup_session(origin, dest)

                    if not ok:
                        logger.error(f"    Backfill warmup failed for {origin}->{dest} — skipping")
                        for td in dates:
                            self.missed.append((origin, dest, td))
                            done += 1
                        continue

                    for td in dates:
                        if self.should_stop():
                            break
                        done += 1
                        await self._maybe_proactive_reconnect()
                        date_str = td.strftime("%d/%m/%Y")
                        logger.info(f"    Backfill [{done}/{total_to_retry}]: {origin}->{dest} {date_str}")

                        success, rows_added = await self._scrape_date(origin, dest, td)
                        if success:
                            recovered += 1
                            logger.info(f"      ✓ Recovered ({rows_added} fare row(s))")
                        else:
                            self.missed.append((origin, dest, td))
                            logger.error(f"      ✗ Still failing — permanently missed")

                        await asyncio.sleep(INTER_CALL_DELAY_S)

                logger.info("=" * 60)
                logger.info(f"BACKFILL COMPLETE — recovered {recovered}/{total_to_retry}")
                if self.missed:
                    logger.error(f"⚠️  Permanently missed {len(self.missed)} date(s):")
                    for origin, dest, td in self.missed:
                        logger.error(f"    {origin}->{dest} {td.strftime('%d/%m/%Y')}")
                else:
                    logger.info("✓ Zero permanently-missed dates.")
                logger.info("=" * 60)
                self.save_to_csv("nexus_partial_latest.csv")
            else:
                logger.info("Main pass captured everything — no backfill needed.")

            try:
                await self._browser.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # JSON -> rows
    # ------------------------------------------------------------------
    def parse_json(self, data: dict, date: datetime, origin: str, dest: str) -> tuple[int, list[float]]:
        """Append fare rows; return (rows_added, prices_added)."""
        outgoing = data.get("Outgoing", [])
        if not outgoing:
            return 0, []

        target_date_str = date.strftime("%Y-%m-%d")
        rows_added = 0
        prices: list[float] = []
        flights_with_empty_fares = 0

        for flight in outgoing:
            dep_time_iso = flight.get("DepartsLocalISO8601") or ""
            time_str = dep_time_iso.split("T")[1][:5] if "T" in dep_time_iso else ""

            flight_date_str = dep_time_iso.split("T")[0] if "T" in dep_time_iso else ""
            if flight_date_str and flight_date_str != target_date_str:
                continue  # safety: skip flights from other dates in the response window

            fares = flight.get("AdvancedFares", []) or flight.get("Fares", [])
            if not fares:
                flights_with_empty_fares += 1
                continue
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
                    "Date of Departure": target_date_str,
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
                rows_added += 1
                try:
                    prices.append(float(price))
                except (TypeError, ValueError):
                    pass

        if rows_added == 0 and flights_with_empty_fares > 0:
            logger.warning(
                f"      {origin}->{dest} {target_date_str}: "
                f"{flights_with_empty_fares} flight(s) returned with EMPTY fare list"
            )

        return rows_added, prices

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


# ---------------------------------------------------------------------------
# Public entry points (unchanged signatures, so the cron wrapper still works)
# ---------------------------------------------------------------------------
async def scrape_nexus(
    selected_routes=None,
    days_out: int = 84,
    headless: bool = True,
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
        "missed": [(o, d, td.strftime("%Y-%m-%d")) for o, d, td in scraper.missed],
    }


async def main():
    routes = list(ROUTES)
    scraper = NexusScraper(headless=True)
    logger.info(f"Starting full scrape: {len(routes)} routes × 84 days = {len(routes) * 84} date queries")

    try:
        await scraper.scrape_all(routes, days=84)
    except Exception as e:
        logger.critical(f"Scrape aborted with unhandled error: {e}", exc_info=True)
        sys.exit(1)

    stamp = datetime.now().strftime("%d-%m-%Y_%I-%M%p")
    csv_path = OUTPUT_DIR / f"Nexus_Fare_Tracker_{stamp}.csv"
    scraper.save_to_csv(str(csv_path))

    # Per-route row count summary — surfaces routes that captured 0 rows.
    from collections import Counter
    by_route = Counter((r["Origin"], r["Destination"]) for r in scraper.results)
    logger.info("=" * 60)
    logger.info("Per-route row counts:")
    empty_routes = []
    for orig, dest in routes:
        n = by_route.get((orig, dest), 0)
        flag = "  " if n > 0 else "⚠ "
        logger.info(f"  {flag}{orig}->{dest}: {n} rows")
        if n == 0:
            empty_routes.append((orig, dest))
    logger.info("=" * 60)

    # Final summary for cron log
    exit_code = 0
    if scraper.missed:
        logger.error(f"⚠️  Scrape finished with {len(scraper.missed)} permanently-missed date(s).")
        exit_code = 2
    if empty_routes:
        logger.error(
            f"⚠️  {len(empty_routes)} route(s) produced ZERO rows: "
            + ", ".join(f"{o}->{d}" for o, d in empty_routes)
        )
        exit_code = max(exit_code, 3)
    if exit_code == 0:
        logger.info("✅ Scrape complete — 100% capture rate.")
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())
