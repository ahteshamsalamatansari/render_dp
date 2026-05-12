import asyncio
import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

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

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

class NexusScraper:
    def __init__(self, headless=True, progress_callback=None, stop_requested=None):
        self.headless = headless
        self.results = []
        self.captured_json = None
        self.progress_callback = progress_callback
        self.stop_requested = stop_requested

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
                self.captured_json = await response.json()
            except:
                pass

    async def scrape_all(self, routes, days=84):
        total = max(1, len(routes) * days)
        completed = 0
        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context()
            page = await context.new_page()
            page.on("response", self.handle_response)

            print("Establishing session via homepage...")
            await page.goto("https://nexusairlines.com.au/", wait_until="networkidle")
            await asyncio.sleep(2)

            start_date = datetime.now()

            for origin, dest in routes:
                if self.should_stop():
                    break
                print(f"Scraping route: {origin} -> {dest}")
                for i in range(days):
                    if self.should_stop():
                        break
                    target_date = start_date + timedelta(days=i)
                    date_str = target_date.strftime("%d/%m/%Y")
                    print(f"  - Date: {date_str}")
                    if self.progress_callback:
                        self.progress_callback(
                            completed,
                            total,
                            f"Nexus {origin}->{dest} {target_date.strftime('%Y-%m-%d')}",
                        )

                    self.captured_json = None
                    # ... existing search logic ...
                    
                    # Periodic save every 20 dates
                    if (i + 1) % 20 == 0:
                        self.save_to_csv("nexus_verification_partial.csv")
                    search_url = f"https://secure.nexusairlines.com.au/Booking/Search?From={origin}&To={dest}&Depart={date_str}&Adults=1&Children=0&Infants=0"
                    
                    try:
                        await page.goto(search_url, wait_until="networkidle")
                        await asyncio.sleep(1)
                        
                        # Handle modal if it appears (only once or occasionally)
                        await page.evaluate("const btn = document.querySelector('#nonResidentFare'); if(btn) btn.click();")
                        
                        if "Booking/Search" in page.url:
                            await page.click("#submit", force=True)
                            # Wait for results or timeout
                            for _ in range(10):
                                if self.should_stop():
                                    break
                                if self.captured_json: break
                                await asyncio.sleep(1)
                        else:
                            # Already on Flights page or redirected
                            for _ in range(10):
                                if self.should_stop():
                                    break
                                if self.captured_json: break
                                await asyncio.sleep(1)

                        if self.captured_json:
                            self.parse_json(self.captured_json, target_date, origin, dest)
                        else:
                            # print(f"    No data found for {date_str}")
                            pass

                    except Exception as e:
                        print(f"    Error on {date_str}: {e}")
                    finally:
                        completed += 1
                        if self.progress_callback:
                            self.progress_callback(
                                completed,
                                total,
                                f"Nexus {origin}->{dest} {target_date.strftime('%Y-%m-%d')} complete",
                            )

            await browser.close()

    def parse_json(self, data, date, origin, dest):
        outgoing = data.get("Outgoing", [])
        if not outgoing:
            return

        for flight in outgoing:
            dep_time_iso = flight.get("DepartsLocalISO8601", "")
            # ISO format: 2026-05-15T08:00:00
            time_str = dep_time_iso.split("T")[1][:5] if "T" in dep_time_iso else ""
            
            fares = flight.get("AdvancedFares", [])
            if not fares:
                # Try regular Fares
                fares = flight.get("Fares", [])
            
            if fares:
                for fare in fares:
                    price = fare.get("Adult", 0)
                    fare_class = fare.get("FareClass", "")
                    fare_name = fare.get("DisplayName", "")
                    
                    self.results.append({
                        "Date Checked": datetime.now().strftime("%d/%m/%Y"),
                        "Time Checked": datetime.now().strftime("%H:%M"),
                        "Airline": "Nexus Airlines",
                        "Date of Departure": date.strftime("%Y-%m-%d"),
                        "Time of Departure": time_str,
                        "Origin": origin,
                        "Destination": dest,
                        "Fare Price": price,
                        "Fare Class": f"{fare_name} ({fare_class})",
                        "Source": "https://nexusairlines.com.au/"
                    })
            else:
                # No fares found for this flight?
                pass

    def save_to_csv(self, filename):
        if not self.results:
            print("No results to save.")
            return
        keys = [
            "Date Checked", "Time Checked", "Airline",
            "Date of Departure", "Time of Departure",
            "Origin", "Destination", "Fare Price", "Fare Class", "Source",
        ]
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.results)
        print(f"Saved {len(self.results)} results to {filename}")

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
    # Full list of active routes for Nexus Airlines
    routes = list(ROUTES)
    
    scraper = NexusScraper(headless=True)
    print("Starting full scrape for 84 days across 10 ACTIVE routes...")
    print("This will take some time. Progress will be printed live.")
    
    await scraper.scrape_all(routes, days=84)
    scraper.save_to_csv("nexus_flights_final.csv")

if __name__ == "__main__":
    asyncio.run(main())
