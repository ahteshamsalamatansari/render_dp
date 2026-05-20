"""
Render Cron Job Runner — Sequential Scraper + Email Report
==========================================================
Runs all 4 airline scrapers one-by-one, emails output files after each
airline completes (not at the end), and retries on connection errors.

Usage:
    python cron_runner.py              # Run all scrapers + email
    python cron_runner.py --dry-run    # Skip scrapers, just email any existing output files

Environment variables (set in Render dashboard):
    EMAIL_FROM      — sender Gmail address
    EMAIL_PASSWORD  — Gmail App Password
    EMAIL_TO        — recipient address
"""

import os
import sys
import time
import smtplib
import subprocess
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

# ── Configuration ───────────────────────────────────────

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

EMAIL_FROM = os.getenv("EMAIL_FROM", "ahteshamsalamat@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "ahteshamansari@bizprospex.com")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Retry settings for connection errors
MAX_RETRIES = 3
RETRY_DELAY_S = 60
RETRY_ERRORS = ("Connection aborted.", "RemoteDisconnected")

# ── Scraper definitions ────────────────────────────────

SCRAPERS = [
    {
        "name": "Qantas",
        "command": ["python", "Qantas_4Zones_Deliver_13_05_2026_FixedU.py", "--workers", "1"],
        "routes": [
            "BME → KNX (Broome → Kununurra)",
            "BME → DRW (Broome → Darwin)",
            "DRW → KNX (Darwin → Kununurra)",
            "KNX → BME (Kununurra → Broome)",
        ],
    },
    {
        "name": "Airnorth",
        "command": ["python", "airnorth_brightdata_Main.py", "--all", "--workers", "1"],
        "routes": [
            "BME → KNX (Broome → Kununurra)",
            "BME → DRW (Broome → Darwin)",
            "DRW → KNX (Darwin → Kununurra)",
            "KNX → BME (Kununurra → Broome)",
        ],
    },
    {
        "name": "Nexus Airlines",
        "command": ["python", "scrape_nexus_final.py"],
        "routes": [
            "PER → GET (Perth → Geraldton)",
            "GET → PER (Geraldton → Perth)",
            "PER → BME (Perth → Broome)",
            "BME → PER (Broome → Perth)",
            "KTA → BME (Karratha → Broome)",
            "BME → KTA (Broome → Karratha)",
            "PHE → BME (Port Hedland → Broome)",
            "BME → PHE (Broome → Port Hedland)",
            "GET → BME (Geraldton → Broome)",
            "BME → GET (Broome → Geraldton)",
        ],
    },
    {
        "name": "Rex Airlines",
        "command": [
            "python", "rex_brightdata.py",
            "--skip-unblocker-check",
            "--output", "output/rex_results_all_routes.xlsx",
        ],
        "routes": [
            "PER → ALH (Perth → Albany)",
            "ALH → PER (Albany → Perth)",
            "PER → EPR (Perth → Esperance)",
            "EPR → PER (Esperance → Perth)",
            "PER → CVQ (Perth → Carnarvon)",
            "CVQ → PER (Carnarvon → Perth)",
            "PER → MJK (Perth → Monkey Mia)",
            "MJK → PER (Monkey Mia → Perth)",
            "CVQ → MJK (Carnarvon → Monkey Mia)",
            "MJK → CVQ (Monkey Mia → Carnarvon)",
        ],
    },
]


# ── Helpers ─────────────────────────────────────────────

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def stream_process(cmd: list, env: dict, timeout: float) -> tuple[int, str]:
    """Run a subprocess, stream its output in real-time, and return (returncode, full_output)."""
    output_lines: list[str] = []

    proc = subprocess.Popen(
        cmd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )

    def _reader():
        for line in proc.stdout:
            print(line, end="", flush=True)
            output_lines.append(line)

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        reader_thread.join(timeout=5)
        raise

    reader_thread.join(timeout=5)
    return proc.returncode, "".join(output_lines)


def collect_output_files_since(since_ts: float) -> list[Path]:
    """Collect CSV/XLSX files from output/ modified at or after since_ts (epoch seconds)."""
    files = []
    if not OUTPUT_DIR.exists():
        return files
    for item in OUTPUT_DIR.rglob("*"):
        if item.is_file() and item.suffix.lower() in (".csv", ".xlsx"):
            if item.stat().st_mtime >= since_ts:
                files.append(item)
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files


def collect_output_files(since_hours: float = 8.0) -> list[Path]:
    return collect_output_files_since(time.time() - since_hours * 3600)


def build_email_body(results: list[dict], files: list[Path]) -> str:
    today = datetime.now().strftime("%A, %d %B %Y")
    lines = [
        f"Flight Scraper Daily Report — {today}",
        "=" * 55,
        "",
    ]

    for r in results:
        status_icon = "✅" if r["success"] else "❌"
        lines.append(f"{status_icon}  {r['name']}")
        lines.append(f"    Status   : {'Completed' if r['success'] else 'FAILED'}")
        lines.append(f"    Duration : {r['duration']}")
        lines.append(f"    Routes   :")
        for route in r["routes"]:
            lines.append(f"      • {route}")
        lines.append("")

    lines.append("-" * 55)
    if files:
        lines.append(f"📎 Attached files ({len(files)}):")
        for f in files:
            size_kb = f.stat().st_size / 1024
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%H:%M:%S")
            rel = f.relative_to(OUTPUT_DIR) if str(f).startswith(str(OUTPUT_DIR)) else f.name
            lines.append(f"  • {rel}  ({size_kb:.1f} KB, {mtime})")
    else:
        lines.append("⚠️  No output files were generated during this run.")

    lines.append("")
    lines.append("-" * 55)
    total_ok = sum(1 for r in results if r["success"])
    total = len(results)
    lines.append(f"Summary: {total_ok}/{total} scrapers completed successfully.")
    lines.append("")

    return "\n".join(lines)


def send_email(results: list[dict], files: list[Path]) -> None:
    if not EMAIL_PASSWORD:
        log("⚠️  EMAIL_PASSWORD not set — skipping email.")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    airline_names = ", ".join(r["name"] for r in results)
    total_ok = sum(1 for r in results if r["success"])
    total = len(results)
    subject = f"Flight Scraper Report — {today} — {total_ok}/{total} OK — {airline_names}"

    body = build_email_body(results, files)

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    for filepath in files:
        try:
            with open(filepath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            rel = filepath.relative_to(OUTPUT_DIR) if str(filepath).startswith(str(OUTPUT_DIR)) else filepath.name
            safe_name = str(rel).replace("\\", "/").replace("/", "_")
            part.add_header("Content-Disposition", f"attachment; filename=\"{safe_name}\"")
            msg.attach(part)
        except Exception as e:
            log(f"⚠️  Could not attach {filepath}: {e}")

    log(f"📧 Sending email to {EMAIL_TO} ({len(files)} attachments)...")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg)
        log("✅ Email sent successfully!")
    except Exception as e:
        log(f"❌ Email failed: {e}")


# ── Main runner ─────────────────────────────────────────

def run_all_scrapers() -> list[dict]:
    """Run each scraper sequentially, retry on connection errors, email after each."""
    results = []

    for scraper in SCRAPERS:
        name = scraper["name"]
        cmd = scraper["command"]
        routes = scraper["routes"]

        log(f"{'━' * 55}")
        log(f"🚀 Starting {name} scraper...")
        log(f"   Command: {' '.join(cmd)}")
        log(f"   Routes: {len(routes)}")
        for route in routes:
            log(f"     • {route}")
        log("")

        scraper_start = time.time()
        success = False
        exit_code = -1
        duration = "0s"

        for attempt in range(1, MAX_RETRIES + 1):
            if attempt > 1:
                log(f"⟳  [{name}] Retry {attempt}/{MAX_RETRIES} — waiting {RETRY_DELAY_S}s before next attempt...")
                time.sleep(RETRY_DELAY_S)

            try:
                env = os.environ.copy()
                env["PYTHONUNBUFFERED"] = "1"
                env["TZ"] = "Australia/Perth"

                returncode, output = stream_process(cmd, env, timeout=14400)
                elapsed = time.time() - scraper_start
                duration = format_duration(elapsed)
                exit_code = returncode
                success = returncode == 0

                if success:
                    log(f"✅ {name} completed in {duration}")
                    break

                log(f"❌ {name} failed (exit code {returncode}) after {duration}")

                if any(err in output for err in RETRY_ERRORS):
                    log(f"   ↳ Connection error detected — will retry.")
                    if attempt < MAX_RETRIES:
                        continue
                # Non-connection failure or out of retries — stop
                break

            except subprocess.TimeoutExpired:
                elapsed = time.time() - scraper_start
                duration = format_duration(elapsed)
                log(f"⏰ {name} timed out after {duration}")
                exit_code = -1
                success = False
                break

            except Exception as e:
                elapsed = time.time() - scraper_start
                duration = format_duration(elapsed)
                log(f"💥 {name} crashed: {e}")
                exit_code = -1
                success = False
                break

        result = {
            "name": name,
            "success": success,
            "exit_code": exit_code,
            "duration": duration,
            "routes": routes,
        }
        results.append(result)

        # Email immediately after this airline finishes
        files = collect_output_files_since(scraper_start)
        log(f"\n📁 Found {len(files)} output file(s) for {name}.")
        for f in files:
            log(f"   • {f}")
        send_email([result], files)

    return results


def format_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Cron runner: sequential scrapers + email per airline")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Skip scrapers — just collect existing output files and send email",
    )
    args = parser.parse_args()

    log("=" * 55)
    log("🗓️  Flight Scraper Cron Job")
    log(f"   Date     : {datetime.now().strftime('%A, %d %B %Y %H:%M %Z')}")
    log(f"   Mode     : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    log(f"   Email to : {EMAIL_TO}")
    log(f"   Scrapers : {len(SCRAPERS)}")
    log("=" * 55)
    log("")

    if args.dry_run:
        log("🔸 Dry run — skipping scrapers, emailing existing files.")
        files = collect_output_files(since_hours=8.0)
        results = [
            {
                "name": s["name"],
                "success": True,
                "exit_code": 0,
                "duration": "dry-run",
                "routes": s["routes"],
            }
            for s in SCRAPERS
        ]
        log(f"\n📁 Found {len(files)} output file(s) from the last 8 hours.")
        for f in files:
            log(f"   • {f}")
        send_email(results, files)
    else:
        results = run_all_scrapers()

    # Final summary
    log("")
    log("=" * 55)
    total_ok = sum(1 for r in results if r["success"])
    total = len(results)
    log(f"🏁 Done — {total_ok}/{total} scrapers completed successfully.")
    log("=" * 55)

    if total_ok < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
