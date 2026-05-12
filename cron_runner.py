"""
Render Cron Job Runner — Sequential Scraper + Email Report
==========================================================
Runs all 4 airline scrapers one-by-one, then emails the generated
CSV/XLSX files to the configured recipient.

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
from datetime import datetime, timedelta
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

# ── Scraper definitions ────────────────────────────────

SCRAPERS = [
    {
        "name": "Qantas",
        "command": ["python", "qantas_production_4_Zones.py", "--workers", "1"],
        "routes": [
            "BME → KNX (Broome → Kununurra)",
            "BME → DRW (Broome → Darwin)",
            "DRW → KNX (Darwin → Kununurra)",
            "KNX → BME (Kununurra → Broome)",
        ],
    },
    {
        "name": "Airnorth",
        "command": ["python", "airnorth_fast_async.py", "--all", "--workers", "1"],
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


def collect_output_files(since_hours: float = 8.0) -> list[Path]:
    """
    Collect all CSV/XLSX files from output/ that were modified
    within the last `since_hours` hours.
    """
    cutoff = time.time() - (since_hours * 3600)
    files = []

    if not OUTPUT_DIR.exists():
        return files

    for item in OUTPUT_DIR.rglob("*"):
        if item.is_file() and item.suffix.lower() in (".csv", ".xlsx"):
            if item.stat().st_mtime >= cutoff:
                files.append(item)

    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files


def build_email_body(results: list[dict], files: list[Path]) -> str:
    """Build a detailed plain-text email body with airline names and routes."""
    today = datetime.now().strftime("%A, %d %B %Y")
    lines = [
        f"Flight Scraper Daily Report — {today}",
        "=" * 55,
        "",
    ]

    # Per-scraper summary
    for r in results:
        status_icon = "✅" if r["success"] else "❌"
        lines.append(f"{status_icon}  {r['name']}")
        lines.append(f"    Status   : {'Completed' if r['success'] else 'FAILED'}")
        lines.append(f"    Duration : {r['duration']}")
        lines.append(f"    Routes   :")
        for route in r["routes"]:
            lines.append(f"      • {route}")
        lines.append("")

    # Attached files
    lines.append("-" * 55)
    if files:
        lines.append(f"📎 Attached files ({len(files)}):")
        for f in files:
            size_kb = f.stat().st_size / 1024
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%H:%M:%S")
            # Show relative path from output/
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
    """Send email with all output files attached."""
    if not EMAIL_PASSWORD:
        log("⚠️  EMAIL_PASSWORD not set — skipping email.")
        return

    today = datetime.now().strftime("%Y-%m-%d")

    # Build subject with airline names
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

    # Attach files
    for filepath in files:
        try:
            with open(filepath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            # Use relative path for filename so Airnorth subdirs are clear
            rel = filepath.relative_to(OUTPUT_DIR) if str(filepath).startswith(str(OUTPUT_DIR)) else filepath.name
            safe_name = str(rel).replace("\\", "/").replace("/", "_")
            part.add_header("Content-Disposition", f"attachment; filename=\"{safe_name}\"")
            msg.attach(part)
        except Exception as e:
            log(f"⚠️  Could not attach {filepath}: {e}")

    # Send
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
    """Run each scraper sequentially and collect results."""
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

        start = time.time()
        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            env["TZ"] = "Australia/Perth"

            proc = subprocess.run(
                cmd,
                env=env,
                timeout=14400,  # 4-hour max per scraper
            )

            elapsed = time.time() - start
            duration = format_duration(elapsed)
            success = proc.returncode == 0

            if success:
                log(f"✅ {name} completed in {duration}")
            else:
                log(f"❌ {name} failed (exit code {proc.returncode}) after {duration}")

            results.append({
                "name": name,
                "success": success,
                "exit_code": proc.returncode,
                "duration": duration,
                "routes": routes,
            })

        except subprocess.TimeoutExpired:
            elapsed = time.time() - start
            duration = format_duration(elapsed)
            log(f"⏰ {name} timed out after {duration}")
            results.append({
                "name": name,
                "success": False,
                "exit_code": -1,
                "duration": duration,
                "routes": routes,
            })

        except Exception as e:
            elapsed = time.time() - start
            duration = format_duration(elapsed)
            log(f"💥 {name} crashed: {e}")
            results.append({
                "name": name,
                "success": False,
                "exit_code": -1,
                "duration": duration,
                "routes": routes,
            })

    return results


def format_duration(seconds: float) -> str:
    """Format seconds into a human-readable string."""
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

    parser = argparse.ArgumentParser(description="Cron runner: sequential scrapers + email")
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
        log("🔸 Dry run — skipping scrapers.")
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
    else:
        results = run_all_scrapers()

    # Collect output files
    files = collect_output_files(since_hours=8.0)
    log(f"\n📁 Found {len(files)} output file(s) from the last 8 hours.")
    for f in files:
        log(f"   • {f}")

    # Send email
    send_email(results, files)

    # Final summary
    log("")
    log("=" * 55)
    total_ok = sum(1 for r in results if r["success"])
    total = len(results)
    log(f"🏁 Done — {total_ok}/{total} scrapers completed successfully.")
    log("=" * 55)

    # Exit with error if any scraper failed (so Render marks the cron run as failed)
    if total_ok < total:
        sys.exit(1)


if __name__ == "__main__":
    main()
