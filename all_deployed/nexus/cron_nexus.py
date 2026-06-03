"""
Cron: Nexus Airlines scraper + email
Runs Nexus scraper, splits combined output into one CSV per route,
counts no-price rows per route, emails per-route files with full breakdown.
"""

import csv
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

EMAIL_FROM     = os.getenv("EMAIL_FROM", "ahteshamsalamat@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO       = os.getenv("EMAIL_TO", "ahteshamansari@bizprospex.com")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

MAX_RETRIES   = 3
RETRY_DELAY_S = 60
RETRY_ERRORS  = ("Connection aborted.", "RemoteDisconnected")

# ── Scraper definition ─────────────────────────────────

SCRAPER_CMD  = ["python", "scrape_nexus_final_with_error_handling.py"]
SCRAPER_NAME = "Nexus Airlines"

AIRPORT_NAMES = {
    "PER": "Perth", "GET": "Geraldton", "BME": "Broome",
    "KTA": "Karratha", "PHE": "Port Hedland",
}

ROUTES = [
    ("PER", "GET"), ("GET", "PER"),
    ("PER", "BME"), ("BME", "PER"),
    ("KTA", "BME"), ("BME", "KTA"),
    ("PHE", "BME"), ("BME", "PHE"),
    ("GET", "BME"), ("BME", "GET"),
]


def route_label(orig: str, dest: str) -> str:
    return (
        f"{orig} -> {dest} "
        f"({AIRPORT_NAMES.get(orig, orig)} -> {AIRPORT_NAMES.get(dest, dest)})"
    )


# ── Helpers ─────────────────────────────────────────────

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def format_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def stream_process(cmd: list, env: dict, timeout: float) -> tuple[int, str]:
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

    t = threading.Thread(target=_reader, daemon=True)
    t.start()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        t.join(timeout=5)
        raise
    t.join(timeout=5)
    return proc.returncode, "".join(output_lines)


def _is_no_price(val) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    if s in ("", "N/A", "No Data", "0", "0.0"):
        return True
    try:
        return float(s) == 0.0
    except ValueError:
        return False


def find_combined_csv_since(since_ts: float) -> "Path | None":
    candidates = [
        p for p in OUTPUT_DIR.rglob("Nexus_Fare_Tracker_*.csv")
        if p.is_file() and p.stat().st_mtime >= since_ts
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def split_combined_csv(combined_csv: Path, stamp: str) -> list[dict]:
    """Read combined CSV, split by route, write per-route CSVs, return stats list."""
    if not combined_csv.exists():
        log(f"  Combined CSV not found: {combined_csv}")
        return []

    route_rows: dict[tuple[str, str], list[dict]] = {}
    fieldnames: list[str] = []

    with open(combined_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            key = (row.get("Origin", ""), row.get("Destination", ""))
            route_rows.setdefault(key, []).append(row)

    stats: list[dict] = []

    for orig, dest in ROUTES:
        rows  = route_rows.get((orig, dest), [])
        total = len(rows)
        label = route_label(orig, dest)

        if total == 0:
            # Don't emit empty header-only CSVs — they were the visible symptom
            # of the silent no-data bug. Record the gap so the email flags it.
            log(f"   {label}: 0 rows — SKIPPING file creation (no data captured)")
            stats.append({
                "orig":          orig,
                "dest":          dest,
                "label":         label,
                "file":          None,
                "total_rows":    0,
                "no_price_rows": 0,
            })
            continue

        fname = f"Nexus_{orig}_{dest}_{stamp}.csv"
        fpath = OUTPUT_DIR / fname

        with open(fpath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        no_price = sum(1 for r in rows if _is_no_price(r.get("Fare Price")))

        log(f"   {label}: {total} rows, {no_price} no-price -> {fname}")

        stats.append({
            "orig":          orig,
            "dest":          dest,
            "label":         label,
            "file":          fpath,
            "total_rows":    total,
            "no_price_rows": no_price,
        })

    return stats


# ── Email ────────────────────────────────────────────────

def build_email_body(result: dict, route_stats: list[dict]) -> str:
    today = datetime.now().strftime("%A, %d %B %Y")
    lines = [
        f"Flight Scraper Report -- {SCRAPER_NAME} -- {today}",
        "=" * 62, "",
        f"Status   : {'Completed' if result['success'] else 'FAILED'}",
        f"Duration : {result['duration']}",
        "",
    ]

    if route_stats:
        lines += [
            "-" * 62,
            f"Per-Route Breakdown  ({len(route_stats)} routes)",
            "-" * 62,
        ]
        for s in route_stats:
            price_ok = s["total_rows"] - s["no_price_rows"]
            pct      = (price_ok / s["total_rows"] * 100) if s["total_rows"] else 0
            r_status = (
                "NO DATA" if s["total_rows"] == 0 else
                "OK"      if s["no_price_rows"] == 0 else
                "PARTIAL" if price_ok > 0 else
                "NO PRICE"
            )
            file_label = s["file"].name if s["file"] else "(no file — 0 rows captured)"
            lines += [
                f"  [{r_status}] {s['label']}",
                f"       File        : {file_label}",
                f"       Total rows  : {s['total_rows']}",
                f"       With price  : {price_ok}  ({pct:.0f}%)",
                f"       No price    : {s['no_price_rows']}",
                "",
            ]

        attached = [s for s in route_stats if s["file"] is not None]
        skipped  = [s for s in route_stats if s["file"] is None]
        lines += [
            "-" * 62,
            f"Attached files ({len(attached)}):",
        ]
        for s in attached:
            size_kb = s["file"].stat().st_size / 1024 if s["file"].exists() else 0
            lines.append(f"  - {s['file'].name}  ({size_kb:.1f} KB)")
        if skipped:
            lines += [
                "",
                f"Routes with NO data — no file attached ({len(skipped)}):",
            ]
            for s in skipped:
                lines.append(f"  - {s['label']}")
    else:
        lines.append("  No route data -- scraper produced no output.")

    lines.append("")
    return "\n".join(lines)


def send_email(result: dict, route_stats: list[dict]) -> None:
    if not EMAIL_PASSWORD:
        log("EMAIL_PASSWORD not set -- skipping email.")
        return

    today   = datetime.now().strftime("%Y-%m-%d")
    status  = "OK" if result["success"] else "FAILED"
    subject = f"Nexus Airlines Scraper -- {today} -- {status}"
    body    = build_email_body(result, route_stats)

    msg = MIMEMultipart()
    msg["From"]    = EMAIL_FROM
    msg["To"]      = EMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    attachments = 0
    for s in route_stats:
        filepath = s["file"]
        if filepath is None:
            continue  # route had no data — no file to attach
        if not filepath.exists():
            log(f"File missing, skipping attachment: {filepath.name}")
            continue
        try:
            with open(filepath, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filepath.name}"')
            msg.attach(part)
            attachments += 1
        except Exception as e:
            log(f"Could not attach {filepath.name}: {e}")

    log(f"Sending email to {EMAIL_TO} ({attachments} attachments)...")
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg)
        log("Email sent successfully!")
    except Exception as e:
        log(f"Email failed: {e}")


# ── Runner ───────────────────────────────────────────────

def run_scraper() -> dict:
    log(f"{'=' * 55}")
    log(f"Starting {SCRAPER_NAME} scraper...")
    log(f"   Command: {' '.join(SCRAPER_CMD)}")
    for orig, dest in ROUTES:
        log(f"     - {route_label(orig, dest)}")
    log("")

    start     = time.time()
    success   = False
    exit_code = -1
    duration  = "0s"

    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            log(f"Retry {attempt}/{MAX_RETRIES} -- waiting {RETRY_DELAY_S}s...")
            time.sleep(RETRY_DELAY_S)

        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            env["TZ"] = "Australia/Perth"

            returncode, output = stream_process(SCRAPER_CMD, env, timeout=14400)
            elapsed   = time.time() - start
            duration  = format_duration(elapsed)
            exit_code = returncode
            success   = returncode == 0

            if success:
                log(f"{SCRAPER_NAME} completed in {duration}")
                break

            log(f"{SCRAPER_NAME} failed (exit code {returncode}) after {duration}")
            if any(err in output for err in RETRY_ERRORS):
                log("   Connection error -- will retry.")
                if attempt < MAX_RETRIES:
                    continue
            break

        except subprocess.TimeoutExpired:
            duration  = format_duration(time.time() - start)
            log(f"{SCRAPER_NAME} timed out after {duration}")
            exit_code = -1
            success   = False
            break

        except Exception as e:
            duration  = format_duration(time.time() - start)
            log(f"{SCRAPER_NAME} crashed: {e}")
            exit_code = -1
            success   = False
            break

    return {"name": SCRAPER_NAME, "success": success, "exit_code": exit_code, "duration": duration}


# ── Main ─────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Nexus Airlines cron: scrape + split + email")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip scraper, split + email existing combined CSV")
    args = parser.parse_args()

    log("=" * 55)
    log("Nexus Airlines Scraper Cron")
    log(f"   Date  : {datetime.now().strftime('%A, %d %B %Y %H:%M %Z')}")
    log(f"   Mode  : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    log("=" * 55)
    log("")

    job_start = time.time()
    stamp     = datetime.now().strftime("%d-%m-%Y_%H-%M")

    if args.dry_run:
        log("Dry run -- skipping scraper.")
        result = {"name": SCRAPER_NAME, "success": True, "exit_code": 0, "duration": "dry-run"}
        combined = max(
            (p for p in OUTPUT_DIR.rglob("Nexus_Fare_Tracker_*.csv") if p.is_file()),
            key=lambda p: p.stat().st_mtime,
            default=None,
        )
    else:
        result   = run_scraper()
        combined = find_combined_csv_since(job_start)

    route_stats: list[dict] = []

    if combined:
        log(f"Combined CSV: {combined.name}")
        log("Splitting into per-route files...")
        route_stats = split_combined_csv(combined, stamp)
        files_written = sum(1 for s in route_stats if s["file"] is not None)
        log(f"   -> {files_written}/{len(route_stats)} per-route file(s) created.\n")
    else:
        log("No combined CSV found -- scraper produced no output.")

    send_email(result, route_stats)

    empty_routes = [s for s in route_stats if s["total_rows"] == 0]
    if empty_routes:
        log("")
        log("WARNING: routes captured 0 rows -- no file emitted:")
        for s in empty_routes:
            log(f"   - {s['label']}")

    log("")
    log("=" * 55)
    overall_ok = result["success"] and not empty_routes
    log(f"Done -- {'Success' if overall_ok else 'FAILED'}")
    log("=" * 55)

    if not result["success"]:
        sys.exit(1)
    if empty_routes:
        sys.exit(3)  # scraper exited 0 but some routes have no data


if __name__ == "__main__":
    main()
