"""
Cron: Airnorth scraper + email
Runs each Airnorth route one at a time and emails the output file as soon as
that route finishes, so the client receives 4 separate emails with attachments
rather than waiting for all routes to complete.
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

EMAIL_FROM     = os.getenv("EMAIL_FROM", "ahteshamsalamat@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO       = os.getenv("EMAIL_TO", "ahteshamansari@bizprospex.com")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

MAX_RETRIES   = 3
RETRY_DELAY_S = 60
RETRY_ERRORS  = ("Connection aborted.", "RemoteDisconnected")

# ── Route definitions ──────────────────────────────────
# Each route runs as its own subprocess so an email is sent as soon as it
# finishes — the client gets data incrementally rather than all at once.

ROUTES = [
    {
        "name": "Airnorth BME → KNX",
        "route_arg": "BME-KNX",
        "label": "BME → KNX (Broome → Kununurra)",
    },
    {
        "name": "Airnorth BME → DRW",
        "route_arg": "BME-DRW",
        "label": "BME → DRW (Broome → Darwin)",
    },
    {
        "name": "Airnorth DRW → KNX",
        "route_arg": "DRW-KNX",
        "label": "DRW → KNX (Darwin → Kununurra)",
    },
    {
        "name": "Airnorth DRW → BME",
        "route_arg": "DRW-BME",
        "label": "DRW → BME (Darwin → Broome)",
    },
    {
        "name": "Airnorth KNX → DRW",
        "route_arg": "KNX-DRW",
        "label": "KNX → DRW (Kununurra → Darwin)",
    },
    {
        "name": "Airnorth KNX → BME",
        "route_arg": "KNX-BME",
        "label": "KNX → BME (Kununurra → Broome)",
    },
]

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


def collect_output_files_since(since_ts: float) -> list[Path]:
    files = []
    if not OUTPUT_DIR.exists():
        return files
    for item in OUTPUT_DIR.rglob("*"):
        if item.is_file() and item.suffix.lower() in (".csv", ".xlsx"):
            if item.stat().st_mtime >= since_ts:
                files.append(item)
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files


def build_email_body(result: dict, files: list[Path]) -> str:
    today = datetime.now().strftime("%A, %d %B %Y")
    lines = [
        f"Flight Scraper Report — {result['name']} — {today}",
        "=" * 55, "",
    ]
    status_icon = "✅" if result["success"] else "❌"
    lines.append(f"{status_icon}  {result['name']}")
    lines.append(f"    Status   : {'Completed' if result['success'] else 'FAILED'}")
    lines.append(f"    Duration : {result['duration']}")
    lines.append(f"    Route    : {result['label']}")
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
        lines.append("⚠️  No output files were generated.")
    lines.append("")
    return "\n".join(lines)


def send_email(result: dict, files: list[Path]) -> None:
    if not EMAIL_PASSWORD:
        log("⚠️  EMAIL_PASSWORD not set — skipping email.")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    status = "OK" if result["success"] else "FAILED"
    subject = f"Airnorth {result['route_arg']} — {today} — {status}"
    body = build_email_body(result, files)

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


# ── Per-route runner ─────────────────────────────────────

def run_route(route: dict) -> dict:
    name      = route["name"]
    route_arg = route["route_arg"]
    label     = route["label"]
    cmd       = ["python", "airnorth_brightdata_Main.py", "--route", route_arg, "--workers", "8"]

    log(f"{'━' * 55}")
    log(f"🚀 Starting {name}...")
    log(f"   Route  : {label}")
    log(f"   Command: {' '.join(cmd)}")
    log("")

    start     = time.time()
    success   = False
    exit_code = -1
    duration  = "0s"

    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            log(f"⟳  [{name}] Retry {attempt}/{MAX_RETRIES} — waiting {RETRY_DELAY_S}s...")
            time.sleep(RETRY_DELAY_S)

        try:
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"

            returncode, output = stream_process(cmd, env, timeout=7200)
            elapsed  = time.time() - start
            duration = format_duration(elapsed)
            exit_code = returncode
            success   = returncode == 0

            if success:
                log(f"✅ {name} completed in {duration}")
                break

            log(f"❌ {name} failed (exit code {returncode}) after {duration}")
            if any(err in output for err in RETRY_ERRORS):
                log("   ↳ Connection error detected — will retry.")
                if attempt < MAX_RETRIES:
                    continue
            break

        except subprocess.TimeoutExpired:
            duration = format_duration(time.time() - start)
            log(f"⏰ {name} timed out after {duration}")
            exit_code = -1
            success   = False
            break

        except Exception as e:
            duration = format_duration(time.time() - start)
            log(f"💥 {name} crashed: {e}")
            exit_code = -1
            success   = False
            break

    return {
        "name":      name,
        "route_arg": route_arg,
        "label":     label,
        "success":   success,
        "exit_code": exit_code,
        "duration":  duration,
    }


# ── Main ────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Airnorth cron: scrape all routes + email per route")
    parser.add_argument("--dry-run", action="store_true", help="Skip scrapers, email existing files")
    args = parser.parse_args()

    log("=" * 55)
    log("🗓️  Airnorth Scraper Cron")
    log(f"   Date  : {datetime.now().strftime('%A, %d %B %Y %H:%M %Z')}")
    log(f"   Mode  : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    log(f"   Routes: {len(ROUTES)}")
    log("=" * 55)
    log("")

    any_failed = False

    for i, route in enumerate(ROUTES, 1):
        log(f"[Route {i}/{len(ROUTES)}] {route['label']}")

        route_start = time.time()

        if args.dry_run:
            result = {
                "name":      route["name"],
                "route_arg": route["route_arg"],
                "label":     route["label"],
                "success":   True,
                "exit_code": 0,
                "duration":  "dry-run",
            }
            files = [
                f for f in OUTPUT_DIR.rglob("*")
                if f.is_file() and f.suffix.lower() in (".csv", ".xlsx")
            ]
        else:
            result = run_route(route)
            files  = collect_output_files_since(route_start)

        log(f"\n📁 Found {len(files)} output file(s) for {route['name']}.")
        for f in files:
            log(f"   • {f}")

        send_email(result, files)
        log("")

        if not result["success"]:
            any_failed = True

    log("=" * 55)
    log(f"🏁 Done — {'FAILED (one or more routes)' if any_failed else 'All routes succeeded'}")
    log("=" * 55)

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
