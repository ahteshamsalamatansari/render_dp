"""
Cron: Qantas scraper + email
Runs each of the 4 routes as a separate subprocess. If a route produces no stdout
for 5 minutes (stall), it is killed and marked failed. After all 4 routes finish,
any failed routes are retried once using alternate Brightdata credentials (if set).
Emails all output files on completion.
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

# Kill a route subprocess if no stdout line appears for this many seconds
ROUTE_STALL_S      = 300   # 5 minutes
INTER_ROUTE_WAIT_S = 20    # pause between routes

# Route list: (route_num, origin, dest) — must match --route N in the scraper
ROUTES = [
    (1, "BME", "KNX"),
    (2, "BME", "DRW"),
    (3, "DRW", "KNX"),
    (4, "KNX", "BME"),
]

# Maps route_num → (alt_zone_var, alt_pass_var, prim_zone_var, prim_pass_var)
# On retry, prim vars are overridden with the alt values so the scraper picks them up
ALT_CRED_MAP = {
    1: ("QANTAS_ALT_BME_KNX_ZONE", "QANTAS_ALT_BME_KNX_PASS", "QANTAS_BME_KNX_ZONE", "QANTAS_BME_KNX_PASS"),
    2: ("QANTAS_ALT_BME_DRW_ZONE", "QANTAS_ALT_BME_DRW_PASS", "QANTAS_BME_DRW_ZONE", "QANTAS_BME_DRW_PASS"),
    3: ("QANTAS_ALT_DRW_KNX_ZONE", "QANTAS_ALT_DRW_KNX_PASS", "QANTAS_DRW_KNX_ZONE", "QANTAS_DRW_KNX_PASS"),
    4: ("QANTAS_ALT_KNX_BME_ZONE", "QANTAS_ALT_KNX_BME_PASS", "QANTAS_KNX_BME_ZONE", "QANTAS_KNX_BME_PASS"),
}

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
    lines.append(f"    Routes   :")
    for route in result["routes"]:
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
        lines.append("⚠️  No output files were generated.")
    lines.append("")
    return "\n".join(lines)


def send_email(result: dict, files: list[Path]) -> None:
    if not EMAIL_PASSWORD:
        log("⚠️  EMAIL_PASSWORD not set — skipping email.")
        return

    today = datetime.now().strftime("%Y-%m-%d")
    status = "OK" if result["success"] else "FAILED"
    subject = f"Qantas Scraper — {today} — {status}"
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


# ── Per-route runner ────────────────────────────────────

def run_single_route(route_num: int, origin: str, dest: str,
                     env: dict | None = None) -> tuple[bool, str]:
    """
    Run one Qantas route as a subprocess (--route N).
    Kills the process if no stdout line appears for ROUTE_STALL_S seconds.
    Returns (success, full_output_text).
    """
    cmd = ["python", "Qantas_4Zones_Deliver_13_05_2026_FixedU.py", "--route", str(route_num)]
    run_env = (env if env is not None else os.environ).copy()
    run_env["PYTHONUNBUFFERED"] = "1"
    run_env["TZ"] = "Australia/Perth"

    log(f"▶ Route {route_num} ({origin}→{dest}): starting  [stall limit = {ROUTE_STALL_S}s]")
    start = time.time()

    proc = subprocess.Popen(
        cmd, env=run_env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )

    output_lines: list[str] = []
    last_line_ts = [time.time()]
    stalled      = [False]

    def _reader():
        for line in proc.stdout:
            print(line, end="", flush=True)
            output_lines.append(line)
            last_line_ts[0] = time.time()

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    while proc.poll() is None:
        time.sleep(5)
        idle = time.time() - last_line_ts[0]
        if idle >= ROUTE_STALL_S:
            log(f"⏰ Route {route_num} ({origin}→{dest}): no output for {int(idle)}s — killing (stall)")
            proc.kill()
            stalled[0] = True
            break

    t.join(timeout=5)
    duration = format_duration(time.time() - start)

    if stalled[0]:
        log(f"❌ Route {route_num} ({origin}→{dest}): stalled — {duration}")
        return False, "".join(output_lines)

    exit_code = proc.returncode if proc.returncode is not None else -1
    success   = exit_code == 0
    icon      = "✅" if success else "❌"
    log(f"{icon} Route {route_num} ({origin}→{dest}): exit {exit_code} — {duration}")
    return success, "".join(output_lines)


# ── Alternate credential helpers ────────────────────────

def build_alt_env(route_num: int) -> dict | None:
    """
    Return a copy of os.environ with alternate zone/pass injected for the given route,
    or None if alternate credentials are not configured for that route.
    """
    alt_zone_var, alt_pass_var, prim_zone_var, prim_pass_var = ALT_CRED_MAP[route_num]
    alt_zone = os.getenv(alt_zone_var, "").strip()
    alt_pass = os.getenv(alt_pass_var, "").strip()
    if not alt_zone or not alt_pass:
        return None
    env = os.environ.copy()
    env[prim_zone_var] = alt_zone
    env[prim_pass_var] = alt_pass
    return env


# ── Scraper orchestrator ────────────────────────────────

def run_scraper() -> dict:
    job_start = time.time()
    results: dict[int, dict] = {}
    failed:  list[int] = []

    log(f"{'━' * 55}")
    log(f"🚀 Starting Qantas — {len(ROUTES)} routes, sequential")
    log(f"   Stall timeout per route : {ROUTE_STALL_S}s (5 min)")
    log("")

    # ── First pass: run all routes ────────────────────────
    for i, (route_num, origin, dest) in enumerate(ROUTES):
        success, _ = run_single_route(route_num, origin, dest)
        results[route_num] = {
            "origin": origin, "dest": dest,
            "success": success, "attempt": "primary",
        }
        if not success:
            failed.append(route_num)
        if i < len(ROUTES) - 1:
            log(f"⏳ Waiting {INTER_ROUTE_WAIT_S}s before next route...")
            time.sleep(INTER_ROUTE_WAIT_S)

    # ── Retry pass: failed routes with alternate credentials
    if failed:
        retryable = [(n, build_alt_env(n)) for n in failed if build_alt_env(n) is not None]
        if retryable:
            log(f"\n{'━' * 55}")
            log(f"🔁 Retrying {len(retryable)} failed route(s) with alternate credentials...")
            for j, (route_num, alt_env) in enumerate(retryable):
                origin   = results[route_num]["origin"]
                dest     = results[route_num]["dest"]
                alt_zone = alt_env.get(f"QANTAS_{origin}_{dest}_ZONE", "?")
                log(f"   Route {route_num} ({origin}→{dest}): alt zone = {alt_zone}")
                success, _ = run_single_route(route_num, origin, dest, env=alt_env)
                results[route_num]["success"] = success
                results[route_num]["attempt"] = "alternate"
                if j < len(retryable) - 1:
                    log(f"⏳ Waiting {INTER_ROUTE_WAIT_S}s before next retry...")
                    time.sleep(INTER_ROUTE_WAIT_S)
        else:
            log(f"\n⚠️  {len(failed)} route(s) failed — no alternate credentials configured, skipping retry.")

    all_success = all(r["success"] for r in results.values())
    duration    = format_duration(time.time() - job_start)

    route_labels = [
        f"{r['origin']} → {r['dest']} ({'✅' if r['success'] else '❌'} {r['attempt']})"
        for r in results.values()
    ]

    return {
        "name":     "Qantas",
        "success":  all_success,
        "exit_code": 0 if all_success else 1,
        "duration": duration,
        "routes":   route_labels,
    }


# ── Entry point ─────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Qantas cron: scrape + email")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip scraper, email existing files")
    args = parser.parse_args()

    log("=" * 55)
    log("🗓️  Qantas Scraper Cron")
    log(f"   Date  : {datetime.now().strftime('%A, %d %B %Y %H:%M %Z')}")
    log(f"   Mode  : {'DRY RUN' if args.dry_run else 'FULL RUN'}")
    log("=" * 55)
    log("")

    job_start = time.time()

    if args.dry_run:
        log("🔸 Dry run — skipping scraper.")
        result = {
            "name": "Qantas", "success": True, "exit_code": 0,
            "duration": "dry-run",
            "routes": [f"{o} → {d} (dry-run)" for _, o, d in ROUTES],
        }
        files = [f for f in OUTPUT_DIR.rglob("*")
                 if f.is_file() and f.suffix.lower() in (".csv", ".xlsx")]
    else:
        result = run_scraper()
        files  = collect_output_files_since(job_start)

    log(f"\n📁 Found {len(files)} output file(s).")
    for f in files:
        log(f"   • {f}")

    send_email(result, files)

    log("")
    log("=" * 55)
    log(f"🏁 Done — {'Success' if result['success'] else 'FAILED'}")
    log("=" * 55)

    if not result["success"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
