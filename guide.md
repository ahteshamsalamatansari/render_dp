# Render Deployment Guide — 4 Airline Cron Jobs

Each airline runs as its **own separate Cron Job** on Render, connected to its own branch.  
All 4 crons live in the same GitHub repo — only the branch and start command differ.

---

## Overview

| Cron Job Name        | Branch          | Start Command              | Workers |
|----------------------|-----------------|----------------------------|---------|
| `cron-qantas`        | `cron/qantas`   | `python cron_qantas.py`    | 1 (sequential) |
| `cron-airnorth`      | `cron/airnorth` | `python cron_airnorth.py`  | 16 |
| `cron-nexus`         | `cron/nexus`    | `python cron_nexus.py`     | — |
| `cron-rex`           | `cron/rex`      | `python cron_rex.py`       | — |

Each script:
- Retries up to 3 times (60s delay) if a `Connection aborted` / `RemoteDisconnected` error is detected
- Emails output files (CSV/XLSX) immediately after the scraper finishes
- Exits with code 1 on failure so Render marks the run as failed

---

## Step 1 — Create a Cron Job on Render (repeat for all 4)

1. Go to [render.com](https://render.com) → **New** → **Cron Job**
2. Connect your GitHub repo: `ahteshamsalamatansari/render_dp`
3. Fill in the settings as shown in each section below
4. Click **Create Cron Job**

---

## Step 2 — Cron Job Settings

### Cron 1 — Qantas

| Setting | Value |
|---------|-------|
| Name | `cron-qantas` |
| Branch | `cron/qantas` |
| Runtime | `Python` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `python cron_qantas.py` |
| Schedule | *(set per timezone table below)* |

**Routes:** BME→KNX, BME→DRW, DRW→KNX, KNX→BME (run one by one)  
**Script:** `Qantas_4Zones_Deliver_13_05_2026_FixedU.py` — 4 dedicated Brightdata zones, one per route

**Environment Variables — add these in Render under Environment:**

| Variable | Value |
|----------|-------|
| `PYTHON_VERSION` | `3.11.8` |
| `EMAIL_FROM` | `ahteshamsalamat@gmail.com` |
| `EMAIL_PASSWORD` | `oxar pkne tppr dtys` |
| `EMAIL_TO` | `ahteshamansari@bizprospex.com` |
| `BRIGHTDATA_HOST` | `brd.superproxy.io` |
| `BRIGHTDATA_PORT` | `9515` |
| `BRIGHTDATA_CUSTOMER_ID` | `hl_fbc4a16a` |
| `QANTAS_BME_KNX_ZONE` | `scraping_browser2` |
| `QANTAS_BME_KNX_PASS` | `nymmsv0ffs60` |
| `QANTAS_BME_DRW_ZONE` | `qantas_1` |
| `QANTAS_BME_DRW_PASS` | `x9ck9dpthpsg` |
| `QANTAS_DRW_KNX_ZONE` | `qantas_2` |
| `QANTAS_DRW_KNX_PASS` | `kgu154ajo3d9` |
| `QANTAS_KNX_BME_ZONE` | `qantas_3` |
| `QANTAS_KNX_BME_PASS` | `n748kj03bomt` |

---

### Cron 2 — Airnorth

| Setting | Value |
|---------|-------|
| Name | `cron-airnorth` |
| Branch | `cron/airnorth` |
| Runtime | `Python` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `python cron_airnorth.py` |
| Schedule | *(set per timezone table below)* |

**Routes:** BME→KNX, BME→DRW, DRW→KNX, KNX→BME  
**Script:** `airnorth_brightdata_Main.py` — Brightdata Web Unlocker API, 16 async workers

**Environment Variables — add these in Render under Environment:**

| Variable | Value |
|----------|-------|
| `PYTHON_VERSION` | `3.11.8` |
| `EMAIL_FROM` | `ahteshamsalamat@gmail.com` |
| `EMAIL_PASSWORD` | `oxar pkne tppr dtys` |
| `EMAIL_TO` | `ahteshamansari@bizprospex.com` |
| `BRIGHTDATA_API_TOKEN` | `7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7` |
| `BRIGHTDATA_API_ZONE` | `airnorth_sc_01` |
| `BRIGHTDATA_API_URL` | `https://api.brightdata.com/request` |
| `BRIGHTDATA_CHECK_URL` | `https://geo.brdtest.com/welcome.txt?product=unlocker&method=api` |

---

### Cron 3 — Nexus Airlines

| Setting | Value |
|---------|-------|
| Name | `cron-nexus` |
| Branch | `cron/nexus` |
| Runtime | `Python` |
| Build Command | `pip install -r requirements.txt && playwright install chromium` |
| Start Command | `python cron_nexus.py` |
| Schedule | *(set per timezone table below)* |

**Routes:** PER↔GET, PER↔BME, KTA↔BME, PHE↔BME, GET↔BME (10 routes total)  
**Script:** `scrape_nexus_final.py`

**Environment Variables — add these in Render under Environment:**

| Variable | Value |
|----------|-------|
| `PYTHON_VERSION` | `3.11.8` |
| `EMAIL_FROM` | `ahteshamsalamat@gmail.com` |
| `EMAIL_PASSWORD` | `oxar pkne tppr dtys` |
| `EMAIL_TO` | `ahteshamansari@bizprospex.com` |
| `PLAYWRIGHT_BROWSERS_PATH` | `/opt/render/project/src` |

> `PLAYWRIGHT_BROWSERS_PATH` tells Playwright to install and look for Chromium inside the project folder, which Render persists between build and runtime. Without it, browsers go into `~/.cache` which is wiped after each build.

---

### Cron 4 — Rex Airlines

| Setting | Value |
|---------|-------|
| Name | `cron-rex` |
| Branch | `cron/rex` |
| Runtime | `Python` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `python cron_rex.py` |
| Schedule | *(set per timezone table below)* |

**Routes:** PER↔ALH, PER↔EPR, PER↔CVQ, PER↔MJK, CVQ↔MJK (10 routes total)  
**Script:** `rex_brightdata.py`

**Environment Variables — add these in Render under Environment:**

| Variable | Value |
|----------|-------|
| `PYTHON_VERSION` | `3.11.8` |
| `EMAIL_FROM` | `ahteshamsalamat@gmail.com` |
| `EMAIL_PASSWORD` | `oxar pkne tppr dtys` |
| `EMAIL_TO` | `ahteshamansari@bizprospex.com` |
| `BD_BROWSER_USER` | `brd-customer-hl_fbc4a16a-zone-cont_rex` |
| `BD_BROWSER_PASS` | `072res2p22t3` |
| `BD_BROWSER_HOST` | `brd.superproxy.io` |
| `BD_BROWSER_PORT` | `9222` |
| `BD_AUTH_TOKEN` | `7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7` |
| `BD_WEB_UNLOCKER_ZONE` | `unblocker1` |
| `BD_UNLOCKER_ENDPOINT` | `https://api.brightdata.com/request` |

---

## Step 3 — Schedule (UTC)

Render schedules run in **UTC**. Use the table below to target 7:00 AM local time:

| Local Time | Timezone | UTC Cron Expression |
|------------|----------|---------------------|
| 7:00 AM AWST (Perth) | UTC+8, no DST | `0 23 * * *` |
| 7:00 AM AEST (Sydney, non-DST) | UTC+10 | `0 21 * * *` |
| 7:00 AM AEDT (Sydney, DST) | UTC+11 | `0 20 * * *` |

> Perth does **not** observe daylight saving, so `0 23 * * *` is always correct for AWST.

---

## Step 4 — Environment Variables

Set these in each cron job under **Environment** in the Render dashboard.  
All 4 crons share the same email and Python vars. Airnorth has its own Brightdata vars.

### All 4 Crons — Email

| Variable | Value |
|----------|-------|
| `EMAIL_FROM` | `ahteshamsalamat@gmail.com` |
| `EMAIL_PASSWORD` | *(Gmail App Password)* |
| `EMAIL_TO` | `ahteshamansari@bizprospex.com` |

### All 4 Crons — Python Version

| Variable | Value |
|----------|-------|
| `PYTHON_VERSION` | `3.11.8` |

> Required — without this Render may pick Python 3.14+ where some wheels are unavailable.

---

### Qantas — Brightdata Scraping Browser

| Variable | Value |
|----------|-------|
| `BRIGHTDATA_HOST` | `brd.superproxy.io` |
| `BRIGHTDATA_PORT` | `9515` |
| `BRIGHTDATA_CUSTOMER_ID` | `hl_fbc4a16a` |
| `QANTAS_BME_KNX_ZONE` | `scraping_browser2` |
| `QANTAS_BME_KNX_PASS` | `nymmsv0ffs60` |
| `QANTAS_BME_DRW_ZONE` | `qantas_1` |
| `QANTAS_BME_DRW_PASS` | `x9ck9dpthpsg` |
| `QANTAS_DRW_KNX_ZONE` | `qantas_2` |
| `QANTAS_DRW_KNX_PASS` | `kgu154ajo3d9` |
| `QANTAS_KNX_BME_ZONE` | `qantas_3` |
| `QANTAS_KNX_BME_PASS` | `n748kj03bomt` |

---

### Airnorth — Brightdata Web Unlocker API

| Variable | Value |
|----------|-------|
| `BRIGHTDATA_API_TOKEN` | `7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7` |
| `BRIGHTDATA_API_ZONE` | `airnorth_sc_01` |
| `BRIGHTDATA_API_URL` | `https://api.brightdata.com/request` |
| `BRIGHTDATA_CHECK_URL` | `https://geo.brdtest.com/welcome.txt?product=unlocker&method=api` |

---

### Rex — Brightdata Browser API + Web Unlocker API

| Variable | Value |
|----------|-------|
| `BD_BROWSER_USER` | `brd-customer-hl_fbc4a16a-zone-cont_rex` |
| `BD_BROWSER_PASS` | `072res2p22t3` |
| `BD_BROWSER_HOST` | `brd.superproxy.io` |
| `BD_BROWSER_PORT` | `9222` |
| `BD_AUTH_TOKEN` | `7b1cdf1c-e4e0-4b6c-925b-0121031e6bf7` |
| `BD_WEB_UNLOCKER_ZONE` | `unblocker1` |
| `BD_UNLOCKER_ENDPOINT` | `https://api.brightdata.com/request` |

---

### Nexus — No Extra Variables

Nexus uses no proxy. No additional env vars needed beyond email + Python version.

---

## Step 5 — Trigger a Test Run

1. Open any cron job in the Render dashboard
2. Click **Trigger Run**
3. Open the **Logs** tab and watch the output
4. A successful run looks like:

```
[2026-05-14 07:00:00] =======================================================
[2026-05-14 07:00:00] 🗓️  Qantas Scraper Cron
[2026-05-14 07:00:00]    Date  : Thursday, 14 May 2026 07:00
[2026-05-14 07:00:00]    Mode  : FULL RUN
[2026-05-14 07:00:00] =======================================================
[2026-05-14 07:00:00] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[2026-05-14 07:00:00] 🚀 Starting Qantas scraper...
...
[2026-05-14 09:30:00] ✅ Qantas completed in 2h 30m 0s
[2026-05-14 09:30:00] 📁 Found 4 output file(s).
[2026-05-14 09:30:00] 📧 Sending email to ahteshamansari@bizprospex.com (4 attachments)...
[2026-05-14 09:30:00] ✅ Email sent successfully!
[2026-05-14 09:30:00] 🏁 Done — Success
```

5. Check `ahteshamansari@bizprospex.com` — you should receive an email with the airline's CSV/XLSX files attached

---

## Testing Email Without Running Scrapers

Run any cron script locally with `--dry-run` to verify email works without scraping:

```bash
python cron_qantas.py --dry-run
python cron_airnorth.py --dry-run
python cron_nexus.py --dry-run
python cron_rex.py --dry-run
```

This skips the scraper and emails any existing files in `output/`.

---

## Updating a Cron

When you update a scraper script:

1. Make changes on the relevant branch (e.g. `cron/qantas`)
2. Commit and push
3. Render auto-deploys on the next scheduled run (or click **Trigger Run** to test immediately)

To update **all 4 crons** at once, make changes on `claude/zen-davinci-2848a8` then merge/rebase into each `cron/*` branch and push.

---

## Important Notes

- **Ephemeral disk** — Output files are lost when the container shuts down. They are emailed before exit, so nothing is lost
- **4-hour timeout** — Each scraper has a 4-hour maximum. If it hangs, Render kills it and the cron is marked failed
- **Retry logic** — On `Connection aborted` or `RemoteDisconnected` errors, the script retries up to 3 times with a 60-second wait
- **Exit code** — A failed scraper causes the script to exit with code 1, which Render marks as a failed run in the dashboard
- **No web server** — These are cron jobs. Do not use `gunicorn` or any web server command
