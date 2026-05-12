# Render Cron Job — Deployment Guide

This folder contains everything needed to deploy the daily flight scraper cron job on Render.com.

---

## 📁 Files In This Folder

| File | Purpose |
|------|---------|
| `cron_runner.py` | **Main entry point** — runs all 4 scrapers sequentially, then emails the results |
| `qantas_production_4_Zones.py` | Qantas scraper (4 routes via Bright Data Scraping Browser) |
| `airnorth_fast_async.py` | Airnorth scraper (async Playwright + Oxylabs CDP) |
| `scrape_nexus_final.py` | Nexus Airlines scraper (stealth Playwright) |
| `rex_brightdata.py` | Rex Airlines scraper (Bright Data Browser API) |
| `requirements.txt` | Python dependencies |
| `.env` | Environment variables reference (⚠️ do NOT commit — use Render env vars instead) |

---

## 🚀 Step-by-Step Deployment

### Step 1: Push this folder to GitHub

Create a **new GitHub repository** (or a branch) and push the contents of this `render_cron_deploy` folder as the root of the repo.

```
your-repo/
├── cron_runner.py
├── qantas_production_4_Zones.py
├── airnorth_fast_async.py
├── scrape_nexus_final.py
├── rex_brightdata.py
└── requirements.txt
```

> ⚠️ **Do NOT push the `.env` file.** Add it to `.gitignore`. All secrets go into Render's environment variables.

---

### Step 2: Create a Render Cron Job

1. Log in to [render.com](https://render.com)
2. Click **New** → **Cron Job**
3. Connect your GitHub repository
4. Configure:

| Setting | Value |
|---------|-------|
| **Name** | `flight-scraper-cron` |
| **Environment** | `Python` |
| **Branch** | `main` |
| **Build Command** | `pip install -r requirements.txt && playwright install chromium` |
| **Command** | `python cron_runner.py` |
| **Schedule** | `0 21 * * *` (see schedule section below) |

---

### Step 3: Set the Schedule

Render uses **UTC** for cron schedules.

| Target Time (Australia) | UTC Cron | When to Use |
|--------------------------|----------|-------------|
| 7:00 AM AEST (Sydney/Melbourne) | `0 21 * * *` | Standard time (Apr–Oct) |
| 7:00 AM AEDT (Sydney/Melbourne) | `0 20 * * *` | Daylight saving (Oct–Apr) |
| 7:00 AM AWST (Perth) | `0 23 * * *` | Perth (no daylight saving) |

---

### Step 4: Add Environment Variables

In the Render Cron Job settings, go to **Environment** and add these variables:

#### 📧 Email (Required)

| Variable | Value |
|----------|-------|
| `EMAIL_FROM` | `ahteshamsalamat@gmail.com` |
| `EMAIL_PASSWORD` | `oxar pkne tppr dtys` |
| `EMAIL_TO` | `ahteshamansari@bizprospex.com` |

#### 🐍 Python Version (Required)

| Variable | Value |
|----------|-------|
| `PYTHON_VERSION` | `3.11.8` |

> ⚠️ **Critical**: If you skip `PYTHON_VERSION`, Render may default to Python 3.14+ where `pandas` wheels aren't available, causing the build to fail.

#### ✈️ Qantas — Bright Data Scraping Browser

| Variable | Value |
|----------|-------|
| `BRIGHTDATA_HOST` | `brd.superproxy.io` |
| `BRIGHTDATA_PORT` | `9515` |
| `BRIGHTDATA_CUSTOMER_ID` | `hl_fbc4a16a` |
| `QANTAS_BME_KNX_ZONE` | `scraping_browser2` |
| `QANTAS_BME_KNX_PASS` | *(copy from .env)* |
| `QANTAS_BME_DRW_ZONE` | `qantas_1` |
| `QANTAS_BME_DRW_PASS` | *(copy from .env)* |
| `QANTAS_DRW_KNX_ZONE` | `qantas_2` |
| `QANTAS_DRW_KNX_PASS` | *(copy from .env)* |
| `QANTAS_KNX_BME_ZONE` | `qantas_3` |
| `QANTAS_KNX_BME_PASS` | *(copy from .env)* |

#### 🛩️ Airnorth — Oxylabs CDP (Primary)

| Variable | Value |
|----------|-------|
| `OXY_USER` | *(copy from .env)* |
| `OXY_PASS` | *(copy from .env)* |
| `OXY_ENDPOINT` | `ubc.oxylabs.io` |

#### 🛩️ Airnorth — Bright Data CDP (Fallback)

| Variable | Value |
|----------|-------|
| `BRIGHT_CDP_URL` | *(copy from .env)* |

#### 🛩️ Airnorth — Bright Data Residential Proxy (Secondary Fallback)

| Variable | Value |
|----------|-------|
| `BRIGHT_PROXY_SERVER` | `brd.superproxy.io` |
| `BRIGHT_PROXY_USERNAME` | *(copy from .env)* |
| `BRIGHT_PROXY_PASSWORD` | *(copy from .env)* |

#### 🦊 Rex — Bright Data Browser API

These use the defaults baked into `rex_brightdata.py`. Only override if your credentials change:

| Variable | Value |
|----------|-------|
| `BD_BROWSER_HOST` | `brd.superproxy.io` |
| `BD_BROWSER_PORT` | `9222` |
| `BD_BROWSER_USER` | *(copy from .env or use default)* |
| `BD_BROWSER_PASS` | *(copy from .env or use default)* |

#### 🌏 Nexus — No Extra Variables Needed

Nexus uses stealth Playwright with no proxy. No additional env vars required.

---

### Step 5: Trigger a Test Run

1. Open the cron job in the **Render Dashboard**
2. Click **Trigger Run**
3. Watch the **Logs** tab
4. You should see output like:

```
[2026-05-13 07:00:00] =======================================================
[2026-05-13 07:00:00] 🗓️  Flight Scraper Cron Job
[2026-05-13 07:00:00]    Date     : Tuesday, 13 May 2026 07:00
[2026-05-13 07:00:00]    Mode     : FULL RUN
[2026-05-13 07:00:00]    Email to : ahteshamansari@bizprospex.com
[2026-05-13 07:00:00]    Scrapers : 4
[2026-05-13 07:00:00] =======================================================

[2026-05-13 07:00:00] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[2026-05-13 07:00:00] 🚀 Starting Qantas scraper...
...
[2026-05-13 09:30:00] ✅ Qantas completed in 2h 30m 0s

[2026-05-13 09:30:00] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[2026-05-13 09:30:00] 🚀 Starting Airnorth scraper...
...
```

5. After all scrapers finish, check your email at `ahteshamansari@bizprospex.com`
6. The email should contain all CSV/XLSX files as attachments

---

## 🧪 Testing Email Without Running Scrapers

To verify the email setup works before a full run:

```bash
python cron_runner.py --dry-run
```

This skips all scrapers and just sends an email with any existing files in `output/`.

---

## ⚙️ How It Works

```
cron_runner.py
    │
    ├── 1. python qantas_production_4_Zones.py --workers 1
    │       → 4 routes: BME→KNX, BME→DRW, DRW→KNX, KNX→BME
    │       → Output: output/Qantas_*.xlsx
    │
    ├── 2. python airnorth_fast_async.py --all --workers 1
    │       → 4 routes: BME→KNX, BME→DRW, DRW→KNX, KNX→BME
    │       → Output: output/airnorth_*/Fare_Tracker_Airnorth_*.csv/.xlsx
    │
    ├── 3. python scrape_nexus_final.py
    │       → 10 routes: PER↔GET, PER↔BME, KTA↔BME, PHE↔BME, GET↔BME
    │       → Output: output/Nexus_Fare_Tracker_*.csv
    │
    ├── 4. python rex_brightdata.py --skip-unblocker-check --output output/rex_results_all_routes.xlsx
    │       → 10 routes: PER↔ALH, PER↔EPR, PER↔CVQ, PER↔MJK, CVQ↔MJK
    │       → Output: output/rex_results_all_routes.xlsx
    │
    └── 📧 Email all CSV/XLSX files to ahteshamansari@bizprospex.com
```

If any scraper **fails**, the runner continues to the next one (unlike `&&` in bash). The email report shows which scrapers succeeded and which failed.

---

## 📋 Important Notes

- **One at a time**: Render guarantees only one instance of the same cron job runs at a time
- **Ephemeral disk**: Output files are temporary — they're emailed before the container shuts down, so nothing is lost
- **4-hour timeout**: Each scraper has a 4-hour max timeout built into `cron_runner.py`
- **No web server**: This is a cron job, not a web service. Do NOT use `gunicorn app:app` here
- **Exit code**: If any scraper fails, `cron_runner.py` exits with code 1 so Render marks the run as failed in the dashboard
