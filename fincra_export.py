#!/usr/bin/env python3
"""
Fincra Dashboard - Automated Payment Export -> S3 Upload -> Slack DM

Downloads both Pay-In and Pay-Out CSVs for the given date range,
uploads them to S3, then sends a Slack DM.

Flow:
  1. Accept --start_date / --end_date (YYYY-MM-DD) or fall back to .env.
  2. Login (email + password + Google Authenticator TOTP).
  3. Pay-Ins : Fetch via /api/collections with Bearer token + date params.
  4. Pay-Outs: Download unfiltered CSV via Export Table, filter rows in Python.
  5. Upload Pay-In  -> s3://payout-recon/fincra/collect/raw/<filename>
  6. Upload Pay-Out -> s3://payout-recon/fincra/payout/raw/<filename>
  7. Send Slack DM.

Usage:
    python fincra_export.py --start_date 2026-03-18 --end_date 2026-03-24

Note: Start Ultrasurf VPN before running this script.
"""

import argparse
import asyncio
import os
import pyotp
import requests
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
_ENV_FILE = Path(__file__).parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
_parser = argparse.ArgumentParser()
_parser.add_argument("--start_date", type=str, default=None)
_parser.add_argument("--end_date",   type=str, default=None)
_args = _parser.parse_args()

def _default_date():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

START_DATE = _args.start_date or os.environ.get("FINCRA_START_DATE", _default_date())
END_DATE   = _args.end_date   or os.environ.get("FINCRA_END_DATE",   _default_date())

START_DT = datetime.strptime(START_DATE, "%Y-%m-%d")
END_DT   = datetime.strptime(END_DATE,   "%Y-%m-%d")

def to_file_date(d: datetime) -> str:
    return d.strftime("%d%m%Y")

PAYIN_FILENAME  = f"FINCRA_PAYIN_{to_file_date(START_DT)}_to_{to_file_date(END_DT)}.csv"
PAYOUT_FILENAME = f"FINCRA_PAYOUT_{to_file_date(START_DT)}_to_{to_file_date(END_DT)}.csv"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
USERNAME    = os.environ.get("FINCRA_USERNAME", "")
PASSWORD    = os.environ.get("FINCRA_PASSWORD", "")
TOTP_SECRET = os.environ.get("FINCRA_TOTP_SECRET", "")

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_USER_ID   = os.environ.get("SLACK_USER_ID", "")

# S3 config
S3_ENABLED       = os.environ.get("S3_ENABLED", "false").lower() == "true"
S3_BUCKET        = os.environ.get("S3_BUCKET", "payout-recon")
S3_PAYIN_PREFIX  = os.environ.get("S3_PAYIN_PREFIX",  "fincra/collect/raw/")
S3_PAYOUT_PREFIX = os.environ.get("S3_PAYOUT_PREFIX", "fincra/payout/raw/")
S3_REGION        = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")

DOWNLOAD_DIR = Path("downloads")
LOGIN_URL    = "https://app.fincra.com/auth/login"
PAYINS_URL   = "https://app.fincra.com/payins"
PAYOUTS_URL  = "https://app.fincra.com/payouts"
BUSINESS_ID  = "6334454b10ed4b05f62955b6"   # Tazapay business ID on Fincra


def get_otp() -> str:
    return pyotp.TOTP(TOTP_SECRET).now()


async def ss(page, name: str) -> None:
    path = f"fincra_dbg_{name}.png"
    await page.screenshot(path=path)
    print(f"  [screenshot] {path}")


# ---------------------------------------------------------------------------
# S3 Upload
# ---------------------------------------------------------------------------
def upload_to_s3(local_path: Path, prefix: str) -> str:
    """
    Upload file to s3://{S3_BUCKET}/{prefix}<filename>
    Returns the S3 URI.
    """
    s3_key = f"{prefix}{local_path.name}"
    print(f"[s3] Uploading to s3://{S3_BUCKET}/{s3_key} ...")
    try:
        s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        s3.upload_file(
            str(local_path), S3_BUCKET, s3_key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        uri = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"[s3] Upload complete -> {uri}")
        return uri
    except NoCredentialsError:
        print("[s3] ERROR: AWS credentials not found")
        raise
    except ClientError as e:
        print(f"[s3] ERROR: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
        raise


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
def notify_slack(message: str, color: str = "good") -> None:
    if not SLACK_BOT_TOKEN or not SLACK_USER_ID:
        print("[slack] Not configured - skipping.")
        return
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type":  "application/json",
    }
    channel_id = SLACK_USER_ID
    if SLACK_USER_ID.startswith("U"):
        try:
            resp = requests.post(
                "https://slack.com/api/conversations.open",
                json={"users": SLACK_USER_ID}, headers=headers, timeout=10
            )
            data = resp.json()
            if data.get("ok"):
                channel_id = data["channel"]["id"]
        except Exception as e:
            print(f"[slack] conversations.open error: {e}")

    icon = {"good": ":white_check_mark:", "warning": ":warning:", "danger": ":x:"}.get(color, "")
    payload = {
        "channel": channel_id,
        "text":    f"{icon} {message}",
        "attachments": [{
            "color":  color,
            "text":   message,
            "footer": "Fincra Exporter",
            "ts":     int(datetime.now().timestamp()),
        }],
    }
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            json=payload, headers=headers, timeout=10
        )
        if resp.json().get("ok"):
            print("[slack] DM sent.")
        else:
            print(f"[slack] Error: {resp.json().get('error')}")
    except Exception as e:
        print(f"[slack] Failed: {e}")


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------
async def do_login(page) -> None:
    print("[login] Navigating to login page ...")
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(2_000)
    await ss(page, "00_login")

    await page.locator('input[type="email"], input[name="email"]').first.fill(USERNAME)
    await page.locator('input[type="password"]').first.fill(PASSWORD)
    await page.locator('button[type="submit"]').first.click()
    await page.wait_for_timeout(3_000)
    await ss(page, "01_after_submit")

    # 2FA OTP page
    if "twofa" in page.url or await page.locator('input').count() >= 6:
        print("[login] 2FA page detected ...")
        for attempt in range(1, 4):
            # If already logged in, stop
            if "dashboard" in page.url or "payins" in page.url or "payouts" in page.url:
                print("[login] Already on dashboard — skipping further OTP attempts.")
                break
            # Wait until we have 6 OTP input boxes
            try:
                await page.wait_for_function("document.querySelectorAll('input').length >= 6", timeout=10_000)
            except Exception:
                if "dashboard" in page.url:
                    break
            code = get_otp()
            print(f"[login] OTP attempt {attempt}: {code}")
            inputs = page.locator('input')
            filled = 0
            for i, digit in enumerate(code):
                try:
                    loc = inputs.nth(i)
                    await loc.click(timeout=5_000)
                    await page.wait_for_timeout(100)
                    el = await loc.element_handle(timeout=5_000)
                    await page.evaluate(
                        """([el, val]) => {
                            el.focus();
                            const setter = Object.getOwnPropertyDescriptor(
                                HTMLInputElement.prototype, 'value').set;
                            setter.call(el, val);
                            el.dispatchEvent(new Event('focus',  {bubbles: true}));
                            el.dispatchEvent(new Event('input',  {bubbles: true}));
                            el.dispatchEvent(new Event('change', {bubbles: true}));
                            el.dispatchEvent(new KeyboardEvent('keyup', {key: val, bubbles: true}));
                        }""",
                        [el, digit]
                    )
                    filled += 1
                except Exception:
                    break
            print(f"[login] Filled {filled}/6 OTP digits")
            if "dashboard" in page.url:
                break
            # Try pressing Enter first
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(1_000)
            if "dashboard" in page.url:
                break
            # Also try clicking the submit button
            try:
                submit = page.locator('button:has-text("Verify Account"), button[type="submit"]').first
                await submit.click(timeout=10_000)
            except Exception:
                pass
            await page.wait_for_timeout(3_000)
            if "dashboard" in page.url:
                break
            if attempt < 3:
                print("[login] OTP not accepted — waiting for next window ...")
                await page.wait_for_timeout(15_000)
        await ss(page, "02_after_2fa")

    await _dismiss_survey(page)
    print(f"[login] Done. URL: {page.url}")


async def _dismiss_survey(page) -> None:
    """Dismiss the NPS survey popup or any ReactModal overlay if present."""
    try:
        remind_btn = page.locator('button:has-text("Remind Me Later"), button:has-text("Remind me later")')
        if await remind_btn.count() > 0:
            await remind_btn.first.click()
            await page.wait_for_timeout(800)
        if await page.locator('.ReactModal__Overlay').count() > 0:
            await page.keyboard.press('Escape')
            await page.wait_for_timeout(500)
        try:
            await page.wait_for_selector('.ReactModal__Overlay', state='hidden', timeout=5_000)
        except Exception:
            pass
    except Exception:
        pass


async def ensure_logged_in(page) -> None:
    await page.goto("https://app.fincra.com/dashboard", wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(2_000)
    if "auth/login" in page.url:
        await do_login(page)
    else:
        await _dismiss_survey(page)
        print("[auth] Session already active.")


# ---------------------------------------------------------------------------
# Date picker helpers (shared by Pay-Ins and Pay-Outs)
# ---------------------------------------------------------------------------
async def _get_calendar_month_year(page):
    """Return (month_1indexed, year) from the open calendar's month select."""
    selects = page.locator('select')
    n = await selects.count()
    month = None
    year  = None
    for i in range(n):
        s    = selects.nth(i)
        html = await s.inner_html()
        val  = await s.input_value()
        if 'value="0"' in html and 'value="11"' in html:
            try:
                month = int(val) + 1
            except Exception:
                pass
        elif html.count('<option') >= 3:
            try:
                y = int(val)
                if 2000 < y < 2100:
                    year = y
            except Exception:
                pass
    return month, year


async def _calendar_nav_to(page, target_month: int, target_year: int) -> None:
    """Navigate the open calendar to the target month using arrow buttons only."""
    for _ in range(24):
        cur_month, cur_year = await _get_calendar_month_year(page)
        if cur_month is None or cur_year is None:
            break
        if cur_month == target_month and cur_year == target_year:
            break
        cur_total    = cur_year * 12 + cur_month
        target_total = target_year * 12 + target_month
        if cur_total > target_total:
            prev_btn = page.locator('.rdrPprevButton, .rdrPrevButton')
            if await prev_btn.count() == 0:
                prev_btn = page.locator('button[aria-label*="Go to previous"], button[aria-label*="previous"]')
            if await prev_btn.count() > 0:
                await prev_btn.first.click()
        else:
            next_btn = page.locator('.rdrNextButton')
            if await next_btn.count() == 0:
                next_btn = page.locator('button[aria-label*="Go to next"], button[aria-label*="next"]')
            if await next_btn.count() > 0:
                await next_btn.first.click()
        await page.wait_for_timeout(350)


async def _click_calendar_day(page, day: int) -> bool:
    """Click a day button in the open calendar. Returns True on success."""
    day_str = str(day)
    all_btns = page.locator('button').filter(has_text=day_str)
    n = await all_btns.count()
    for i in range(n):
        btn = all_btns.nth(i)
        txt = (await btn.text_content() or "").strip()
        if txt != day_str:
            continue
        if await btn.is_disabled():
            continue
        try:
            await btn.click(timeout=3_000)
            return True
        except Exception:
            continue
    return False


# ---------------------------------------------------------------------------
# Pay-Ins Export (via direct API — avoids react-date-range date picker issues)
# ---------------------------------------------------------------------------
async def export_payins(page, context) -> Path | None:
    """
    Downloads Pay-Ins via the Fincra collections API using the Bearer token
    from the browser session. Bypasses the buggy UI date range picker.
    """
    import csv

    print(f"\n[payin] Fetching Pay-Ins via API for {START_DATE} -> {END_DATE} ...")

    cookies = await context.cookies()
    token = next((c["value"] for c in cookies if c["name"] == "accessToken"), "")
    if not token:
        print("[payin] ERROR: accessToken cookie not found — cannot use API")
        return None

    headers_api = {
        "Authorization": f"Bearer {token}",
        "x-business-id":  BUSINESS_ID,
        "Accept":          "application/json, text/plain, */*",
    }

    all_results = []
    page_num    = 1
    per_page    = 200

    while True:
        resp = await context.request.get(
            "https://app.fincra.com/api/collections",
            params={
                "business":           BUSINESS_ID,
                "page":               str(page_num),
                "perPage":            str(per_page),
                "includeSubAccounts": "false",
                "dateInitiatedFrom":  START_DATE,
                "dateInitiatedTo":    END_DATE,
            },
            headers=headers_api,
        )
        if resp.status != 200:
            print(f"[payin] API error {resp.status}: {await resp.text()}")
            return None

        body = await resp.json()
        results = body.get("data", {}).get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"[payin] Page {page_num}: got {len(results)} records (total so far: {len(all_results)})")

        total = body.get("data", {}).get("total") or body.get("data", {}).get("count") or 0
        if len(all_results) >= total or len(results) < per_page:
            break
        page_num += 1

    if not all_results:
        print("[payin] No Pay-In records found for this date range.")
        return None

    dest = DOWNLOAD_DIR / PAYIN_FILENAME
    keys = list(all_results[0].keys())
    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_results)

    print(f"[payin] {len(all_results)} records -> {dest.resolve()}")
    return dest


# ---------------------------------------------------------------------------
# Pay-Outs Export (unfiltered download + Python date filter)
# ---------------------------------------------------------------------------
async def export_payouts(page, context) -> Path | None:
    """
    Downloads the full Pay-Outs export (unfiltered) via the Export Table button,
    then filters rows in Python to keep only records within START_DATE..END_DATE.
    """
    import csv

    print(f"\n[payout] Navigating to {PAYOUTS_URL} ...")
    await page.goto(PAYOUTS_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(2_000)
    await _dismiss_survey(page)
    await ss(page, "20_payouts")

    print("[payout] Clicking Export Table -> CSV (unfiltered) ...")
    export_table = page.locator('button:has-text("Export Table")')
    if await export_table.count() == 0:
        print("[payout] ERROR: Export Table button not found")
        return None
    await export_table.first.click()
    await page.wait_for_timeout(800)

    raw_path = DOWNLOAD_DIR / f"_payout_raw_{to_file_date(START_DT)}.csv"
    try:
        async with page.expect_download(timeout=30_000) as dl_info:
            await page.locator('text=CSV').click()
        dl = await dl_info.value
        await dl.save_as(raw_path)
        print(f"[payout] Raw download -> {raw_path.resolve()}")
    except PwTimeout:
        current_url = page.url
        if "s3.amazonaws.com" in current_url or ".csv" in current_url:
            print(f"[payout] Download via S3 URL: {current_url}")
            resp = requests.get(current_url, timeout=60)
            raw_path.write_bytes(resp.content)
        else:
            raise RuntimeError(f"Export Table download timed out. URL: {current_url}")

    await ss(page, "24_payout_done")

    # Filter rows by date range in Python
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt   = datetime.strptime(END_DATE,   "%Y-%m-%d").replace(hour=23, minute=59, second=59)

    filtered    = []
    headers_row = None
    date_col    = None
    with open(raw_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if headers_row is None:
                headers_row = list(row.keys())
                for col in headers_row:
                    if "date" in col.lower() or "created" in col.lower() or "initiated" in col.lower():
                        date_col = col
                        break
                print(f"[payout] Using date column: {date_col}")
            if date_col:
                raw_date = row.get(date_col, "")
                try:
                    dt = datetime.fromisoformat(raw_date.replace("Z", "").replace("T", " ").split(".")[0])
                    if start_dt <= dt <= end_dt:
                        filtered.append(row)
                except Exception:
                    pass
            else:
                filtered.append(row)

    if not filtered:
        print(f"[payout] No rows in date range — keeping all rows as-is")
        raw_path.rename(DOWNLOAD_DIR / PAYOUT_FILENAME)
        return DOWNLOAD_DIR / PAYOUT_FILENAME

    dest = DOWNLOAD_DIR / PAYOUT_FILENAME
    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers_row)
        writer.writeheader()
        writer.writerows(filtered)

    print(f"[payout] {len(filtered)} rows in range -> {dest.resolve()}")
    try:
        raw_path.unlink()
    except Exception:
        pass
    return dest


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    DOWNLOAD_DIR.mkdir(exist_ok=True)

    print("=" * 60)
    print(f"[*] Fincra Export")
    print(f"[*] Date range  : {START_DATE}  ->  {END_DATE}")
    print(f"[*] Pay-In file : {PAYIN_FILENAME}")
    print(f"[*] Pay-Out file: {PAYOUT_FILENAME}")
    print(f"[*] S3 enabled  : {S3_ENABLED}")
    if S3_ENABLED:
        print(f"[*] S3 payin    : s3://{S3_BUCKET}/{S3_PAYIN_PREFIX}{PAYIN_FILENAME}")
        print(f"[*] S3 payout   : s3://{S3_BUCKET}/{S3_PAYOUT_PREFIX}{PAYOUT_FILENAME}")
    print("=" * 60)

    IS_CI   = os.environ.get("CI", "false").lower() == "true"
    SLOW_MO = 0 if IS_CI else 80

    payin_path  = None
    payout_path = None

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=IS_CI, slow_mo=SLOW_MO)
        context = await browser.new_context(
            accept_downloads=True,
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            await ensure_logged_in(page)
            payin_path  = await export_payins(page, context)
            payout_path = await export_payouts(page, context)

        except Exception as exc:
            msg = (f"Fincra export FAILED\n"
                   f"Period : {START_DATE} -> {END_DATE}\n"
                   f"Error  : {exc}")
            print(f"\n[!] {msg}")
            notify_slack(msg, color="danger")
            try:
                await ss(page, "error_final")
            except Exception:
                pass
            raise
        finally:
            await browser.close()

    # --- S3 upload ---
    payin_s3_uri  = None
    payout_s3_uri = None

    if S3_ENABLED:
        if payin_path and payin_path.exists():
            try:
                payin_s3_uri = upload_to_s3(payin_path, S3_PAYIN_PREFIX)
            except Exception as e:
                notify_slack(f"Fincra Pay-In S3 upload FAILED\nError: {e}", color="warning")

        if payout_path and payout_path.exists():
            try:
                payout_s3_uri = upload_to_s3(payout_path, S3_PAYOUT_PREFIX)
            except Exception as e:
                notify_slack(f"Fincra Pay-Out S3 upload FAILED\nError: {e}", color="warning")
    else:
        print("[s3] S3_ENABLED=false — skipping upload.")

    # --- Slack summary ---
    lines = [
        f"*Fincra Export Complete*",
        f"Period : `{START_DATE}` → `{END_DATE}`",
    ]
    if payin_path and payin_path.exists():
        size_kb = payin_path.stat().st_size // 1024
        lines.append(f"Pay-In  : `{payin_path.name}` ({size_kb} KB)")
        if payin_s3_uri:
            lines.append(f"Pay-In S3 : `{payin_s3_uri}`")
    else:
        lines.append(f"Pay-In  : no data for this period")

    if payout_path and payout_path.exists():
        size_kb = payout_path.stat().st_size // 1024
        lines.append(f"Pay-Out : `{payout_path.name}` ({size_kb} KB)")
        if payout_s3_uri:
            lines.append(f"Pay-Out S3: `{payout_s3_uri}`")
    else:
        lines.append(f"Pay-Out : no data for this period")

    color = "good" if (payin_path or payout_path) else "warning"
    notify_slack("\n".join(lines), color=color)

    print(f"\n[+] All done!")
    if payin_path:
        print(f"    Pay-In  : {payin_path.resolve()}")
        if payin_s3_uri:
            print(f"    Pay-In S3 : {payin_s3_uri}")
    if payout_path:
        print(f"    Pay-Out : {payout_path.resolve()}")
        if payout_s3_uri:
            print(f"    Pay-Out S3: {payout_s3_uri}")


if __name__ == "__main__":
    asyncio.run(main())
