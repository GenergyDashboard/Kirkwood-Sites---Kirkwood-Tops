"""
download_sunsynk.py

Scrapes the current day's total PV generation (kWh) from sunsynk.net
for a configured plant, and saves it as data/sunsynk_snapshot.json.

The snapshot is a simple timestamped reading:
  { "total_kwh": 42.3, "timestamp": "2026-03-13T09:00:00+02:00", "date": "2026-03-13" }

process_sunsynk.py then compares the current snapshot to the previous
one to derive the hourly generation delta.

Environment variables / GitHub secrets:
  SUNSYNK_USERNAME   - your sunsynk.net email
  SUNSYNK_PASSWORD   - your sunsynk.net password
  PLANT_NAME         - plant to search for (e.g. "kirkwood tops")
"""

import json
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

# =============================================================================
# ✏️  SITE CONFIGURATION
# =============================================================================
SITE_CONFIG = {
    "plant_name": os.environ.get("PLANT_NAME", "kirkwood tops"),
}

SAST        = timezone(timedelta(hours=2))
_HERE       = Path(__file__).parent
SNAPSHOT    = _HERE / "data" / "sunsynk_snapshot.json"
LOGIN_URL   = "https://sunsynk.net/login"


def human_delay(min_s=1.5, max_s=4.0):
    delay = random.uniform(min_s, max_s)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


def scrape_total_kwh() -> float:
    """
    Log in to sunsynk.net, search for the plant, and return today's
    total kWh from the  <div class="cell el-tooltip">  cell in the
    plant list row.
    """
    username = os.environ.get("SUNSYNK_USERNAME")
    password = os.environ.get("SUNSYNK_PASSWORD")
    if not username or not password:
        print("❌ SUNSYNK_USERNAME and SUNSYNK_PASSWORD must be set")
        sys.exit(1)

    plant_name = SITE_CONFIG["plant_name"]
    print(f"🚀 Scraping Sunsynk for plant: '{plant_name}'")
    print(f"🔐 Username: {username[:4]}***")

    with sync_playwright() as pw:
        print("🌐 Launching browser (headless)...")
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Johannesburg",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = context.new_page()

        try:
            # ── Step 1: Login ──────────────────────────────────────────────
            print("📱 Step 1: Navigating to Sunsynk login...")
            page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
            human_delay(2, 4)

            print("👤 Step 2: Entering credentials...")
            page.get_by_role("textbox", name="Please input your E-mail").fill(username)
            human_delay(0.5, 1.5)
            page.get_by_role("textbox", name="Please re-enter password").fill(password)
            human_delay(0.5, 1.5)
            page.get_by_role("button", name="Login").click()
            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(3, 5)
            print(f"  📍 After login: {page.url[:80]}")

            # ── Step 2: Search for plant ───────────────────────────────────
            print(f"🔎 Step 3: Searching for '{plant_name}'...")
            search = page.get_by_role("textbox", name="Plant Name")
            search.click()
            human_delay(0.5, 1)
            search.fill(plant_name)
            human_delay(0.5, 1)

            # Click search / press Enter
            try:
                page.locator(".el-button.el-button--primary").first.click()
            except Exception:
                search.press("Enter")

            page.wait_for_load_state("networkidle", timeout=30000)
            human_delay(3, 5)

            # ── Step 3: Read the kWh cell ──────────────────────────────────
            # The plant list row contains  <div class="cell el-tooltip">16</div>
            # There may be multiple such cells per row (one per column).
            # The kWh today value is the one that is a valid positive number.
            # We collect all visible cell values and pick the numeric one
            # that looks like a reasonable daily kWh reading.
            print("⚡ Step 4: Reading kWh cell...")

            page.wait_for_selector("div.cell.el-tooltip", timeout=20000)
            cells = page.locator("div.cell.el-tooltip")
            count = cells.count()
            print(f"  ℹ️  Found {count} cell(s) on page")

            kwh_value = None
            for i in range(count):
                text = cells.nth(i).inner_text().strip()
                try:
                    val = float(text)
                    # Sanity check: daily kWh should be 0–9999
                    if 0.0 <= val <= 9999.0:
                        print(f"  ✅ Cell [{i}] = '{text}' → {val} kWh")
                        kwh_value = val
                        # Don't break — take the LAST valid numeric cell
                        # in case the first ones are row numbers or status codes
                except ValueError:
                    print(f"  ·  Cell [{i}] = '{text}' (not numeric, skipping)")

            if kwh_value is None:
                raise RuntimeError(
                    "Could not find a numeric kWh value in any 'div.cell.el-tooltip' element"
                )

            print(f"✅ Total kWh today: {kwh_value}")
            return kwh_value

        except Exception as err:
            print(f"❌ Scrape failed: {err}")
            try:
                page.screenshot(path="error_screenshot.png", full_page=True)
                Path("error_page.html").write_text(page.content())
                print("📸 Debug files saved: error_screenshot.png, error_page.html")
            except Exception:
                pass
            raise

        finally:
            context.close()
            browser.close()
            print("🔒 Browser closed")


def save_snapshot(total_kwh: float):
    """Save the current reading as a timestamped snapshot."""
    now  = datetime.now(SAST)
    snap = {
        "total_kwh": total_kwh,
        "timestamp": now.isoformat(),
        "date":      now.strftime("%Y-%m-%d"),
        "hour":      now.hour,
    }
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)

    # Keep previous snapshot before overwriting
    prev_snap = None
    if SNAPSHOT.exists():
        try:
            with open(SNAPSHOT) as f:
                prev_snap = json.load(f)
        except Exception:
            pass

    # Save current as latest, and archive previous alongside it
    with open(SNAPSHOT, "w") as f:
        json.dump(snap, f, indent=2)

    prev_file = SNAPSHOT.parent / "sunsynk_snapshot_prev.json"
    if prev_snap:
        with open(prev_file, "w") as f:
            json.dump(prev_snap, f, indent=2)
        print(f"📦 Previous snapshot saved: {prev_file}")

    print(f"✅ Snapshot saved: {SNAPSHOT}")
    print(f"   total_kwh : {snap['total_kwh']}")
    print(f"   timestamp : {snap['timestamp']}")


if __name__ == "__main__":
    try:
        total = scrape_total_kwh()
        save_snapshot(total)
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
