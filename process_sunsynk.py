"""
process_sunsynk.py

Reads the current and previous sunsynk snapshots, calculates the hourly
generation delta, runs status checks, fires Telegram alerts, and writes
data/processed.json in exactly the same format as process_plant_data.py
so the same dashboard (index.html) works unchanged.

How hourly data works:
  - Each run, download_sunsynk.py saves:
      data/sunsynk_snapshot.json       ← current reading
      data/sunsynk_snapshot_prev.json  ← previous reading (from last run)
  - This script calculates:
      delta_kwh = current.total_kwh - prev.total_kwh
    and stores it in hourly_pv[current_hour].
  - Cumulative hourly_pv is rebuilt from all deltas stored in
      data/sunsynk_hourly.json         ← persisted across runs today
  - At midnight (date change), hourly_pv resets.

This script is IDENTICAL across all Sunsynk sites.
The only values that change per site:
  - DAILY_EXPECTED_KWH and DAILY_LOW_KWH (edit directly below)
  - GitHub secrets: PLANT_NAME, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import json
import math
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# =============================================================================
# ✏️  SITE THRESHOLDS — edit directly here, do NOT set as GitHub secrets
# =============================================================================
DAILY_EXPECTED_KWH = 500.0    # Average good day for this site (kWh)
DAILY_LOW_KWH      = 100.0    # Known worst/low production day (kWh)

# =============================================================================
# 🔒 SECRETS — set in GitHub repo Settings → Secrets → Actions
# =============================================================================
PLANT_NAME         = os.environ.get("PLANT_NAME", "Kirkwood Tops")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

# =============================================================================
# FIXED CONFIG
# =============================================================================
PACE_THRESHOLD_PCT = 0.30
OFFLINE_THRESHOLD  = 0.01

SAST         = timezone(timedelta(hours=2))
_HERE        = Path(__file__).parent
SNAPSHOT     = _HERE / "data" / "sunsynk_snapshot.json"
PREV_SNAP    = _HERE / "data" / "sunsynk_snapshot_prev.json"
HOURLY_FILE  = _HERE / "data" / "sunsynk_hourly.json"
OUTPUT_FILE  = _HERE / "data" / "processed.json"
STATE_FILE   = _HERE / "data" / "alert_state.json"


# =============================================================================
# Solar curve (identical to process_plant_data.py)
# =============================================================================

def solar_window(month: int) -> tuple:
    mid_day   = (month - 1) * 30 + 15
    amplitude = 0.75
    angle     = 2 * math.pi * (mid_day - 355) / 365
    shift     = amplitude * math.cos(angle)
    return 6.0 - shift, 18.0 + shift


def solar_curve_fraction(hour: int, month: int) -> float:
    sunrise, sunset = solar_window(month)
    solar_day = sunset - sunrise
    if solar_day <= 0:
        return 0.0
    elapsed = (hour + 1) - sunrise
    if elapsed <= 0:
        return 0.0
    if elapsed >= solar_day:
        return 1.0
    return (1 - math.cos(math.pi * elapsed / solar_day)) / 2


# =============================================================================
# Hourly delta calculation
# =============================================================================

def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  ⚠️  Could not load {path}: {e}")
        return None


def build_hourly(current: dict, prev: dict | None, today: str) -> list:
    """
    Load persisted hourly accumulator for today, then apply the latest delta.
    Returns a 24-element list of kWh per hour.
    """
    # Load today's hourly accumulator (reset if date changed)
    acc_data = load_json(HOURLY_FILE)
    if acc_data and acc_data.get("date") == today:
        hourly = acc_data["hourly"]
    else:
        print(f"  ℹ️  New day ({today}) — resetting hourly accumulator")
        hourly = [0.0] * 24

    current_hour = current["hour"]

    # Calculate delta from previous snapshot
    if prev is None:
        print("  ℹ️  No previous snapshot — delta = 0 for this run")
        delta = 0.0
    elif prev.get("date") != today:
        print(f"  ℹ️  Previous snapshot is from {prev.get('date')} — delta = 0 (day rollover)")
        delta = 0.0
    else:
        delta = current["total_kwh"] - prev["total_kwh"]
        if delta < 0:
            print(f"  ⚠️  Negative delta ({delta:.3f} kWh) — clamping to 0")
            delta = 0.0
        print(f"  ⚡ Delta this run: {delta:.3f} kWh → hour {current_hour:02d}:00")

    # Accumulate into this hour's slot
    hourly[current_hour] = round(hourly[current_hour] + delta, 4)

    # Persist updated accumulator
    HOURLY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HOURLY_FILE, "w") as f:
        json.dump({"date": today, "hourly": hourly}, f, indent=2)
    print(f"  💾 Hourly accumulator saved: {HOURLY_FILE}")

    return hourly


# =============================================================================
# Status checks (identical logic to process_plant_data.py)
# =============================================================================

def determine_status(total: float, last_hour: int, month: int) -> tuple:
    alerts = {"offline": False, "pace_low": False, "total_low": False}
    sunrise, sunset = solar_window(month)

    if total < OFFLINE_THRESHOLD:
        alerts["offline"] = True
        return "offline", alerts, {
            "reason": "no generation detected",
            "curve_fraction": 0.0, "expected_by_now": 0.0,
            "pace_trigger": 0.0, "projected_total": 0.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    curve_frac = solar_curve_fraction(last_hour, month)

    if curve_frac < 0.10:
        return "ok", alerts, {
            "reason": "too early to assess",
            "curve_fraction": round(curve_frac, 3),
            "expected_by_now": round(DAILY_EXPECTED_KWH * curve_frac, 1),
            "pace_trigger": 0.0, "projected_total": 0.0,
            "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        }

    expected_by_now = DAILY_EXPECTED_KWH * curve_frac
    pace_trigger    = expected_by_now * PACE_THRESHOLD_PCT
    projected_total = total / curve_frac

    if total < pace_trigger:
        alerts["pace_low"] = True
    if projected_total < DAILY_LOW_KWH:
        alerts["total_low"] = True

    debug = {
        "curve_fraction":  round(curve_frac, 3),
        "expected_by_now": round(expected_by_now, 1),
        "actual_kwh":      round(total, 2),
        "pace_trigger":    round(pace_trigger, 1),
        "projected_total": round(projected_total, 1),
        "low_day_kwh":     DAILY_LOW_KWH,
        "sunrise":         round(sunrise, 2),
        "sunset":          round(sunset, 2),
        "checks": {
            "pace_low":  alerts["pace_low"],
            "total_low": alerts["total_low"],
        },
    }

    status = "low" if (alerts["pace_low"] or alerts["total_low"]) else "ok"
    return status, alerts, debug


# =============================================================================
# Telegram (identical to process_plant_data.py)
# =============================================================================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("  ⚠️  Telegram not configured — skipping")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            print("  ✅ Telegram alert sent")
            return True
        print(f"  ❌ Telegram error {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        print(f"  ❌ Telegram request failed: {e}")
        return False


def send_alerts(status: str, alerts: dict, total: float, last_hour: int, debug: dict):
    now_str         = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    expected_by_now = debug.get("expected_by_now", 0)
    projected_total = debug.get("projected_total", 0)

    prev_status = "ok"
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    if alerts["offline"]:
        send_telegram(
            f"🔴 <b>{PLANT_NAME} — OFFLINE</b>\n"
            f"No generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {last_hour:02d}:00)\n"
            f"🕐 {now_str}"
        )
    else:
        if alerts["pace_low"]:
            send_telegram(
                f"🟡 <b>{PLANT_NAME} — LOW PACE</b>\n"
                f"Generation is well behind the expected curve.\n"
                f"Actual so far:   <b>{total:.1f} kWh</b>\n"
                f"Expected by now: <b>~{expected_by_now:.0f} kWh</b>\n"
                f"Hour: {last_hour:02d}:00 | 🕐 {now_str}"
            )
        if alerts["total_low"]:
            send_telegram(
                f"🟠 <b>{PLANT_NAME} — POOR DAY PROJECTED</b>\n"
                f"At current pace, today will finish below the known low day.\n"
                f"Actual so far:     <b>{total:.1f} kWh</b>\n"
                f"Projected end-day: <b>~{projected_total:.0f} kWh</b>\n"
                f"Known low day:     <b>{DAILY_LOW_KWH:.0f} kWh</b>\n"
                f"Hour: {last_hour:02d}:00 | 🕐 {now_str}"
            )
        if status == "ok" and prev_status in ("low", "offline"):
            send_telegram(
                f"✅ <b>{PLANT_NAME} — RECOVERED</b>\n"
                f"System is back within normal range.\n"
                f"Total today: <b>{total:.1f} kWh</b> (as of {last_hour:02d}:00)\n"
                f"🕐 {now_str}"
            )
        if not alerts["pace_low"] and not alerts["total_low"] and status == "ok":
            print(f"  ✅ All checks passed — no alert needed")

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing Sunsynk: {PLANT_NAME}")
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    current = load_json(SNAPSHOT)
    if not current:
        print(f"❌ Snapshot not found: {SNAPSHOT}")
        sys.exit(1)

    prev    = load_json(PREV_SNAP)
    now     = datetime.now(SAST)
    month   = now.month
    today   = now.strftime("%Y-%m-%d")
    sunrise, sunset = solar_window(month)

    total     = current["total_kwh"]
    last_hour = current["hour"]

    print(f"  📅 Date:      {today}")
    print(f"  ⚡ Total kWh: {total}")
    print(f"  🕐 Hour:      {last_hour:02d}:00")
    if prev:
        print(f"  📦 Prev snap: {prev.get('timestamp','?')} → {prev.get('total_kwh','?')} kWh")

    hourly = build_hourly(current, prev, today)

    status, alerts, debug = determine_status(total, last_hour, month)

    print(f"  🌅 Solar window:    {sunrise:.1f}h – {sunset:.1f}h")
    print(f"  📈 Curve fraction:  {debug.get('curve_fraction', 0.0):.1%}")
    print(f"  🎯 Expected by now: {debug.get('expected_by_now', 0.0):.1f} kWh")
    print(f"  📉 Pace trigger:    {debug.get('pace_trigger', 0.0):.1f} kWh → pace_low={alerts['pace_low']}")
    print(f"  📊 Projected total: {debug.get('projected_total', 0.0):.1f} kWh → total_low={alerts['total_low']}")
    print(f"  🚦 Status:          {status.upper()}")

    send_alerts(status, alerts, total, last_hour, debug)

    output = {
        "plant":        PLANT_NAME,
        "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
        "date":         today,
        "total_kwh":    total,
        "last_hour":    last_hour,
        "status":       status,
        "alerts":       alerts,
        "thresholds": {
            "expected_daily_kwh": DAILY_EXPECTED_KWH,
            "low_day_kwh":        DAILY_LOW_KWH,
            "pace_threshold_pct": PACE_THRESHOLD_PCT,
            "solar_window": {
                "sunrise": round(sunrise, 2),
                "sunset":  round(sunset,  2),
            },
        },
        "debug":     debug,
        "hourly_pv": hourly,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✅ Saved: {OUTPUT_FILE}")
    print("✅ Done!")


if __name__ == "__main__":
    main()
