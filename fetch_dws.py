"""
fetch_dws.py
------------
Uses Playwright (headless Chromium) to load the DWS Unverified Hydrology
page, click each WMA radio button, wait for the table to update, then
scrape the station data.

This is the only reliable way to get all 6 WMAs since the DWS site
renders each WMA's data via JavaScript after a radio button click.

Runs via GitHub Actions twice daily (06:15 and 18:15 UTC).

We are guests on DWS's server. We:
  - Identify ourselves honestly via User-Agent
  - Wait politely between each WMA tab click (5 seconds)
  - Run only twice a day
  - Contact: waterresearchobservatory.org
"""

import json
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

DELAY_BETWEEN_WMAS = 5   # seconds between clicking each WMA tab
PAGE_LOAD_TIMEOUT  = 30000  # ms to wait for page load
TABLE_TIMEOUT      = 10000  # ms to wait for table to update after click

print("Python version:", sys.version)
print("Starting DWS scraper (Playwright headless browser mode)...")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    print("playwright imported OK")
except ImportError as e:
    print("ERROR: playwright not installed:", e)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────

BASE_URL = "https://www.dws.gov.za/Hydrology/Unverified/"

WMA_CONFIG = {
    "WMA1": {"name": "Limpopo-Olifants",    "prefixes": ["A", "B"],              "label": "Limpopo-Olifants"},
    "WMA2": {"name": "Inkomati-Usuthu",     "prefixes": ["X", "W"],              "label": "Inkomati-Usuthu"},
    "WMA3": {"name": "Pongola-Mtamvuna",    "prefixes": ["V", "T"],              "label": "Pongola-Mtamvuna"},
    "WMA4": {"name": "Vaal-Orange",          "prefixes": ["C", "D"],              "label": "Vaal-Orange"},
    "WMA5": {"name": "Mzimvubu-Tsitsikama", "prefixes": ["E", "F", "G", "H", "J", "K"], "label": "Mzimvubu-Tsitsikama"},
    "WMA6": {"name": "Breede-Olifants",     "prefixes": ["L", "M", "N", "P", "Q", "R", "S"], "label": "Breede-Olifants"},
}

OUTPUT_DIR = Path("data")

# ── Helpers ───────────────────────────────────────────────────────────────

def clean_float(text):
    try:
        return float(str(text).replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0.0

def is_dam(station_code):
    return bool(re.search(r"R\d", station_code, re.IGNORECASE))

def wma_for_station(code):
    code = code.upper()
    for wma_key, config in WMA_CONFIG.items():
        if any(code.startswith(p.upper()) for p in config["prefixes"]):
            return wma_key
    return None

def parse_table_html(html, expected_prefixes):
    """Parse station rows from HTML, filtering by expected WMA prefixes."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        link = tds[0].find("a")
        if not link:
            continue

        station = link.get_text(strip=True)
        if not station or not re.match(r"^[A-Z]\d", station, re.IGNORECASE):
            continue
        if not any(station.upper().startswith(p.upper()) for p in expected_prefixes):
            continue

        place    = tds[1].get_text(strip=True) if len(tds) > 1 else ""
        dt_str   = tds[2].get_text(strip=True) if len(tds) > 2 else ""
        stage    = clean_float(tds[3].get_text()) if len(tds) > 3 else 0.0
        flow     = clean_float(tds[4].get_text()) if len(tds) > 4 else 0.0
        spill    = clean_float(tds[5].get_text()) if len(tds) > 5 else 0.0
        comment  = tds[6].get_text(strip=True)    if len(tds) > 6 else ""

        rows.append({
            "station":  station,
            "place":    place,
            "datetime": dt_str,
            "stage":    stage,
            "flow":     flow,
            "spill":    spill,
            "comment":  comment,
            "isDam":    is_dam(station),
        })

    return rows

# ── Main scraper ──────────────────────────────────────────────────────────

def scrape_all_wmas():
    """Use Playwright to load each WMA tab and scrape the station table."""
    results = {}

    with sync_playwright() as p:
        print("\nLaunching headless Chromium...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "DWS-Hydrology-Monitor/1.0 "
                "(Water Research Observatory; +https://waterresearchobservatory.org; "
                "runs twice daily; respectful scraper)"
            )
        )
        page = context.new_page()

        # ── Load the page ─────────────────────────────────────────────────
        print(f"Loading {BASE_URL}...")
        try:
            page.goto(BASE_URL, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
            print(f"  Page loaded: {page.title()}")
        except PlaywrightTimeout:
            print("  Timeout loading page — trying with domcontentloaded...")
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)

        # ── Find the WMA radio buttons ────────────────────────────────────
        print("\nLooking for WMA radio buttons...")
        radio_buttons = page.query_selector_all("input[type='radio']")
        print(f"  Found {len(radio_buttons)} radio buttons")

        if len(radio_buttons) == 0:
            print("  ERROR: No radio buttons found — page may not have loaded correctly")
            print(f"  Page HTML snippet: {page.content()[:500]}")
            browser.close()
            return {}

        # ── Click each WMA radio button and scrape ────────────────────────
        wma_keys = list(WMA_CONFIG.keys())

        for idx, wma_key in enumerate(wma_keys):
            config = WMA_CONFIG[wma_key]
            print(f"\nScraping {wma_key} ({config['name']})...")

            if idx >= len(radio_buttons):
                print(f"  WARNING: No radio button at index {idx}, skipping")
                results[wma_key] = []
                continue

            try:
                # Click the radio button for this WMA
                radio_buttons[idx].click()
                print(f"  Clicked radio button {idx}")

                # Wait for the table to update
                page.wait_for_timeout(TABLE_TIMEOUT)

                # Get the updated page HTML
                html = page.content()
                stations = parse_table_html(html, config["prefixes"])
                print(f"  → {len(stations)} stations found for {wma_key}")
                results[wma_key] = stations

            except Exception as e:
                print(f"  ERROR scraping {wma_key}: {e}")
                results[wma_key] = []

            # Polite pause between WMA clicks
            if idx < len(wma_keys) - 1:
                print(f"  ⏳ Waiting {DELAY_BETWEEN_WMAS}s before next WMA...")
                import time
                time.sleep(DELAY_BETWEEN_WMAS)

        browser.close()
        print("\nBrowser closed.")

    return results

# ── Main ──────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    fetched_at = datetime.now(timezone.utc).isoformat()

    print(f"Fetch time (UTC): {fetched_at}")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")

    results = scrape_all_wmas()

    if not results:
        print("FATAL: No data returned from scraper.")
        sys.exit(1)

    # ── Save one JSON file per WMA ────────────────────────────────────────
    print("\nSaving JSON files...")
    summary = {}

    for wma_key, config in WMA_CONFIG.items():
        stations = results.get(wma_key, [])
        payload = {
            "wma":        wma_key,
            "name":       config["name"],
            "fetched_at": fetched_at,
            "count":      len(stations),
            "stations":   stations,
        }
        out_path = OUTPUT_DIR / f"{wma_key.lower()}.json"
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        summary[wma_key] = len(stations)
        status = "✓" if len(stations) > 0 else "⚠ EMPTY"
        print(f"  {wma_key}: {len(stations)} stations {status} → {out_path}")

    # ── Summary index ─────────────────────────────────────────────────────
    index_path = OUTPUT_DIR / "index.json"
    index_path.write_text(json.dumps({
        "fetched_at": fetched_at,
        "schedule":   "Twice daily — 06:15 and 18:15 UTC (08:15 and 20:15 SAST)",
        "source":     BASE_URL,
        "method":     "Playwright headless Chromium",
        "wmas":       summary,
    }, indent=2))

    print("\n" + "="*50)
    print("COMPLETE. Results:")
    total = sum(summary.values())
    for k, v in summary.items():
        print(f"  {k}: {v} stations")
    print(f"  Total: {total} stations")
    print("="*50)

    if total == 0:
        print("FATAL: 0 stations across all WMAs.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
