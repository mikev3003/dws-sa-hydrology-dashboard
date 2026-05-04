"""
fetch_dws.py
------------
Uses Playwright (headless Chromium) to load the DWS Unverified Hydrology
page, click each WMA radio button, wait for the table to update, then
scrape the station data.

Runs via GitHub Actions twice daily (06:15 and 18:15 UTC / 08:15 and 20:15 SAST).

We are guests on DWS's server. We:
  - Identify ourselves honestly via User-Agent
  - Wait politely between each WMA tab click
  - Run only twice a day
  - Contact: waterresearchobservatory.org
"""

import json
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

print("Python version:", sys.version)
print("Starting DWS scraper (Playwright headless browser mode)...")

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    print("playwright imported OK")
except ImportError as e:
    print("ERROR: playwright not installed:", e)
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
    print("beautifulsoup4 imported OK")
except ImportError as e:
    print("ERROR: beautifulsoup4 not installed:", e)
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────

BASE_URL = "https://www.dws.gov.za/Hydrology/Unverified/"

WMA_CONFIG = {
    "WMA1": {"name": "Limpopo-Olifants",    "prefixes": ["A", "B"]},
    "WMA2": {"name": "Inkomati-Usuthu",     "prefixes": ["X", "W"]},
    "WMA3": {"name": "Pongola-Mtamvuna",    "prefixes": ["V", "T"]},
    "WMA4": {"name": "Vaal-Orange",          "prefixes": ["C", "D"]},
    "WMA5": {"name": "Mzimvubu-Tsitsikama", "prefixes": ["E", "F", "G", "H", "J", "K"]},
    "WMA6": {"name": "Breede-Olifants",     "prefixes": ["L", "M", "N", "P", "Q", "R", "S"]},
}

DELAY_BETWEEN_WMAS  = 5      # polite pause between WMA clicks (seconds)
WAIT_AFTER_CLICK    = 8000   # ms to wait after clicking a radio button
PAGE_LOAD_WAIT      = 5000   # ms extra wait after page load for JS to settle
OUTPUT_DIR          = Path("data")

# ── Helpers ───────────────────────────────────────────────────────────────

def clean_float(text):
    try:
        return float(str(text).replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0.0

def is_dam(station_code):
    return bool(re.search(r"R\d", station_code, re.IGNORECASE))

def parse_table(html, prefixes):
    """Parse station rows from page HTML, filtered by WMA prefix."""
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
        if not any(station.upper().startswith(p.upper()) for p in prefixes):
            continue
        rows.append({
            "station":  station,
            "place":    tds[1].get_text(strip=True) if len(tds) > 1 else "",
            "datetime": tds[2].get_text(strip=True) if len(tds) > 2 else "",
            "stage":    clean_float(tds[3].get_text()) if len(tds) > 3 else 0.0,
            "flow":     clean_float(tds[4].get_text()) if len(tds) > 4 else 0.0,
            "spill":    clean_float(tds[5].get_text()) if len(tds) > 5 else 0.0,
            "comment":  tds[6].get_text(strip=True)    if len(tds) > 6 else "",
            "isDam":    is_dam(station),
        })
    return rows

def debug_page(page, label=""):
    """Print useful debug info about the current page state."""
    print(f"\n  --- DEBUG {label} ---")
    # Count all inputs
    inputs = page.query_selector_all("input")
    print(f"  Total <input> elements: {len(inputs)}")
    for i, inp in enumerate(inputs[:10]):
        try:
            itype = inp.get_attribute("type") or "unknown"
            iname = inp.get_attribute("name") or ""
            iid   = inp.get_attribute("id") or ""
            ival  = inp.get_attribute("value") or ""
            print(f"    [{i}] type={itype} name={iname} id={iid} value={ival}")
        except:
            pass
    # Count table rows
    rows = page.query_selector_all("table tr")
    print(f"  Total <tr> elements: {len(rows)}")
    # Look for any links that look like station codes
    links = page.query_selector_all("table td a")
    print(f"  Total <td><a> links (potential stations): {len(links)}")
    if links:
        for lnk in links[:5]:
            try:
                print(f"    Sample link: {lnk.inner_text()}")
            except:
                pass
    print(f"  --- END DEBUG ---\n")

# ── Main scraper ──────────────────────────────────────────────────────────

def scrape_all_wmas():
    results = {}

    with sync_playwright() as p:
        print("\nLaunching headless Chromium...")
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36 "
                "DWS-Monitor/1.0 (+https://waterresearchobservatory.org)"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        # ── Load the page ─────────────────────────────────────────────────
        print(f"\nNavigating to {BASE_URL}...")
        try:
            page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
        except PlaywrightTimeout:
            print("  networkidle timeout — continuing with domcontentloaded...")
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)

        print(f"  Page title: {page.title()}")

        # Extra wait for JS to settle and ASP.NET UpdatePanels to init
        print(f"  Waiting {PAGE_LOAD_WAIT}ms for JavaScript to settle...")
        page.wait_for_timeout(PAGE_LOAD_WAIT)

        # Debug: inspect the page
        debug_page(page, "AFTER INITIAL LOAD")

        # ── Try to find radio buttons several ways ────────────────────────
        radio_buttons = page.query_selector_all("input[type='radio']")
        print(f"\nRadio buttons found: {len(radio_buttons)}")

        # If still no radio buttons, try waiting for them explicitly
        if len(radio_buttons) == 0:
            print("  No radio buttons yet — waiting up to 15s for them...")
            try:
                page.wait_for_selector("input[type='radio']", timeout=15000)
                radio_buttons = page.query_selector_all("input[type='radio']")
                print(f"  Found {len(radio_buttons)} radio buttons after wait")
            except PlaywrightTimeout:
                print("  Still no radio buttons after waiting.")

        # ── If we have radio buttons, click each WMA ───────────────────────
        if len(radio_buttons) > 0:
            wma_keys = list(WMA_CONFIG.keys())
            for idx, wma_key in enumerate(wma_keys):
                config = WMA_CONFIG[wma_key]
                print(f"\nScraping {wma_key} ({config['name']})...")

                if idx >= len(radio_buttons):
                    print(f"  WARNING: No radio button at index {idx}")
                    results[wma_key] = []
                    continue

                try:
                    radio_buttons[idx].click()
                    print(f"  Clicked radio button {idx}")
                    page.wait_for_timeout(WAIT_AFTER_CLICK)

                    html = page.content()
                    stations = parse_table(html, config["prefixes"])
                    print(f"  → {len(stations)} stations for {wma_key}")
                    results[wma_key] = stations

                except Exception as e:
                    print(f"  ERROR on {wma_key}: {e}")
                    results[wma_key] = []

                if idx < len(wma_keys) - 1:
                    print(f"  ⏳ Waiting {DELAY_BETWEEN_WMAS}s...")
                    time.sleep(DELAY_BETWEEN_WMAS)

        else:
            # ── Fallback: no radio buttons — scrape whatever is on the page ─
            print("\nNo radio buttons found — scraping default page content...")
            html = page.content()

            # Try all WMA prefixes against whatever stations are on the page
            all_stations = []
            soup = BeautifulSoup(html, "html.parser")
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
                all_stations.append({
                    "station":  station,
                    "place":    tds[1].get_text(strip=True) if len(tds) > 1 else "",
                    "datetime": tds[2].get_text(strip=True) if len(tds) > 2 else "",
                    "stage":    clean_float(tds[3].get_text()) if len(tds) > 3 else 0.0,
                    "flow":     clean_float(tds[4].get_text()) if len(tds) > 4 else 0.0,
                    "spill":    clean_float(tds[5].get_text()) if len(tds) > 5 else 0.0,
                    "comment":  tds[6].get_text(strip=True)    if len(tds) > 6 else "",
                    "isDam":    is_dam(station),
                })

            print(f"  Fallback: found {len(all_stations)} total stations on default page")

            # Split by prefix
            for wma_key, config in WMA_CONFIG.items():
                wma_stations = [
                    s for s in all_stations
                    if any(s["station"].upper().startswith(p.upper()) for p in config["prefixes"])
                ]
                results[wma_key] = wma_stations
                print(f"  {wma_key}: {len(wma_stations)} stations")

        browser.close()
        print("\nBrowser closed.")

    return results

# ── Main ──────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    fetched_at = datetime.now(timezone.utc).isoformat()

    print(f"\nFetch time (UTC): {fetched_at}")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")

    results = scrape_all_wmas()
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
        print(f"  {wma_key}: {len(stations)} stations {status}")

    # Summary
    (OUTPUT_DIR / "index.json").write_text(json.dumps({
        "fetched_at": fetched_at,
        "schedule":   "Twice daily — 06:15 and 18:15 UTC",
        "source":     BASE_URL,
        "method":     "Playwright headless Chromium",
        "wmas":       summary,
    }, indent=2))

    total = sum(summary.values())
    print(f"\nTotal stations across all WMAs: {total}")

    if total == 0:
        print("FATAL: 0 stations — DWS page may have changed or be down.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
