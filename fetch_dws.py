"""
fetch_dws.py
------------
Scrapes all 6 WMAs from the DWS Unverified Hydrology page and saves
one JSON file per WMA into the data/ folder.

Runs as a GitHub Action every hour.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────

BASE_URL = "https://www.dws.gov.za/Hydrology/Unverified/"

# The DWS page uses a POST or query param to select the WMA.
# Each WMA has a radio button value on the page.
# We'll try fetching with the WMA parameter directly.
WMA_PARAMS = {
    "WMA1": {"wma": "1", "name": "Limpopo-Olifants",    "prefixes": ["A", "B"]},
    "WMA2": {"wma": "2", "name": "Inkomati-Usuthu",     "prefixes": ["X", "W"]},
    "WMA3": {"wma": "3", "name": "Pongola-Mtamvuna",    "prefixes": ["V", "T"]},
    "WMA4": {"wma": "4", "name": "Vaal-Orange",          "prefixes": ["C", "D"]},
    "WMA5": {"wma": "5", "name": "Mzimvubu-Tsitsikama", "prefixes": ["E", "F", "G", "H", "J", "K"]},
    "WMA6": {"wma": "6", "name": "Breede-Olifants",     "prefixes": ["L", "M", "N", "P", "Q", "R", "S"]},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DWS-Hydrology-Bot/1.0; +https://waterresearchobservatory.org)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-ZA,en;q=0.9",
}

OUTPUT_DIR = Path("data")


# ── Helpers ───────────────────────────────────────────────────────────────

def clean_float(text: str) -> float:
    """Parse a possibly comma-formatted float string."""
    try:
        return float(text.replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0.0


def is_dam(station_code: str) -> bool:
    """Station codes with R followed by digits indicate a reservoir/dam."""
    return bool(re.search(r"R\d", station_code, re.IGNORECASE))


def parse_table(html: str, prefixes: list[str]) -> list[dict]:
    """Extract station rows from DWS HTML, filtered by station code prefix."""
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
        if not station or not any(station.upper().startswith(p) for p in prefixes):
            continue

        # Blue rows = dams (DWS uses inline style or class)
        row_style = tr.get("style", "") + tr.get("class", [""])[0] if tr.get("class") else tr.get("style", "")

        place    = tds[1].get_text(strip=True) if len(tds) > 1 else ""
        datetime_str = tds[2].get_text(strip=True) if len(tds) > 2 else ""
        stage    = clean_float(tds[3].get_text()) if len(tds) > 3 else 0.0
        flow     = clean_float(tds[4].get_text()) if len(tds) > 4 else 0.0
        spill    = clean_float(tds[5].get_text()) if len(tds) > 5 else 0.0
        comment  = tds[6].get_text(strip=True)    if len(tds) > 6 else ""

        rows.append({
            "station":  station,
            "place":    place,
            "datetime": datetime_str,
            "stage":    stage,
            "flow":     flow,
            "spill":    spill,
            "comment":  comment,
            "isDam":    is_dam(station),
        })

    return rows


def fetch_wma(wma_key: str, config: dict) -> list[dict]:
    """Fetch a single WMA page from DWS, trying multiple URL patterns."""
    prefixes = config["prefixes"]

    # Strategy 1: POST with __EVENTTARGET to select the WMA radio button
    # (DWS uses ASP.NET WebForms with ViewState)
    strategies = [
        # Try direct GET with RadioButtonList selection via query string
        {"method": "GET",  "url": BASE_URL, "params": {"WMA": config["wma"]}},
        # Try POST mimicking the radio button click
        {"method": "POST", "url": BASE_URL, "data": {
            "__EVENTTARGET": "RadioButtonList1",
            "__EVENTARGUMENT": str(int(config["wma"]) - 1),
            "RadioButtonList1": str(int(config["wma"]) - 1),
        }},
        # Fallback: plain GET (returns WMA4 default but we filter by prefix)
        {"method": "GET",  "url": BASE_URL, "params": {}},
    ]

    for strategy in strategies:
        try:
            if strategy["method"] == "GET":
                resp = requests.get(
                    strategy["url"],
                    params=strategy.get("params", {}),
                    headers=HEADERS,
                    timeout=20,
                )
            else:
                # Need ViewState — first do a GET to grab it
                init = requests.get(BASE_URL, headers=HEADERS, timeout=20)
                soup_init = BeautifulSoup(init.text, "html.parser")
                viewstate = soup_init.find("input", {"name": "__VIEWSTATE"})
                evvalidation = soup_init.find("input", {"name": "__EVENTVALIDATION"})
                post_data = strategy["data"].copy()
                if viewstate:
                    post_data["__VIEWSTATE"] = viewstate.get("value", "")
                if evvalidation:
                    post_data["__EVENTVALIDATION"] = evvalidation.get("value", "")
                resp = requests.post(
                    strategy["url"],
                    data=post_data,
                    headers=HEADERS,
                    timeout=20,
                )

            resp.raise_for_status()
            stations = parse_table(resp.text, prefixes)
            if stations:
                print(f"  ✓ {wma_key}: {len(stations)} stations via {strategy['method']}")
                return stations
            else:
                print(f"  ↩ {wma_key}: 0 stations with {strategy['method']}, trying next…")

        except Exception as e:
            print(f"  ✗ {wma_key} strategy failed: {e}")

        time.sleep(1)  # be polite between retries

    print(f"  ⚠ {wma_key}: all strategies failed, returning empty list")
    return []


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    fetched_at = datetime.now(timezone.utc).isoformat()
    summary = {}

    for wma_key, config in WMA_PARAMS.items():
        print(f"\nFetching {wma_key} ({config['name']})…")
        stations = fetch_wma(wma_key, config)

        payload = {
            "wma":        wma_key,
            "name":       config["name"],
            "fetched_at": fetched_at,
            "count":      len(stations),
            "stations":   stations,
        }

        out_path = OUTPUT_DIR / f"{wma_key.lower()}.json"
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        print(f"  → Saved {out_path} ({len(stations)} stations)")
        summary[wma_key] = len(stations)

    # Write a summary index
    index_path = OUTPUT_DIR / "index.json"
    index_path.write_text(json.dumps({
        "fetched_at": fetched_at,
        "wmas": summary,
    }, indent=2))
    print(f"\n✅ Done. Summary: {summary}")
    print(f"   Written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
