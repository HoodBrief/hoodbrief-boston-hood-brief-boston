"""
Hood Brief Boston — Multi-City Pipeline
========================================
Polls Boston BPD and Cambridge CPD incident data via Cloudflare Worker proxy.
No audio transcription — structured open data only. Zero AI cost.

Cities:
  Boston   — Analyze Boston CKAN API (~2 week lag)
  Cambridge — Socrata Daily Police Log (daily updates)
"""

import os
import re
import time
import json
import requests
import threading
from datetime import datetime, timezone, timedelta

# ── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")
WORKER_URL    = os.environ.get("WORKER_URL", "")
POLL_INTERVAL = 300   # 5 minutes
LOOKBACK_DAYS = 30    # days to look back on first run
ONGOING_DAYS  = 3     # days to look back on subsequent polls

# ── BPD District Info ────────────────────────────────────────────────────────
DISTRICTS = {
    "A1":  {"name": "Downtown/Charlestown",    "color": "#6366f1"},
    "A7":  {"name": "East Boston",             "color": "#8b5cf6"},
    "A15": {"name": "Charlestown",             "color": "#a78bfa"},
    "B2":  {"name": "Roxbury",                 "color": "#ec4899"},
    "B3":  {"name": "Mattapan",                "color": "#f43f5e"},
    "C6":  {"name": "South Boston",            "color": "#f97316"},
    "C11": {"name": "Dorchester",              "color": "#fb923c"},
    "D4":  {"name": "South End/Back Bay",      "color": "#14b8a6"},
    "D14": {"name": "Brighton/Allston",        "color": "#06b6d4"},
    "E5":  {"name": "West Roxbury",            "color": "#22c55e"},
    "E13": {"name": "Hyde Park",               "color": "#84cc16"},
    "E18": {"name": "Roslindale",              "color": "#eab308"},
    "CPD": {"name": "Cambridge PD",            "color": "#38bdf8"},
}

# ── Hotspots ─────────────────────────────────────────────────────────────────
BOSTON_HOTSPOTS = [
    {"name": "Roxbury",       "streets": ["blue hill ave", "dudley", "washington st", "warren st", "humboldt ave", "dale st"]},
    {"name": "Mattapan",      "streets": ["morton st", "blue hill ave", "mattapan sq", "river st", "cummins hwy"]},
    {"name": "Dorchester",    "streets": ["bowdoin st", "columbia rd", "geneva ave", "talbot ave", "washington st", "harvard st"]},
    {"name": "Hyde Park",     "streets": ["hyde park ave", "fairmount ave", "readville", "cleary sq"]},
    {"name": "East Boston",   "streets": ["meridian st", "chelsea st", "maverick sq", "paris st"]},
    {"name": "South Boston",  "streets": ["old colony", "silver st", "east broadway", "west broadway"]},
    {"name": "Jamaica Plain", "streets": ["egleston sq", "centre st", "jackson sq", "stony brook"]},
    {"name": "Mission Hill",  "streets": ["huntington ave", "tremont st", "brigham circle"]},
    {"name": "Area 4",        "streets": ["main st cambridge", "albany st", "columbia st", "mass ave cambridge"]},
]

def check_hotspot(street):
    if not street:
        return False, None
    sl = street.lower()
    for zone in BOSTON_HOTSPOTS:
        if any(s in sl for s in zone["streets"]):
            return True, zone["name"]
    return False, None

# ── Priority Classification ───────────────────────────────────────────────────
P1_KEYWORDS = [
    "homicide", "murder", "manslaughter", "shooting", "shot",
    "robbery", "armed", "weapon", "firearm", "gun", "knife",
    "assault with", "aggravated assault", "kidnap", "hostage",
    "rape", "sexual assault", "sex offense", "home invasion", "carjack",
]
MEDICAL_KEYWORDS = [
    "medical", "ambulance", "overdose", "unconscious", "unresponsive",
    "suicide", "self harm", "mental health", "psychiatric",
]
P2_DESC_KEYWORDS = [
    "burglary", "larceny", "vandal", "drug", "narcotic",
    "trespass", "missing", "assault", "threat", "harass",
    "stolen", "investigate", "restraining", "disorderly",
    "robbery", "fraud", "forgery", "weapon", "firearm",
    "auto theft", "motor vehicle", "breaking", "entering",
    "shoplifting", "warrant", "arrest", "disturbance",
    "property", "damage", "violation", "offenses",
    "val -", "m/v", "b&e", "uumv", "neighbor", "noise",
    "unwanted", "harassment", "fight", "argument", "domestic",
]

def classify(desc, shooting="0", ucr=""):
    desc_l = (desc or "").lower()
    if str(shooting) in ("Y", "1"): return "p1"
    if "part one" in (ucr or "").lower(): return "p1"
    if any(k in desc_l for k in P1_KEYWORDS): return "p1"
    if any(k in desc_l for k in MEDICAL_KEYWORDS): return "medical"
    if any(k in desc_l for k in P2_DESC_KEYWORDS): return "p2"
    if len(desc_l) > 3: return "p2"
    return "p3"

def make_title(desc, shooting="0"):
    if str(shooting) in ("Y", "1"): return "Shooting"
    return (desc or "Incident").strip().title()

# ── Supabase ──────────────────────────────────────────────────────────────────
def sb_upsert(rows):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/boston_incidents",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "resolution=ignore-duplicates,return=minimal",
        },
        json=rows,
        timeout=15,
    )
    if r.status_code not in (200, 201, 204):
        print(f"  [Supabase] {r.status_code}: {r.text[:150]}")

# ── Worker fetch ──────────────────────────────────────────────────────────────
def fetch_boston(since_dt):
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    r = requests.get(WORKER_URL, params={"city": "boston", "since": since_str, "limit": 200}, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        print(f"  [Boston API] Error: {data.get('error', {}).get('message','unknown')}")
        return []
    return data["result"]["records"]

def fetch_cambridge(since_dt):
    since_str = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
    r = requests.get(WORKER_URL, params={"city": "cambridge", "since": since_str, "limit": 200}, timeout=25)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "error" in data:
        print(f"  [Cambridge API] Error: {data['error']}")
        return []
    return data if isinstance(data, list) else []

# ── Process Boston incidents ──────────────────────────────────────────────────
def process_boston(rows, seen):
    saved = 0
    records = []
    for row in rows:
        inc_num = row.get("INCIDENT_NUMBER", "")
        if not inc_num or inc_num in seen:
            continue
        try:
            lat = float(row.get("Lat") or 0)
            lng = float(row.get("Long") or 0)
        except:
            continue
        if not lat or not lng or lat == -1:
            continue

        desc     = (row.get("OFFENSE_DESCRIPTION") or "").strip()
        shooting = str(row.get("SHOOTING") or "0")
        ucr      = row.get("UCR_PART") or ""
        priority = classify(desc, shooting, ucr)
        if priority == "p3":
            continue

        district  = (row.get("DISTRICT") or "").strip().upper()
        dist_info = DISTRICTS.get(district, {"name": district or "Unknown", "color": "#6b7d96"})
        street    = (row.get("STREET") or "").strip().title()
        hotspot, zone = check_hotspot(street)
        occurred  = row.get("OCCURRED_ON_DATE", "")

        records.append({
            "incident_number": f"BPD-{inc_num}",
            "title":           make_title(desc, shooting),
            "offense_group":   row.get("OFFENSE_CODE_GROUP") or "",
            "offense_desc":    desc,
            "location":        street,
            "lat":             lat,
            "lng":             lng,
            "priority":        priority,
            "district":        district,
            "district_name":   dist_info["name"],
            "shooting":        shooting in ("Y", "1"),
            "gang_hotspot":    hotspot,
            "gang_zone":       zone,
            "occurred_at":     occurred,
            "created_at":      datetime.now(timezone.utc).isoformat(),
        })
        seen.add(inc_num)
        saved += 1
        if len(records) >= 50:
            sb_upsert(records)
            records = []

    if records:
        sb_upsert(records)
    return saved

# ── Process Cambridge incidents ───────────────────────────────────────────────
def process_cambridge(rows, seen):
    saved = 0
    records = []
    for row in rows:
        # Cambridge fields: id, case_number, date_time, event, location,
        # neighborhood, lat, long (as :@computed_region fields or separate)
        case_num = str(row.get("case_number") or row.get("id") or "")
        if not case_num or case_num in seen:
            continue

        # Get coordinates
        try:
            # Cambridge may nest location in a point object
            loc_obj = row.get("location") or {}
            if isinstance(loc_obj, dict):
                lat = float(loc_obj.get("latitude") or loc_obj.get("lat") or 0)
                lng = float(loc_obj.get("longitude") or loc_obj.get("lon") or loc_obj.get("long") or 0)
            else:
                lat = float(row.get("lat") or row.get("latitude") or 0)
                lng = float(row.get("long") or row.get("longitude") or 0)
        except:
            continue
        if not lat or not lng:
            continue

        desc     = (row.get("event") or row.get("offense") or row.get("nature") or "Incident").strip()
        street   = (row.get("address") or row.get("street") or row.get("location_street") or "").strip().title()
        occurred = row.get("date_time") or row.get("occurred_on_date") or ""
        priority = classify(desc)
        if priority == "p3":
            continue

        hotspot, zone = check_hotspot(street)

        records.append({
            "incident_number": f"CPD-{case_num}",
            "title":           make_title(desc),
            "offense_group":   "",
            "offense_desc":    desc,
            "location":        street or "Cambridge",
            "lat":             lat,
            "lng":             lng,
            "priority":        priority,
            "district":        "CPD",
            "district_name":   "Cambridge PD",
            "shooting":        "shoot" in desc.lower() or "gun" in desc.lower(),
            "gang_hotspot":    hotspot,
            "gang_zone":       zone,
            "occurred_at":     occurred,
            "created_at":      datetime.now(timezone.utc).isoformat(),
        })
        seen.add(case_num)
        saved += 1
        if len(records) >= 50:
            sb_upsert(records)
            records = []

    if records:
        sb_upsert(records)
    return saved

# ── Main loop ─────────────────────────────────────────────────────────────────
def run():
    print("[Hood Brief Boston] Pipeline started — polling every 5 minutes...")
    seen      = set()
    first_run = True

    while True:
        try:
            days = LOOKBACK_DAYS if first_run else ONGOING_DAYS
            since = datetime.now(timezone.utc) - timedelta(days=days)
            first_run = False

            # Boston
            boston_rows = fetch_boston(since)
            boston_saved = process_boston(boston_rows, seen)
            if boston_saved:
                print(f"[Boston] Saved {boston_saved} incidents")
            else:
                print("[Boston] No new incidents")

            # Cambridge
            cambridge_rows = fetch_cambridge(since)
            cambridge_saved = process_cambridge(cambridge_rows, seen)
            if cambridge_saved:
                print(f"[Cambridge] Saved {cambridge_saved} incidents")
            else:
                print("[Cambridge] No new incidents")

        except Exception as e:
            print(f"[Poll error] {e}")

        time.sleep(POLL_INTERVAL)

# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — Pipeline Starting   ║")
    print("║  Boston + Cambridge — Multi-City         ║")
    print("╚══════════════════════════════════════════╝")

    errors = []
    if not SUPABASE_URL: errors.append("SUPABASE_URL not set")
    if not SUPABASE_KEY: errors.append("SUPABASE_KEY not set")
    if not WORKER_URL:   errors.append("WORKER_URL not set")
    if errors:
        for e in errors: print(f"  ❌ {e}")
        exit(1)

    run()
