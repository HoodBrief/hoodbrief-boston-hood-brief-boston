"""
Hood Brief Boston — BPD Incident Pipeline
==========================================
Polls the Boston Open Data CKAN API every 5 minutes for new BPD incidents.
No audio transcription needed — data comes in structured from BPD's RMS.

Data source: Analyze Boston Crime Incident Reports
API: https://data.boston.gov/api/3/action/datastore_search_sql
Resource: 12cb3883-56f5-47de-afa5-3b1cf61b257b
Updated: Real-time as officers file reports

Cost: $0/month (no AI APIs needed)
"""

import os
import re
import time
import json
import requests
import threading
from datetime import datetime, timezone, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
POLL_INTERVAL = 300   # seconds — poll every 5 minutes
LOOKBACK_MINS = 15    # fetch incidents from last N minutes each poll

# Cloudflare Worker proxy — routes around Boston API allowlist restriction
# Set this to your deployed worker URL after deploying boston_worker.js
WORKER_URL    = os.environ.get("WORKER_URL", "")
RESOURCE_ID   = "12cb3883-56f5-47de-afa5-3b1cf61b257b"

# ── BPD District Info ────────────────────────────────────────────────────────
DISTRICTS = {
    "A1":  {"name": "Downtown/Charlestown/East Boston",  "color": "#6366f1"},
    "A7":  {"name": "East Boston",                       "color": "#8b5cf6"},
    "A15": {"name": "Charlestown",                       "color": "#a78bfa"},
    "B2":  {"name": "Roxbury",                           "color": "#ec4899"},
    "B3":  {"name": "Mattapan",                          "color": "#f43f5e"},
    "C6":  {"name": "South Boston",                      "color": "#f97316"},
    "C11": {"name": "Dorchester",                        "color": "#fb923c"},
    "D4":  {"name": "South End/Back Bay",                "color": "#14b8a6"},
    "D14": {"name": "Brighton/Allston",                  "color": "#06b6d4"},
    "E5":  {"name": "West Roxbury/Jamaica Plain",        "color": "#22c55e"},
    "E13": {"name": "Hyde Park/Jamaica Plain",           "color": "#84cc16"},
    "E18": {"name": "Hyde Park/Roslindale",              "color": "#eab308"},
}

# ── Priority Classification ───────────────────────────────────────────────────
# Based on UCR Part and offense code groups
P1_OFFENSE_GROUPS = {
    "Homicide", "Robbery", "Aggravated Assault", "Firearm Violations",
    "Ballistics", "Home Invasion", "Kidnapping", "Manslaughter",
    "Offenses Against Child / Family", "Sex Offender Registration",
}
P1_KEYWORDS = [
    "homicide", "murder", "manslaughter", "shooting", "shot",
    "robbery", "armed", "weapon", "firearm", "gun", "knife",
    "assault with", "aggravated", "kidnap", "hostage", "rape",
    "sexual assault", "sex offense", "home invasion", "carjack",
]
P2_OFFENSE_GROUPS = {
    "Burglary", "Auto Theft", "Larceny", "Vandalism",
    "Drug Violation", "Harassment", "Restraining Order Violations",
    "Threats", "Simple Assault", "Investigate Person",
    "Missing Person", "Property Lost", "Property Found",
    "Trespassing", "Disorderly Conduct",
}
MEDICAL_KEYWORDS = [
    "medical", "ambulance", "overdose", "unconscious", "unresponsive",
    "suicide", "self harm", "mental health", "psychiatric",
]

# ── Boston Neighborhood Gang/High-Crime Zones ─────────────────────────────────
BOSTON_HOTSPOTS = [
    {"name": "Roxbury",      "streets": ["blue hill ave", "dudley", "washington st", "warren st", "humboldt ave", "dale st"]},
    {"name": "Mattapan",     "streets": ["morton st", "blue hill ave", "mattapan sq", "river st", "cummins hwy"]},
    {"name": "Dorchester",   "streets": ["bowdoin st", "columbia rd", "geneva ave", "talbot ave", "washington st", "harvard st"]},
    {"name": "Hyde Park",    "streets": ["hyde park ave", "fairmount ave", "readville", "cleary sq"]},
    {"name": "East Boston",  "streets": ["meridian st", "chelsea st", "maverick sq", "paris st"]},
    {"name": "South Boston", "streets": ["old colony", "silver st", "east broadway", "west broadway"]},
    {"name": "Jamaica Plain",   "streets": ["egleston sq", "centre st", "jackson sq", "stony brook"]},
    {"name": "Mission Hill",  "streets": ["huntington ave", "tremont st", "brigham circle"]},
]

def check_hotspot(street):
    if not street:
        return False, None
    sl = street.lower()
    for zone in BOSTON_HOTSPOTS:
        if any(s in sl for s in zone["streets"]):
            return True, zone["name"]
    return False, None

# ── Priority Classifier ───────────────────────────────────────────────────────
def classify(row):
    group   = (row.get("OFFENSE_CODE_GROUP") or "").strip()
    desc    = (row.get("OFFENSE_DESCRIPTION") or "").lower()
    ucr     = (row.get("UCR_PART") or "").strip()
    shooting= (row.get("SHOOTING") or "").strip().upper()

    if shooting == "Y":
        return "p1"
    if group in P1_OFFENSE_GROUPS or ucr == "Part One":
        return "p1"
    if any(k in desc for k in P1_KEYWORDS):
        return "p1"
    if any(k in desc for k in MEDICAL_KEYWORDS):
        return "medical"
    if group in P2_OFFENSE_GROUPS or ucr == "Part Two":
        return "p2"
    return "p3"

def make_title(row):
    group = (row.get("OFFENSE_CODE_GROUP") or "").strip()
    desc  = (row.get("OFFENSE_DESCRIPTION") or "").strip().title()
    shooting = (row.get("SHOOTING") or "").strip().upper()
    if shooting == "Y":
        return "Shooting"
    if group:
        return group
    return desc or "Incident"

# ── Supabase Helpers ──────────────────────────────────────────────────────────
def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def sb_upsert(table, rows):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**sb_headers(), "Prefer": "resolution=ignore-duplicates,return=minimal"},
        json=rows,
        timeout=15,
    )
    r.raise_for_status()

# ── Boston API Poller ─────────────────────────────────────────────────────────
def fetch_recent_incidents(since_dt):
    """Fetch BPD incidents via Cloudflare Worker proxy."""
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    if not WORKER_URL:
        print("  [API] WORKER_URL not set — skipping")
        return []
    r = requests.get(
        WORKER_URL,
        params={"since": since_str, "limit": 100},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        print(f"  [API] Error: {data.get('error')}")
        return []
    return data["result"]["records"]

def run_boston_pipeline():
    print("[Boston] Pipeline started — polling every 5 minutes...")
    seen = set()   # track incident numbers to avoid duplicates within session

    while True:
        try:
            since = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINS)
            incidents = fetch_recent_incidents(since)
            new_count = 0

            for row in incidents:
                inc_num = row.get("INCIDENT_NUMBER", "")
                if not inc_num or inc_num in seen:
                    continue

                priority = classify(row)
                if priority == "p3":
                    continue   # skip low-priority

                lat  = float(row.get("Lat",  0) or 0)
                lng  = float(row.get("Long", 0) or 0)
                if not lat or not lng:
                    continue

                street   = (row.get("STREET") or "").strip().title()
                district = (row.get("DISTRICT") or "").strip().upper()
                dist_info= DISTRICTS.get(district, {"name": district, "color": "#6b7d96"})
                hotspot, zone = check_hotspot(street)
                title    = make_title(row)
                occurred = row.get("OCCURRED_ON_DATE", "")

                record = {
                    "incident_number": inc_num,
                    "title":           title,
                    "offense_group":   row.get("OFFENSE_CODE_GROUP", ""),
                    "offense_desc":    row.get("OFFENSE_DESCRIPTION", ""),
                    "location":        street,
                    "lat":             lat,
                    "lng":             lng,
                    "priority":        priority,
                    "district":        district,
                    "district_name":   dist_info["name"],
                    "shooting":        row.get("SHOOTING", "N") == "Y",
                    "gang_hotspot":    hotspot,
                    "gang_zone":       zone,
                    "occurred_at":     occurred,
                    "created_at":      datetime.now(timezone.utc).isoformat(),
                }

                sb_upsert("boston_incidents", [record])
                seen.add(inc_num)
                new_count += 1
                print(f"  ✅ [{priority.upper()}] {title} @ {street} [{dist_info['name']}]")

            if new_count > 0:
                print(f"[Boston] Saved {new_count} new incidents")
            else:
                print(f"[Boston] No new incidents")

        except Exception as e:
            print(f"[Boston] Poll error: {e}")

        time.sleep(POLL_INTERVAL)

# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — Pipeline Starting   ║")
    print("║  Boston, MA — BPD Only                   ║")
    print("╚══════════════════════════════════════════╝")

    errors = []
    if not SUPABASE_URL: errors.append("SUPABASE_URL not set")
    if not SUPABASE_KEY: errors.append("SUPABASE_KEY not set")
    if not WORKER_URL:   errors.append("WORKER_URL not set — deploy Cloudflare Worker first")
    if errors:
        for e in errors:
            print(f"  ❌ {e}")
        exit(1)

    run_boston_pipeline()
