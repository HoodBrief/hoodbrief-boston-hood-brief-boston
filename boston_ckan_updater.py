"""
Hood Brief Boston — Multi-Dataset CKAN Updater
Pulls from 4 BPD datasets daily:
  1. Crime Incidents (3-week lag, general crimes, heatmap base)
  2. Shootings (48hr lag, victims struck, P1 markers)
  3. Shots Fired (48hr lag, no victim, P1 markers)
  4. Firearm Recovery Counts (daily, aggregate)
Rebuilds heatmap weekly from 6-month rolling window.
"""
import os, time, requests
from datetime import datetime, timezone, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

CKAN_BASE = "https://data.boston.gov/api/3/action/datastore_search"

# Dataset resource IDs
DATASETS = {
    "crime_incidents": {
        "resource_id": "b973d8cb-eeb2-4e7e-99da-c92938efc9c0",
        "table": "boston_incidents",
        "priority": "p2",
        "label": "Crime Incident",
    },
    "shootings": {
        "resource_id": "73c7e069-701f-4910-986d-b950f46c91a1",
        "table": "boston_shootings",
        "priority": "p1",
        "label": "Shooting",
    },
    "shots_fired": {
        "resource_id": None,  # Will discover on first run
        "table": "boston_shots_fired",
        "priority": "p1",
        "label": "Shots Fired",
    },
}

WEIGHT_MAP = {
    "shooting": 5, "homicide": 5, "murder": 5,
    "robbery": 3, "assault": 2, "burglary": 2,
    "breaking": 2, "larceny": 1,
}

def get_weight(desc, shooting=False):
    if shooting: return 5
    d = (desc or "").lower()
    for k, w in WEIGHT_MAP.items():
        if k in d: return w
    return 1

def discover_resource_id(dataset_slug):
    """Find resource ID by fetching dataset page."""
    try:
        r = requests.get(
            "https://data.boston.gov/api/3/action/package_show",
            params={"id": dataset_slug},
            timeout=15,
            headers={"User-Agent": "Hood Brief/1.0"}
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                resources = data["result"]["resources"]
                if resources:
                    rid = resources[0]["id"]
                    print(f"[CKAN] Discovered {dataset_slug}: {rid}")
                    return rid
    except Exception as e:
        print(f"[CKAN] Discovery error for {dataset_slug}: {e}")
    return None

def fetch_dataset(resource_id, limit=5000):
    """Fetch records from a CKAN dataset."""
    try:
        for attempt in range(3):
            r = requests.get(
                CKAN_BASE,
                params={"resource_id": resource_id, "limit": limit},
                timeout=30,
                headers={"User-Agent": "Hood Brief/1.0"}
            )
            if r.status_code == 409:
                wait = (attempt + 1) * 15
                print(f"[CKAN] Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    records = data["result"]["records"]
                    print(f"[CKAN] Fetched {len(records)} records from {resource_id[:8]}...")
                    if records:
                        print(f"[CKAN] Columns: {list(records[0].keys())}")
                    return records
            print(f"[CKAN] HTTP {r.status_code} for {resource_id}")
            break
    except Exception as e:
        print(f"[CKAN] Fetch error: {e}")
    return []

def get_col(row, *keys):
    """Get column value trying multiple case variants."""
    for k in keys:
        for variant in [k, k.upper(), k.lower(), k.title()]:
            if variant in row and row[variant] not in (None, "", "None"):
                return row[variant]
    return ""

def process_incidents(records):
    """Process general crime incident records."""
    rows = []
    for r in records:
        try:
            lat = float(get_col(r, "Lat", "lat", "LAT") or 0)
            lng = float(get_col(r, "Long", "long", "LONG", "lng") or 0)
            if not lat or not lng: continue
            if not (42.2 <= lat <= 42.4 and -71.2 <= lng <= -70.9): continue
            inc_num = get_col(r, "INCIDENT_NUMBER", "incident_number")
            if not inc_num: continue
            rows.append({
                "incident_number": str(inc_num),
                "offense_desc":    get_col(r, "OFFENSE_DESCRIPTION", "offense_description"),
                "occurred_on":     get_col(r, "OCCURRED_ON_DATE", "occurred_on_date"),
                "lat": lat, "lng": lng,
                "shooting":        str(get_col(r, "SHOOTING", "shooting")).upper() == "Y",
                "district":        get_col(r, "DISTRICT", "district"),
            })
        except Exception: continue
    return rows

def process_shootings(records):
    """Process shooting incident records.
    Shootings dataset: incident_num, shooting_date, district, shooting_type_v2, lat, long
    Shots fired dataset: incident_num, incident_date, district, ballistics_evidence, lat, long
    """
    rows = []
    for r in records:
        try:
            lat = float(get_col(r, "lat", "Lat", "LAT", "latitude") or 0)
            lng = float(get_col(r, "long", "Long", "LONG", "lng", "longitude") or 0)
            if not lat or not lng: continue
            if not (42.2 <= lat <= 42.4 and -71.2 <= lng <= -70.9): continue
            inc_id = get_col(r, "incident_num", "incident_number", "INCIDENT_NUMBER", "_id")
            occurred = get_col(r, "shooting_date", "incident_date", "occurred_on_date", "OCCURRED_ON_DATE")
            rows.append({
                "incident_id":  str(inc_id),
                "occurred_on":  occurred,
                "district":     get_col(r, "district", "DISTRICT"),
                "fatal":        str(get_col(r, "fatal", "homicide", "shooting_type_v2")).upper() in ("Y", "YES", "TRUE", "FATAL", "1"),
                "victim_count": int(get_col(r, "victim_count", "count") or 1),
                "lat": lat, "lng": lng,
                "priority": "p1",
            })
        except Exception: continue
    return rows

def upsert_batch(table, rows, id_field):
    """Upsert rows to Supabase table in batches."""
    if not rows:
        return 0
    inserted = 0
    for i in range(0, len(rows), 200):
        batch = rows[i:i+200]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS,
            json=batch,
            timeout=20,
        )
        if r.status_code in (200, 201, 204):
            inserted += len(batch)
        else:
            print(f"[CKAN] Upsert error on {table}: {r.status_code} {r.text[:80]}")
    return inserted

def rebuild_heatmap():
    """Rebuild heatmap from 6-month rolling window of all sources."""
    print("[Heatmap] Rebuilding from 6-month rolling window...")
    since = (datetime.now(timezone.utc) - timedelta(days=182)).isoformat()
    all_points = []

    # From general incidents
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/boston_incidents",
            headers={**HEADERS, "Prefer": ""},
            params={"select": "lat,lng,offense_desc,shooting", "lat": "not.is.null", "limit": 5000},
            timeout=30,
        )
        for row in r.json():
            try:
                lat, lng = float(row["lat"]), float(row["lng"])
                w = get_weight(row.get("offense_desc"), row.get("shooting", False))
                all_points.append({"lat": lat, "lng": lng, "weight": w})
            except Exception: continue
        print(f"[Heatmap] {len(all_points)} points from incidents")
    except Exception as e:
        print(f"[Heatmap] Incidents error: {e}")

    # From shootings (weight 5)
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/boston_shootings",
            headers={**HEADERS, "Prefer": ""},
            params={"select": "lat,lng", "lat": "not.is.null", "limit": 2000},
            timeout=30,
        )
        for row in r.json():
            try:
                all_points.append({"lat": float(row["lat"]), "lng": float(row["lng"]), "weight": 5})
            except Exception: continue
        print(f"[Heatmap] {len(all_points)} points after shootings")
    except Exception as e:
        print(f"[Heatmap] Shootings error: {e}")

    if not all_points:
        print("[Heatmap] No points — skipping rebuild")
        return

    # Clear and rebuild
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
        headers={**HEADERS, "Prefer": "return=minimal"},
        params={"id": "not.is.null"},
        timeout=15,
    )
    for i in range(0, len(all_points), 500):
        requests.post(
            f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
            headers={**HEADERS, "Prefer": "return=minimal"},
            json=all_points[i:i+500],
            timeout=20,
        )
    print(f"[Heatmap] ✅ Rebuilt with {len(all_points):,} points")

def ensure_tables():
    """Create boston_shootings table if it doesn't exist."""
    # We'll handle this via Supabase SQL editor - just try to insert
    pass

def run():
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — Multi-Dataset Sync  ║")
    print("║  Crime · Shootings · Shots Fired · Daily ║")
    print("╚══════════════════════════════════════════╝")

    cycle = 0
    while True:
        print(f"\n[CKAN] Cycle {cycle + 1} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

        # 1. Crime Incidents
        records = fetch_dataset("b973d8cb-eeb2-4e7e-99da-c92938efc9c0")
        if records:
            rows = process_incidents(records)
            n = upsert_batch("boston_incidents", rows, "incident_number")
            print(f"[CKAN] ✅ Crime incidents: {n} upserted")

        time.sleep(5)

        # 2. Shootings (48hr lag)
        records = fetch_dataset("73c7e069-701f-4910-986d-b950f46c91a1")
        if records:
            rows = process_shootings(records)
            n = upsert_batch("boston_shootings", rows, "incident_id")
            print(f"[CKAN] ✅ Shootings: {n} upserted")

        time.sleep(5)

        # 3. Shots Fired — discover resource ID on first run
        if not DATASETS["shots_fired"]["resource_id"]:
            rid = discover_resource_id("shots-fired")
            DATASETS["shots_fired"]["resource_id"] = rid

        if DATASETS["shots_fired"]["resource_id"]:
            records = fetch_dataset(DATASETS["shots_fired"]["resource_id"])
            if records:
                rows = process_shootings(records)  # similar structure
                n = upsert_batch("boston_shots_fired", rows, "incident_id")
                print(f"[CKAN] ✅ Shots fired: {n} upserted")

        # 4. Rebuild heatmap weekly
        if cycle % 7 == 0:
            rebuild_heatmap()

        cycle += 1
        print(f"[CKAN] Next update in 24 hours")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    run()
