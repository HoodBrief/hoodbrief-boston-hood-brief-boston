"""
Hood Brief Boston — Multi-Dataset CKAN Updater v3
Uses b973d8cb resource which has lat/lng, street, UCR_PART, OFFENSE_CODE_GROUP
for proper priority classification and address display.
"""
import os, time, random, threading
import requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

# Headers that silently ignore duplicates
HEADERS_IGNORE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=ignore-duplicates,return=minimal",
}

CKAN_BASE = "https://data.boston.gov/api/3/action/datastore_search"

RESOURCE_IDS = {
    "crime_incidents": "b973d8cb-eeb2-4e7e-99da-c92938efc9c0",
    "shootings":       "73c7e069-701f-4910-986d-b950f46c91a1",
    "shots_fired":     "c1e4e6ac-8a84-4b48-8a23-7b2645a32ede",
}

DISTRICT_CENTROIDS = {
    "A1":  (42.3614, -71.0576), "A15": (42.3614, -71.0576),  # A15 = A1
    "A7":  (42.3697, -71.0335),
    "B2":  (42.3289, -71.0839), "B3":  (42.2932, -71.0801),
    "C6":  (42.3376, -71.0527), "C11": (42.3040, -71.0633),
    "D4":  (42.3421, -71.0724), "D14": (42.3521, -71.1546),
    "E5":  (42.2890, -71.1605), "E13": (42.3131, -71.1116),
    "E18": (42.2561, -71.1275),
    # Common aliases
    "A":   (42.3614, -71.0576), "B":   (42.3289, -71.0839),
    "C":   (42.3376, -71.0527), "D":   (42.3421, -71.0724),
    "E":   (42.2890, -71.1605),
}

# Priority classification from OFFENSE_CODE_GROUP
P1_GROUPS = {
    'homicide','robbery','aggravated assault','rape','murder',
    'shooting','carjacking','human trafficking','manslaughter',
    'kidnapping','harassment','criminal harassment'
}
P2_GROUPS = {
    'burglary','larceny','motor vehicle theft','arson','fraud',
    'vandalism','weapons','drug violation','simple assault',
    'other assault','auto theft','mvt','embezzlement','extortion',
    'confidence games','counterfeiting','forgery','prostitution',
    'property damage','restraining order','trespassing','disorderly conduct'
}
MEDICAL_GROUPS = {
    'medical assistance','sudden death','investigate person',
    'missing person','biological threat','fire related reports',
    'service','license plate related incidents','ballistics'
}

def get_priority(group, offense_desc, shooting):
    if str(shooting).upper() == "Y": return "p1"
    g = (group or "").lower().strip()
    d = (offense_desc or "").lower()
    if any(p in g for p in P1_GROUPS): return "p1"
    if any(p in d for p in ["homicide","shooting","murder","rape","robbery","carjack","kidnap"]): return "p1"
    if any(p in g for p in MEDICAL_GROUPS): return "medical"
    if any(p in d for p in ["medical","sudden death","investigate person"]): return "medical"
    if any(p in g for p in P2_GROUPS): return "p2"
    return "p3"

def get_col(row, *keys):
    for k in keys:
        if k in row and row[k] not in (None, "", "None"):
            return row[k]
    return ""

def fetch_dataset(resource_id, limit=5000, offset=0, sort=None, filters=None):
    try:
        params = {"resource_id": resource_id, "limit": limit, "offset": offset}
        if sort:
            params["sort"] = sort
        if filters:
            import json as _json
            params["filters"] = _json.dumps(filters)
        for attempt in range(3):
            r = requests.get(CKAN_BASE,
                params=params,
                timeout=30, headers={"User-Agent": "Hood Brief/1.0"})
            if r.status_code == 409:
                time.sleep((attempt+1)*15); continue
            if r.status_code == 200:
                data = r.json()
                if data.get("success"):
                    recs = data["result"]["records"]
                    print(f"[CKAN] Fetched {len(recs)} from {resource_id[:8]} (offset={offset})")
                    return recs
            print(f"[CKAN] HTTP {r.status_code}"); break
    except Exception as e:
        print(f"[CKAN] Error: {e}")
    return []

def geocode_address(street, district):
    """Free geocoding via Nominatim (OpenStreetMap). Rate limit: 1 req/sec."""
    if not street:
        return None, None
    try:
        addr = f"{street}, Boston, MA"
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": addr, "format": "json", "limit": 1, "countrycodes": "us"},
            headers={"User-Agent": "HoodBrief/1.0 (contact@hoodbrief.com)"},
            timeout=5,
        )
        results = r.json()
        if results:
            lat = float(results[0]["lat"])
            lng = float(results[0]["lon"])
            if 42.2 <= lat <= 42.4 and -71.2 <= lng <= -70.9:
                return lat, lng
    except Exception:
        pass
    return None, None

def process_incidents(records):
    rows = []
    for r in records:
        try:
            lat = float(get_col(r, "Lat", "lat") or 0)
            lng = float(get_col(r, "Long", "long") or 0)
            if not lat or not lng: continue
            if not (42.2 <= lat <= 42.4 and -71.2 <= lng <= -70.9): continue
            inc = get_col(r, "INCIDENT_NUMBER", "incident_number")
            if not inc: continue
            group   = get_col(r, "OFFENSE_CODE_GROUP", "offense_code_group")
            desc    = get_col(r, "OFFENSE_DESCRIPTION", "offense_description")
            shooting = get_col(r, "SHOOTING", "shooting")
            street  = get_col(r, "STREET", "street")
            priority = get_priority(group, desc, shooting)
            rows.append({
                "incident_number": str(inc),
                "offense_desc":    desc,
                "occurred_on":     get_col(r, "OCCURRED_ON_DATE", "occurred_on_date"),
                "lat": lat, "lng": lng,
                "shooting":        str(shooting).upper() == "Y",
                "district":        get_col(r, "DISTRICT", "district"),
                "location":        street or "",
                "priority":        priority,
                "title":           group or desc,
            })
        except Exception:
            continue
    return rows

def process_shootings(records):
    rows = []
    missing_districts = set()
    errors = []
    for r in records:
        try:
            d = (get_col(r, "district", "District") or "").strip().upper()
            c = DISTRICT_CENTROIDS.get(d)
            if not c:
                missing_districts.add(repr(d))
                continue
            stype = get_col(r, "shooting_type_v2", "Shooting_Type_V2")
            inc_id = str(get_col(r, "incident_num", "Incident_Num", "_id"))
            occurred = get_col(r, "shooting_date", "Shooting_Date")
            rows.append({
                "incident_id":  inc_id,
                "occurred_on":  occurred,
                "district":     d,
                "fatal":        str(stype).strip().lower() == "fatal",
                "victim_count": (2 if str(get_col(r, "multi_victim", "Multi_Victim")).lower() in ("t","true","1","yes") else 1),
                # No lat/lng — shootings show in feed only, not mapped (no street address)
                "lat": None,
                "lng": None,
                "priority": "p1",
            })
        except Exception as e:
            errors.append(str(e))
            continue
    print(f"[CKAN] Shootings: {len(rows)} built, {len(missing_districts)} missing districts, {len(errors)} errors")
    if missing_districts:
        print(f"[CKAN] Missing: {list(missing_districts)[:10]}")
    if errors:
        print(f"[CKAN] Errors: {errors[:3]}")
    # Print first record for debug
    if records:
        print(f"[CKAN] Sample record keys: {list(records[0].keys())}")
        print(f"[CKAN] Sample district value: {repr(records[0].get('district',''))}")
    return rows

def process_shots_fired(records):
    rows = []
    for r in records:
        try:
            d = (get_col(r, "district", "District") or "").strip().upper()
            c = DISTRICT_CENTROIDS.get(d)
            if not c: continue
            rows.append({
                "incident_id": str(get_col(r, "incident_num", "Incident_Num", "_id")),
                "occurred_on": get_col(r, "incident_date", "Incident_Date"),
                "district":    d,
                "lat": c[0] + random.uniform(-0.004, 0.004),
                "lng": c[1] + random.uniform(-0.004, 0.004),
                "priority": "p1",
            })
        except Exception:
            continue
    return rows

def clear_table(table):
    """Delete all rows from table before fresh insert."""
    # Use the appropriate unique key for each table
    filters = {
        "boston_incidents":   {"incident_number": "neq.XXXXXXX"},
        "boston_shootings":   {"incident_id": "neq.XXXXXXX"},
        "boston_shots_fired": {"incident_id": "neq.XXXXXXX"},
        "boston_heatmap_points": {"id": "gt.0"},
    }
    # Fallback: delete where lat is not null (all geo records)
    params = filters.get(table, {"lat": "not.is.null"})
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**HEADERS, "Prefer": "return=minimal"},
            params=params,
            timeout=20,
        )
        print(f"[CKAN] Cleared {table}: HTTP {r.status_code}")
    except Exception as e:
        print(f"[CKAN] Clear error {table}: {e}")

def upsert_batch(table, rows, headers=None):
    if not rows: return 0
    if headers is None: headers = HEADERS
    inserted = 0
    for i in range(0, len(rows), 200):
        batch = rows[i:i+200]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}",
            headers=headers, json=batch, timeout=20)
        if r.status_code in (200, 201, 204):
            inserted += len(batch)
        else:
            print(f"[CKAN] Error {table}: {r.status_code} {r.text[:60]}")
    return inserted

def rebuild_heatmap():
    print("[Heatmap] Rebuilding...")
    pts = []
    WEIGHT = {"p1": 5, "p2": 2, "medical": 3, "p3": 1}
    from datetime import timedelta
    six_months_ago = (datetime.now(timezone.utc) - timedelta(days=182)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/boston_incidents",
            headers={**HEADERS, "Prefer": ""},
            params={
                "select": "lat,lng,priority",
                "lat": "not.is.null",
                "occurred_on": f"gte.{six_months_ago}",
                "limit": 5000,
            },
            timeout=30)
        for row in r.json():
            try:
                pts.append({"lat": float(row["lat"]), "lng": float(row["lng"]),
                           "weight": WEIGHT.get(row.get("priority","p3"), 1)})
            except: pass
        print(f"[Heatmap] {len(pts)} from incidents")
    except Exception as e:
        print(f"[Heatmap] incidents: {e}")
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/boston_shootings",
            headers={**HEADERS, "Prefer": ""},
            params={"select": "lat,lng", "lat": "not.is.null", "limit": 5000}, timeout=30)
        for row in r.json():
            try: pts.append({"lat": float(row["lat"]), "lng": float(row["lng"]), "weight": 5})
            except: pass
        print(f"[Heatmap] {len(pts)} total")
    except Exception as e:
        print(f"[Heatmap] shootings: {e}")
    if not pts: print("[Heatmap] No points"); return
    requests.delete(f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
        headers={**HEADERS, "Prefer": "return=minimal"},
        params={"id": "not.is.null"}, timeout=15)
    for i in range(0, len(pts), 500):
        requests.post(f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
            headers={**HEADERS, "Prefer": "return=minimal"},
            json=pts[i:i+500], timeout=20)
    print(f"[Heatmap] {len(pts):,} points rebuilt")

import http.server, threading, io

ARCGIS_PORT = int(os.environ.get("RELAY_PORT", "8080"))
RELAY_SECRET = os.environ.get("RELAY_SECRET", "hoodbrief")

P1_CRIMES = {
    'aggravated assault','robbery','homicide','rape','murder',
    'shooting','carjacking','human trafficking','kidnapping'
}
P2_CRIMES = {
    'burglary','larceny','vandalism','arson','fraud','weapons',
    'drug violation','simple assault','motor vehicle theft',
    'trespassing','disorderly conduct','warrant'
}
MEDICAL_CRIMES = {
    'medical assistance','sudden death','investigate person',
    'missing person','fire'
}

def get_priority_arcgis(crime, crime_category, crime_part):
    c = (crime or "").lower()
    cat = (crime_category or "").lower()
    part = (crime_part or "").lower()
    if cat == "violent" or any(p in c for p in P1_CRIMES): return "p1"
    if any(p in c for p in MEDICAL_CRIMES): return "medical"
    if cat == "property" or "part one" in part or any(p in c for p in P2_CRIMES): return "p2"
    return "p3"

def process_arcgis_csv(csv_text):
    """Process ArcGIS CSV and upsert to boston_incidents."""
    import csv as _csv
    rows = []
    try:
        reader = _csv.DictReader(io.StringIO(csv_text))
        for r in reader:
            try:
                street = r.get("Block Address","").strip()
                district = r.get("BPD District","").strip().upper()
                crime = r.get("Crime","").strip()
                crime_cat = r.get("Crime Category","").strip()
                crime_part = r.get("Crime Part","").strip()
                inc_num = r.get("Incident Number","").strip()
                from_date = r.get("From Date","").strip()
                neighborhood = r.get("Neighborhood","").strip()
                if not inc_num: continue
                priority = get_priority_arcgis(crime, crime_cat, crime_part)
                rows.append({
                    "incident_number": inc_num,
                    "offense_desc":    crime,
                    "occurred_on":     from_date,
                    "lat":             None,
                    "lng":             None,
                    "shooting":        "shooting" in crime.lower(),
                    "district":        district,
                    "location":        street or neighborhood or f"District {district}",
                    "priority":        priority,
                    "title":           crime,
                })
            except Exception as e:
                continue
        print(f"[ArcGIS] Parsed {len(rows):,} rows from CSV")
    except Exception as e:
        print(f"[ArcGIS] CSV parse error: {e}")
        return 0

    if not rows:
        print("[ArcGIS] No rows parsed — check CSV format")
        return 0

    # Clear existing data
    del_r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/boston_incidents",
        headers={**HEADERS, "Prefer": "return=minimal"},
        params={"incident_number": "neq.XXXXXXX"},
        timeout=30,
    )
    print(f"[ArcGIS] Cleared boston_incidents: HTTP {del_r.status_code}")

    # Insert in batches
    inserted = 0
    upsert_headers = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"}
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/boston_incidents",
                headers=upsert_headers,
                json=batch, timeout=30,
            )
            if r.status_code in (200,201,204):
                inserted += len(batch)
            else:
                print(f"[ArcGIS] Batch error: {r.status_code} {r.text[:80]}")
        except Exception as e:
            print(f"[ArcGIS] Batch exception: {e}")
    print(f"[ArcGIS] ✅ Upserted {inserted:,}/{len(rows):,} incidents")
    return inserted

class ArcGISHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def do_POST(self):
        if self.path != "/arcgis-data": 
            self.send_response(404); self.end_headers(); return
        secret = self.headers.get("X-Relay-Secret","")
        if secret != RELAY_SECRET:
            self.send_response(403); self.end_headers(); return
        length = int(self.headers.get("Content-Length",0))
        csv_text = self.rfile.read(length).decode("utf-8")
        self.send_response(200); self.end_headers()
        print(f"[ArcGIS] Received {len(csv_text):,} bytes CSV")
        threading.Thread(target=process_arcgis_csv, args=(csv_text,), daemon=True).start()

def start_arcgis_receiver():
    server = http.server.HTTPServer(("0.0.0.0", ARCGIS_PORT), ArcGISHandler)
    print(f"[ArcGIS] Receiver listening on port {ARCGIS_PORT}")
    server.serve_forever()

def run():
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — Multi-Dataset Sync  ║")
    print("║  Crime · Shootings · Shots Fired · Daily ║")
    print("╚══════════════════════════════════════════╝")
    # Start ArcGIS CSV receiver
    threading.Thread(target=start_arcgis_receiver, daemon=True).start()
    cycle = 0
    while True:
        print(f"\n[CKAN] Cycle {cycle+1} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

        # Crime incidents — fetch all available records (dataset has ~5000 rows)
        recs = fetch_dataset(RESOURCE_IDS["crime_incidents"], limit=5000)
        if recs:
            rows = process_incidents(recs)
            # Clear old data and replace with fresh pull
            clear_table("boston_incidents")
            n = upsert_batch("boston_incidents", rows)
            print(f"[CKAN] Crime incidents: {n} replaced")

        time.sleep(5)

        # Shootings — replace all
        recs = fetch_dataset(RESOURCE_IDS["shootings"])
        if recs:
            rows = process_shootings(recs)
            print(f"[CKAN] Shootings processed: {len(rows)} rows from {len(recs)} records")
            clear_table("boston_shootings")
            n = upsert_batch("boston_shootings", rows)
            print(f"[CKAN] Shootings: {n} replaced")

        time.sleep(5)

        # Shots Fired — replace all
        recs = fetch_dataset(RESOURCE_IDS["shots_fired"])
        if recs:
            clear_table("boston_shots_fired")
            time.sleep(2)  # Wait for clear to propagate
            n = upsert_batch("boston_shots_fired", process_shots_fired(recs), headers=HEADERS_IGNORE)
            print(f"[CKAN] Shots fired: {n} replaced")

        if cycle % 7 == 0:
            rebuild_heatmap()

        cycle += 1
        print(f"[CKAN] Next update in 24 hours")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    run()
