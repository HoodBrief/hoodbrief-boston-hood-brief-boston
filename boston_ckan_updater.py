"""
Hood Brief Boston — BPD CKAN Incident Updater
Polls the Analyze Boston CKAN API daily for new BPD incidents
and keeps boston_incidents table current.
Also rebuilds the heatmap with a 6-month rolling window.
"""
import os, requests, time
from datetime import datetime, timezone, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

RESOURCE_ID = "b973d8cb-eeb2-4e7e-99da-c92938efc9c0"  # 2023-present dataset
RESOURCE_ID  = "b973d8cb-eeb2-4e7e-99da-c92938efc9c0"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}

WEIGHT_MAP = {
    "shooting": 5, "homicide": 5, "murder": 5,
    "robbery":  3, "assault":  2, "burglary": 2,
    "breaking": 2, "larceny":  1,
}

def get_weight(desc, shooting):
    if shooting: return 5
    d = (desc or "").lower()
    for k, w in WEIGHT_MAP.items():
        if k in d: return w
    return 1

def fetch_recent_incidents(days=7):
    """Fetch incidents from the last N days via CKAN SQL endpoint."""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        url = "https://data.boston.gov/api/3/action/datastore_search_sql"
        sql = (
            f'SELECT incident_number, offense_code, offense_description, '
            f'occurred_on_date, lat, long, shooting, district '
            f'FROM "{RESOURCE_ID}" '
            f"WHERE occurred_on_date >= '{since}' "
            f"AND lat IS NOT NULL AND lat != '' "
            f"ORDER BY occurred_on_date DESC LIMIT 5000"
        )
        # Retry up to 3 times with backoff (409 = rate limit)
        import time as _time
        for attempt in range(3):
            r = requests.get(url, params={"sql": sql}, timeout=30)
            if r.status_code == 409:
                print(f"[CKAN] Rate limited (409) — waiting {(attempt+1)*10}s")
                _time.sleep((attempt+1) * 10)
                continue
            r.raise_for_status()
            break
        data = r.json()
        if data.get("success"):
            records = data["result"]["records"]
            print(f"[CKAN] Got {len(records)} records")
            return records
        print(f"[CKAN] API error: {data.get('error', {})}")
        return []
    except Exception as e:
        print(f"[CKAN] Fetch error: {e}")
        return []

def upsert_incidents(records):
    """Upsert incidents into boston_incidents table."""
    rows = []
    for r in records:
        try:
            lat = float(r.get("lat") or 0)
            lng = float(r.get("long") or 0)
            if not lat or not lng: continue
            # Basic Boston bounds check
            if not (42.2 <= lat <= 42.4 and -71.2 <= lng <= -70.9): continue
            rows.append({
                "incident_number": r.get("incident_number", ""),
                "offense_code":    r.get("offense_code", ""),
                "offense_desc":    r.get("offense_description", ""),
                "occurred_on":     r.get("occurred_on_date", ""),
                "lat":             lat,
                "lng":             lng,
                "shooting":        str(r.get("shooting", "")).upper() == "Y",
                "district":        r.get("district", ""),
            })
        except Exception:
            continue

    if not rows:
        return 0

    # Upsert in batches
    inserted = 0
    for i in range(0, len(rows), 200):
        batch = rows[i:i+200]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/boston_incidents",
            headers=HEADERS,
            json=batch,
            timeout=20,
        )
        if resp.status_code in (200, 201, 204):
            inserted += len(batch)
        else:
            print(f"[CKAN] Upsert error: {resp.status_code} {resp.text[:80]}")

    return inserted

def rebuild_heatmap_6months():
    """Rebuild heatmap from last 6 months of boston_incidents only."""
    print("[Heatmap] Rebuilding from 6-month rolling window...")
    since = (datetime.now(timezone.utc) - timedelta(days=182)).isoformat()

    all_points = []
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/boston_incidents",
            headers={**HEADERS, "Prefer": ""},
            params={
                "select":     "lat,lng,offense_desc,shooting",
                "lat":        "not.is.null",
                "occurred_on": f"gte.{since}",
                "limit":      1000,
                "offset":     offset,
            },
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows: break

        for row in rows:
            try:
                lat = float(row["lat"]); lng = float(row["lng"])
                if not lat or not lng: continue
                w = get_weight(row.get("offense_desc"), row.get("shooting", False))
                all_points.append({"lat": lat, "lng": lng, "weight": w})
            except Exception:
                continue

        if len(rows) < 1000: break
        offset += 1000

    print(f"[Heatmap] {len(all_points):,} points from last 6 months")
    if not all_points:
        print("[Heatmap] No points — skipping rebuild")
        return

    # Clear old heatmap
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
        headers={**HEADERS, "Prefer": "return=minimal"},
        params={"id": "not.is.null"},
        timeout=15,
    )

    # Insert new points
    for i in range(0, len(all_points), 500):
        batch = all_points[i:i+500]
        requests.post(
            f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
            headers={**HEADERS, "Prefer": "return=minimal"},
            json=batch,
            timeout=20,
        )
    print(f"[Heatmap] ✅ Rebuilt with {len(all_points):,} points")

def run():
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — CKAN Updater        ║")
    print("║  Daily BPD incident sync + heatmap       ║")
    print("╚══════════════════════════════════════════╝")

    cycle = 0
    while True:
        print(f"\n[CKAN] Cycle {cycle + 1} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

        # Fetch last 7 days of incidents
        records = fetch_recent_incidents(days=7)
        print(f"[CKAN] Fetched {len(records)} recent incidents")

        if records:
            inserted = upsert_incidents(records)
            print(f"[CKAN] ✅ Upserted {inserted} incidents")

        # Rebuild heatmap weekly (every 7 cycles = 7 days)
        if cycle % 7 == 0:
            rebuild_heatmap_6months()

        cycle += 1
        print(f"[CKAN] Next update in 24 hours")
        time.sleep(24 * 3600)

if __name__ == "__main__":
    run()
