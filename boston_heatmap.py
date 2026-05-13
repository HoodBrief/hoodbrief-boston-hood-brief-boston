"""
Hood Brief Boston — Heatmap Generator
Pulls historical incidents from boston_incidents (BPD open data CKAN pipeline)
and loads them as weighted heatmap points.
Run once manually or on a weekly schedule.
"""
import os, requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

# Weight by offense severity
WEIGHTS = {
    "shooting":  5,
    "homicide":  5,
    "robbery":   3,
    "assault":   2,
    "burglary":  2,
    "larceny":   1,
    "mvt":       1,
    "other":     1,
}

def get_weight(offense_code, offense_desc, shooting):
    if shooting:
        return 5
    desc = (offense_desc or "").lower()
    code = (offense_code or "").lower()
    if "homicide" in desc or "murder" in desc:
        return 5
    if "shoot" in desc or "gunshot" in desc:
        return 5
    if "robbery" in desc:
        return 3
    if "assault" in desc:
        return 2
    if "burglary" in desc or "breaking" in desc:
        return 2
    return 1

def run():
    print("[Heatmap] Loading historical BPD incident data...")

    # Fetch all incidents with coordinates from boston_incidents
    all_points = []
    offset = 0
    batch = 1000

    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/boston_incidents",
            headers={**HEADERS, "Prefer": ""},
            params={
                "select": "lat,lng,offense_code,offense_desc,shooting",
                "lat":    "not.is.null",
                "lng":    "not.is.null",
                "limit":  batch,
                "offset": offset,
            },
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break

        for row in rows:
            lat = row.get("lat")
            lng = row.get("lng")
            if not lat or not lng:
                continue
            weight = get_weight(
                row.get("offense_code"),
                row.get("offense_desc"),
                row.get("shooting", False),
            )
            all_points.append({"lat": float(lat), "lng": float(lng), "weight": weight})

        print(f"  Loaded {offset + len(rows):,} incidents...")
        if len(rows) < batch:
            break
        offset += batch

    print(f"[Heatmap] Total points: {len(all_points):,}")

    if not all_points:
        print("[Heatmap] No data found — check boston_incidents table")
        return

    # Clear existing heatmap
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
        headers=HEADERS,
        params={"id": "not.is.null"},
        timeout=15,
    )
    print("[Heatmap] Cleared old points")

    # Insert in batches of 500
    for i in range(0, len(all_points), 500):
        batch_data = all_points[i:i+500]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
            headers=HEADERS,
            json=batch_data,
            timeout=20,
        )
        if r.status_code not in (200, 201, 204):
            print(f"  [Heatmap] Batch error: {r.status_code} {r.text[:80]}")
        else:
            print(f"  [Heatmap] Inserted {i + len(batch_data):,}/{len(all_points):,}")

    print(f"[Heatmap] ✅ Done — {len(all_points):,} points loaded")

if __name__ == "__main__":
    run()
