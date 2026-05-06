"""
Hood Brief Boston — Heatmap Generator
Pulls historical P1 incidents from boston_incidents table
and loads them as heatmap points.
"""
import os, requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def run():
    print("[Heatmap] Loading historical Boston incident data...")

    # Fetch P1 incidents with coordinates from boston_incidents
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/boston_incidents",
        headers=HEADERS,
        params={
            "select": "lat,lng,offense_group,offense_desc,shooting",
            "priority": "eq.p1",
            "lat": "not.is.null",
            "limit": "10000",
        },
        timeout=30,
    )
    r.raise_for_status()
    incidents = r.json()
    print(f"[Heatmap] Got {len(incidents)} P1 incidents")

    # Build heatmap points with categories
    points = []
    for inc in incidents:
        lat = inc.get("lat")
        lng = inc.get("lng")
        if not lat or not lng:
            continue

        desc = (inc.get("offense_desc") or "").lower()
        group = (inc.get("offense_group") or "").lower()
        shooting = inc.get("shooting", False)

        if shooting or "shoot" in desc or "gun" in desc or "ballistic" in group:
            category = "shooting"
            weight = 3
        elif "homicide" in desc or "murder" in desc:
            category = "homicide"
            weight = 5
        elif "robbery" in desc or "robbery" in group:
            category = "robbery"
            weight = 2
        elif "assault" in desc:
            category = "assault"
            weight = 1
        else:
            category = "violent"
            weight = 1

        points.append({
            "lat": float(lat),
            "lng": float(lng),
            "category": category,
            "weight": weight,
        })

    print(f"[Heatmap] Built {len(points)} heatmap points")

    # Clear existing and reload
    requests.delete(
        f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
        headers={**HEADERS, "Prefer": "return=minimal"},
        params={"id": "not.is.null"},
        timeout=15,
    )

    # Insert in batches of 500
    for i in range(0, len(points), 500):
        batch = points[i:i+500]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
            headers={**HEADERS, "Prefer": "return=minimal"},
            json=batch,
            timeout=20,
        )
        if r.status_code not in (200, 201, 204):
            print(f"  [Heatmap] Batch error: {r.status_code} {r.text[:100]}")
        else:
            print(f"  [Heatmap] Inserted batch {i//500 + 1} ({len(batch)} points)")

    print(f"[Heatmap] ✅ Done — {len(points)} points loaded")

if __name__ == "__main__":
    run()
