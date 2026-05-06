"""
Hood Brief Boston — Diplomatic Facilities
Geocodes and saves all Boston metro consulates/embassies to Supabase.
Runs once at startup. Checks proximity (50m) to incidents in the pipeline.
"""
import os, json, math, requests

SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_KEY", "")
GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_KEY", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

CONSULATES = [
    {"country": "Brazil",             "type": "Consulate General", "address": "175 Purchase Street, Boston, MA 02110"},
    {"country": "Canada",             "type": "Consulate General", "address": "3 Copley Place Suite 400, Boston, MA 02116"},
    {"country": "Colombia",           "type": "Consulate General", "address": "31 St James Avenue Suite 960, Boston, MA 02116"},
    {"country": "Dominican Republic", "type": "Consulate General", "address": "20 Park Plaza Suite 601, Boston, MA 02116"},
    {"country": "El Salvador",        "type": "Consulate General", "address": "46 Bennington Street, East Boston, MA 02128"},
    {"country": "France",             "type": "Consulate General", "address": "31 St James Avenue Suite 750, Boston, MA 02116"},
    {"country": "Germany",            "type": "Consulate General", "address": "3 Copley Place Suite 500, Boston, MA 02116"},
    {"country": "Greece",             "type": "Consulate General", "address": "86 Beacon Street, Boston, MA 02108"},
    {"country": "Haiti",              "type": "Consulate General", "address": "545 Boylston Street Suite 201, Boston, MA 02116"},
    {"country": "Ireland",            "type": "Consulate General", "address": "535 Boylston Street 5th Floor, Boston, MA 02116"},
    {"country": "Israel",             "type": "Consulate General", "address": "20 Park Plaza Suite 1020, Boston, MA 02116"},
    {"country": "Italy",              "type": "Consulate General", "address": "600 Atlantic Avenue 17th Floor, Boston, MA 02210"},
    {"country": "Japan",              "type": "Consulate General", "address": "100 High Street 6th Floor, Boston, MA 02110"},
    {"country": "Mexico",             "type": "Consulate General", "address": "55 Franklin Street 1st Floor, Boston, MA 02110"},
    {"country": "Peru",               "type": "Consulate General", "address": "20 Park Plaza Suite 511, Boston, MA 02116"},
    {"country": "Portugal",           "type": "Consulate General", "address": "31 St James Avenue Suite 350, Boston, MA 02116"},
    {"country": "Spain",              "type": "Consulate General", "address": "31 St James Avenue Suite 905, Boston, MA 02116"},
    {"country": "Turkey",             "type": "Consulate General", "address": "31 St James Avenue Suite 840, Boston, MA 02116"},
    {"country": "United Arab Emirates","type": "Consulate General","address": "One International Place Suite 2901, Boston, MA 02110"},
    {"country": "Denmark",            "type": "Consulate General", "address": "245 Main Street, Cambridge, MA 02142"},
    {"country": "Switzerland",        "type": "Consulate General", "address": "420 Broadway, Cambridge, MA 02138"},
    {"country": "United Kingdom",     "type": "Consulate General", "address": "One Broadway, Cambridge, MA 02142"},
    {"country": "Honduras",           "type": "Consulate General", "address": "90 Everett Avenue 3rd Floor, Chelsea, MA 02150"},
    {"country": "South Korea",        "type": "Consulate General", "address": "One Gateway Center 2nd Floor, Newton, MA 02458"},
    {"country": "Cape Verde",         "type": "Consulate General", "address": "300 Congress Street Suite 204, Quincy, MA 02169"},
]

def geocode(address):
    if not GOOGLE_MAPS_KEY:
        return None, None
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": address, "key": GOOGLE_MAPS_KEY},
            timeout=10,
        )
        data = r.json()
        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"])
    except Exception as e:
        print(f"  [Geocode] {e}")
    return None, None

def haversine_meters(lat1, lng1, lat2, lng2):
    """Distance in meters between two lat/lng points."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def load_consulates():
    """Geocode and save all consulates to Supabase. Returns list with coords."""
    print("[Consulates] Checking if already loaded...")
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/boston_consulates?select=id&limit=1",
            headers=HEADERS, timeout=10,
        )
        if r.json():
            print("[Consulates] Already loaded — fetching from Supabase")
            r2 = requests.get(
                f"{SUPABASE_URL}/rest/v1/boston_consulates?select=*&limit=100",
                headers=HEADERS, timeout=10,
            )
            return r2.json()
    except Exception:
        pass

    print(f"[Consulates] Geocoding {len(CONSULATES)} diplomatic facilities...")
    records = []
    for c in CONSULATES:
        lat, lng = geocode(c["address"])
        if lat and lng:
            record = {**c, "lat": lat, "lng": lng}
            records.append(record)
            print(f"  ✅ {c['country']}: {lat:.4f}, {lng:.4f}")
        else:
            print(f"  ❌ {c['country']}: geocode failed")
        import time; time.sleep(0.2)

    if records:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/boston_consulates",
            headers={**HEADERS, "Prefer": "resolution=ignore-duplicates,return=minimal"},
            json=records, timeout=20,
        )
        print(f"[Consulates] ✅ Saved {len(records)} facilities")
    return records

# Global cache — loaded once at startup
_consulates = None

def get_consulates():
    global _consulates
    if _consulates is None:
        _consulates = load_consulates()
    return _consulates

def check_diplomatic_proximity(lat, lng, radius_m=50):
    """
    Check if coordinates are within radius_m meters of any diplomatic facility.
    Returns (True, consulate_dict) or (False, None).
    """
    if not lat or not lng:
        return False, None
    for c in get_consulates():
        clat = c.get("lat")
        clng = c.get("lng")
        if not clat or not clng:
            continue
        dist = haversine_meters(lat, lng, float(clat), float(clng))
        if dist <= radius_m:
            return True, c
    return False, None

if __name__ == "__main__":
    # Standalone test
    records = load_consulates()
    print(f"\nLoaded {len(records)} consulates")
    # Test proximity
    # German Consulate is at 3 Copley Place — test with nearby coords
    if records:
        test_lat, test_lng = records[0]["lat"], records[0]["lng"]
        hit, match = check_diplomatic_proximity(test_lat + 0.0001, test_lng, 50)
        print(f"Proximity test: {'HIT' if hit else 'MISS'} — {match['country'] if match else 'none'}")
