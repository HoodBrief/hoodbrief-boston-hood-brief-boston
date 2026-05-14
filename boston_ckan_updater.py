"""
Hood Brief Boston — Multi-Dataset CKAN Updater
"""
import os, time, random
import requests
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

HEADERS = {
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
    "A1":  (42.3614, -71.0576), "A7":  (42.3697, -71.0335),
    "B2":  (42.3289, -71.0839), "B3":  (42.2932, -71.0801),
    "C6":  (42.3376, -71.0527), "C11": (42.3040, -71.0633),
    "D4":  (42.3421, -71.0724), "D14": (42.3521, -71.1546),
    "E5":  (42.2890, -71.1605), "E13": (42.3131, -71.1116),
    "E18": (42.2561, -71.1275),
}

WEIGHT_MAP = {
    "shooting":5,"homicide":5,"murder":5,
    "robbery":3,"assault":2,"burglary":2,"breaking":2,"larceny":1,
}

def get_weight(desc, shooting=False):
    if shooting: return 5
    d=(desc or "").lower()
    for k,w in WEIGHT_MAP.items():
        if k in d: return w
    return 1

def get_col(row, *keys):
    for k in keys:
        if k in row and row[k] not in (None,"","None"):
            return row[k]
    return ""

def fetch_dataset(resource_id, limit=5000):
    try:
        for attempt in range(3):
            r=requests.get(CKAN_BASE,params={"resource_id":resource_id,"limit":limit},
                          timeout=30,headers={"User-Agent":"Hood Brief/1.0"})
            if r.status_code==409:
                wait=(attempt+1)*15
                print(f"[CKAN] Rate limited - waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code==200:
                data=r.json()
                if data.get("success"):
                    recs=data["result"]["records"]
                    print(f"[CKAN] Fetched {len(recs)} from {resource_id[:8]}")
                    if recs: print(f"[CKAN] Cols: {list(recs[0].keys())[:6]}")
                    return recs
            print(f"[CKAN] HTTP {r.status_code}")
            break
    except Exception as e:
        print(f"[CKAN] Error: {e}")
    return []

def process_incidents(records):
    rows=[]
    for r in records:
        try:
            lat=float(get_col(r,"Lat","lat") or 0)
            lng=float(get_col(r,"Long","long") or 0)
            if not lat or not lng: continue
            if not (42.2<=lat<=42.4 and -71.2<=lng<=-70.9): continue
            inc=get_col(r,"INCIDENT_NUMBER","incident_number")
            if not inc: continue
            rows.append({
                "incident_number":str(inc),
                "offense_desc":get_col(r,"OFFENSE_DESCRIPTION","offense_description"),
                "occurred_on":get_col(r,"OCCURRED_ON_DATE","occurred_on_date"),
                "lat":lat,"lng":lng,
                "shooting":str(get_col(r,"SHOOTING","shooting")).upper()=="Y",
                "district":get_col(r,"DISTRICT","district"),
            })
        except Exception: continue
    return rows

def process_shootings(records):
    rows=[]
    for r in records:
        try:
            d=(get_col(r,"district","District") or "").strip().upper()
            c=DISTRICT_CENTROIDS.get(d)
            if not c: continue
            rows.append({
                "incident_id":str(get_col(r,"incident_num","Incident_Num","_id")),
                "occurred_on":get_col(r,"shooting_date","Shooting_Date"),
                "district":d,
                "fatal":str(get_col(r,"shooting_type_v2","Shooting_Type_V2")).strip()=="Fatal",
                "victim_count":int(get_col(r,"multi_victim","Multi_Victim") or 0)+1,
                "lat":c[0]+random.uniform(-0.004,0.004),
                "lng":c[1]+random.uniform(-0.004,0.004),
                "priority":"p1",
            })
        except Exception: continue
    return rows

def process_shots_fired(records):
    rows=[]
    for r in records:
        try:
            d=(get_col(r,"district","District") or "").strip().upper()
            c=DISTRICT_CENTROIDS.get(d)
            if not c: continue
            rows.append({
                "incident_id":str(get_col(r,"incident_num","Incident_Num","_id")),
                "occurred_on":get_col(r,"incident_date","Incident_Date"),
                "district":d,
                "lat":c[0]+random.uniform(-0.004,0.004),
                "lng":c[1]+random.uniform(-0.004,0.004),
                "priority":"p1",
            })
        except Exception: continue
    return rows

def upsert_batch(table, rows):
    if not rows: return 0
    inserted=0
    for i in range(0,len(rows),200):
        batch=rows[i:i+200]
        r=requests.post(f"{SUPABASE_URL}/rest/v1/{table}",headers=HEADERS,json=batch,timeout=20)
        if r.status_code in (200,201,204): inserted+=len(batch)
        else: print(f"[CKAN] Error {table}: {r.status_code} {r.text[:60]}")
    return inserted

def rebuild_heatmap():
    print("[Heatmap] Rebuilding...")
    pts=[]
    try:
        r=requests.get(f"{SUPABASE_URL}/rest/v1/boston_incidents",
            headers={**HEADERS,"Prefer":""},
            params={"select":"lat,lng,offense_desc,shooting","lat":"not.is.null","limit":5000},timeout=30)
        for row in r.json():
            try: pts.append({"lat":float(row["lat"]),"lng":float(row["lng"]),"weight":get_weight(row.get("offense_desc"),row.get("shooting",False))})
            except: pass
    except Exception as e: print(f"[Heatmap] incidents: {e}")
    try:
        r=requests.get(f"{SUPABASE_URL}/rest/v1/boston_shootings",
            headers={**HEADERS,"Prefer":""},
            params={"select":"lat,lng","lat":"not.is.null","limit":5000},timeout=30)
        for row in r.json():
            try: pts.append({"lat":float(row["lat"]),"lng":float(row["lng"]),"weight":5})
            except: pass
    except Exception as e: print(f"[Heatmap] shootings: {e}")
    if not pts: print("[Heatmap] No points"); return
    requests.delete(f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
        headers={**HEADERS,"Prefer":"return=minimal"},params={"id":"not.is.null"},timeout=15)
    for i in range(0,len(pts),500):
        requests.post(f"{SUPABASE_URL}/rest/v1/boston_heatmap_points",
            headers={**HEADERS,"Prefer":"return=minimal"},json=pts[i:i+500],timeout=20)
    print(f"[Heatmap] {len(pts):,} points")

def run():
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — Multi-Dataset Sync  ║")
    print("╚══════════════════════════════════════════╝")
    cycle=0
    while True:
        print(f"\n[CKAN] Cycle {cycle+1} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        records=fetch_dataset(RESOURCE_IDS["crime_incidents"])
        if records:
            n=upsert_batch("boston_incidents",process_incidents(records))
            print(f"[CKAN] Crime incidents: {n} upserted")
        time.sleep(5)
        records=fetch_dataset(RESOURCE_IDS["shootings"])
        if records:
            n=upsert_batch("boston_shootings",process_shootings(records))
            print(f"[CKAN] Shootings: {n} upserted")
        time.sleep(5)
        records=fetch_dataset(RESOURCE_IDS["shots_fired"])
        if records:
            n=upsert_batch("boston_shots_fired",process_shots_fired(records))
            print(f"[CKAN] Shots fired: {n} upserted")
        if cycle%7==0: rebuild_heatmap()
        cycle+=1
        print(f"[CKAN] Next update in 24 hours")
        time.sleep(24*3600)

if __name__=="__main__":
    run()
