"""
Hood Brief Boston — MSP Scanner Pipeline
==========================================
Three Broadcastify feeds → faster-whisper → rule-based parser → Supabase

Feeds:
  26120 — MSP Metro Boston (Troops A & H) — primary
  3969  — MSP Eastern MA (Essex/Middlesex) — north shore
  36603 — Boston Area Special Event / Working Incident — major incidents

Cost: $0/month (faster-whisper on Railway Pro, no AI APIs)
"""

import os, re, time, json, tempfile, threading, requests
from datetime import datetime, timezone
from faster_whisper import WhisperModel

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")
GOOGLE_MAPS_KEY  = os.environ.get("GOOGLE_MAPS_KEY", "")
CHUNK_SECONDS    = 30
MAX_RETRIES      = 3

# Stream URLs — set via Railway environment variables
# When tokens expire, update these variables in Railway (no code change needed)
# Get fresh URLs from: broadcastify.com/listen/feed/FEEDID -> DevTools -> Network -> .mp3
# BPD worker URL — Cloudflare Worker proxying RapidSOS WebSocket
BPD_WORKER_URL = os.environ.get("BPD_WORKER_URL", "")

CITIES = {
    "bpd_scan": {
        "label":      "Boston PD — All Districts",
        "stream_url": None,  # Uses BPD_WORKER_URL instead of direct MP3
        "center":     (42.3601, -71.0589),
        "use_worker": True,
    },
    "metro_boston": {
        "label":      "MSP Metro Boston",
        "stream_url": os.environ.get("STREAM_URL_METRO",   "https://listen.broadcastify.com/rqwh00c5y28p71b.mp3?nc=5094&xan=xtf9912b"),
        "center":     (42.3601, -71.0589),
        "use_worker": False,
    },
    "eastern_ma": {
        "label":      "MSP Eastern MA",
        "stream_url": os.environ.get("STREAM_URL_EASTERN", "https://listen.broadcastify.com/hm0rd2jq85x1tk3.mp3?nc=92154&xan=xtf9912b"),
        "center":     (42.4673, -71.0180),
        "use_worker": False,
    },
    "special_event": {
        "label":      "MSP Special Event",
        "stream_url": os.environ.get("STREAM_URL_SPECIAL", "https://listen.broadcastify.com/tq8nr7zdskbjy5h.mp3?nc=73155&xan=xtf9912b"),
        "center":     (42.3601, -71.0589),
        "use_worker": False,
    },
}

# ── MSP 10-Code Translation ───────────────────────────────────────────────────
# MSP uses a different system from Memphis — codes 1-22+ not 10-XX format
# They say "code 15" not "10-15"
# MSP uses single-digit codes, not 10-codes
# Source: Broadcastify feed 26120 description (verified)
# Syntax: "H5 583, give me a 2" or "212Echo E4, check XYZ for an 11 and 14"
CODES_MSP = {
    # Core MSP codes (single numbers)
    " a 1 ":   " hold radio traffic ",
    " a 2 ":   " phone your barracks ",
    " a 3 ":   " phone direct ",
    " a 4 ":   " out of service ",
    " code 4 ": " out of service ",
    " a 5 ":   " in service ",
    " code 5 ": " in service ",
    " a 6 ":   " location ",
    " a 7 ":   " return to barracks ",
    " a 8 ":   " stopping suspicious vehicle ",
    " a 9 ":   " registration check ",
    " a 10 ":  " stolen check ",
    " a 11 ":  " license check ",
    " a 12 ":  " any messages ",
    " a 13 ":  " radio check ",
    " a 14 ":  " warrants check ",
    " a 15 ":  " trooper in trouble ",
    " a 16 ":  " motor vehicle accident ",
    " a 17 ":  " clear ",
    " a 22 ":  " complainant ",
    " a 30 ":  " trooper needs assistance ",
    # MSP terminology
    "dmv":     "disabled motor vehicle",
    "q5":      "suicidal statements",
    "bolo":    "be on lookout",
    "oln":     "license number",
    "bdl":     "breakdown lane",
    "pi":      "personal injury accident",
    "dre":     "drug recognition expert",
    "cars":    "collision analysis reconstruction",
}

def translate_codes(text):
    result = " " + text + " "
    for code, meaning in CODES_MSP.items():
        result = result.replace(code, meaning)
        result = result.replace(code.upper(), meaning)
    # Also handle 10-code format if used
    result = re.sub(r"\b10-4\b", "acknowledged", result, flags=re.I)
    result = re.sub(r"\b10-33\b", "emergency", result, flags=re.I)
    result = re.sub(r"\b10-99\b", "trooper needs help", result, flags=re.I)
    return re.sub(r" {2,}", " ", result).strip()

# ── MSP-Specific Incident Patterns ───────────────────────────────────────────
P1_PATTERNS = [
    # Violent / weapons
    r"\bshooting\b", r"\bshots?\s+fired\b", r"\bshot\b",
    r"\bgun\b", r"\bfirearm\b", r"\bweapon\b", r"\bknife\b",
    r"\bstabbing\b", r"\bstab\b",
    r"\brobbery\b", r"\bholdup\b", r"\bhold-?up\b",
    r"\baggravated\s+assault\b",
    r"\bhomicide\b", r"\bmurder\b",
    r"\bkidnap\b", r"\bhostage\b",
    r"\brape\b", r"\bsexual\s+assault\b",
    r"\bcarjack\b", r"\bhome\s+invasion\b",
    r"\barmed\b", r"\bmen\s+with\s+guns\b",
    r"\bperson\s+with\s+a\s+gun\b",
    r"\bpursuit\b", r"\bvehicle\s+pursuit\b",
    r"\bfoot\s+pursuit\b", r"\bfleeing\b",
    r"\bin\s+pursuit\b", r"\bactive\s+pursuit\b",
    r"\bofficer\s+(in\s+trouble|needs\s+help)\b",
    r"\bcode[-\s]15\b",
    r"\b10-99\b", r"\b10-15\b", r"\b10-33\b",
    r"\bbarricade\b", r"\bactive\s+shooter\b",
    r"\bswat\b", r"\bperimeter\b",
    r"\bdead\b", r"\bdeceased\b", r"\bdoa\b",
    r"\bthreatening\b", r"\bwill\s+shoot\b",
    r"\bshoot\s+each\s+other\b",
    r"\bagainst\s+their\s+will\b",
    r"\blarge\s+fight\b", r"\bbrawl\b",
    r"\bfight\s+in\s+progress\b",
]

P2_PATTERNS = [
    r"\bdomestic\b", r"\bburglary\b", r"\bbreak[\-\s]?in\b",
    r"\baccident\b", r"\bcollision\b", r"\bcrash\b",
    r"\bmva\b",  # Motor Vehicle Accident — MSP common term
    r"\bassault\w*\b", r"\bsuspicious\b",
    r"\btheft\b", r"\blarceny\b", r"\bstolen\b",
    r"\bvandal\w*\b", r"\bdrug\b", r"\bnarcotic\b",
    r"\btrespass\w*\b", r"\bharass\w*\b",
    r"\bmissing\s+person\b", r"\bmissing\s+juvenile\b",
    r"\battempt\s+to\s+locate\b", r"\batl\b", r"\brunaway\b",
    r"\bwelfare\s+check\b", r"\bcheck\s+on\b",
    r"\balarm\b", r"\bburglary\s+alarm\b",
    r"\btold\s+\w+\s+to\s+leave\b", r"\brefuses\s+to\s+leave\b",
    r"\bdisorderly\b", r"\bdisturbance\b",
    r"\bin\s+custody\b", r"\bdetained\b",
    r"\bwarrant\b", r"\bfelony\b",
    r"\bshoplifting\b", r"\bmerchandise\b",
    r"\bsuspect\b",
]

MEDICAL_PATTERNS = [
    r"\bmedical\b", r"\bambulance\b", r"\bems\b",
    r"\bmedic\b",
    r"\bunconsci\w+\b", r"\bunresponsive\b",
    r"\boverdos\w+\b", r"\bnot\s+breathing\b",
    r"\bcardiac\b", r"\bseizure\b",
    r"\binjur\w+\b",
    r"\bmedical\s+transport\b",
    r"\bfacility\b",
    r"\bmeds\b", r"\bmedication\b",
    r"\bpsych\w*\b", r"\bmental\b", r"\bptsd\b",
    r"\bsuicid\w+\b",
    r"\bharm\s+him\w*\b", r"\bharm\s+her\w*\b",
    r"\bwanted\s+to\s+harm\b",
    r"\bcpr\b",
]

TITLE_MAP = [
    ("active shooter",      "Active Shooter"),
    ("shots fired",         "Shots Fired"),
    ("shooting",            "Shooting"),
    ("shot",                "Shooting"),
    ("homicide",            "Homicide"),
    ("murder",              "Homicide"),
    ("stabbing",            "Stabbing"),
    ("aggravated assault",  "Aggravated Assault"),
    ("robbery",             "Robbery in Progress"),
    ("hold-up",             "Hold-Up"),
    ("holdup",              "Hold-Up"),
    ("carjacking",          "Carjacking"),
    ("home invasion",       "Home Invasion"),
    ("armed",               "Armed Subject"),
    ("person with a gun",   "Armed Person"),
    ("men with guns",       "Armed Persons"),
    ("weapon",              "Weapons Call"),
    ("gun",                 "Weapons Call"),
    ("kidnap",              "Kidnapping"),
    ("hostage",             "Hostage Situation"),
    ("barricade",           "Barricaded Subject"),
    ("active shooter",      "Active Shooter"),
    ("pursuit",             "Vehicle Pursuit"),
    ("foot pursuit",        "Foot Pursuit"),
    ("fleeing",             "Fleeing Suspect"),
    ("officer in trouble",  "Officer Needs Help"),
    ("officer needs help",  "Officer Needs Help"),
    ("swat",                "SWAT Response"),
    ("dead",                "Deceased Person"),
    ("deceased",            "Deceased Person"),
    ("doa",                 "Dead on Arrival"),
    ("will shoot",          "Threat to Shoot"),
    ("threatening",         "Terroristic Threatening"),
    ("large fight",         "Large Fight"),
    ("brawl",               "Brawl"),
    ("fight in progress",   "Fight in Progress"),
    ("domestic",            "Domestic Disturbance"),
    ("disturbance",         "Disturbance"),
    ("arguing",             "Domestic Disturbance"),
    ("burglary",            "Burglary"),
    ("break-in",            "Breaking and Entering"),
    ("mva",                 "Motor Vehicle Accident"),
    ("accident",            "Motor Vehicle Accident"),
    ("collision",           "Motor Vehicle Accident"),
    ("crash",               "Vehicle Crash"),
    ("theft",               "Theft"),
    ("larceny",             "Larceny"),
    ("shoplifting",         "Shoplifting"),
    ("stolen",              "Stolen Vehicle/Property"),
    ("vandalism",           "Vandalism"),
    ("drug",                "Drug Activity"),
    ("missing juvenile",    "Missing Juvenile"),
    ("missing person",      "Missing Person"),
    ("attempt to locate",   "Attempt to Locate"),
    ("atl",                 "Attempt to Locate"),
    ("runaway",             "Runaway"),
    ("welfare check",       "Welfare Check"),
    ("alarm",               "Alarm Response"),
    ("suspicious",          "Suspicious Person/Vehicle"),
    ("trespassing",         "Trespassing"),
    ("warrant",             "Warrant Check"),
    ("in custody",          "Subject in Custody"),
    ("suicide",             "Suicide Call"),
    ("suicidal",            "Suicidal Subject"),
    ("harm himself",        "Self-Harm Call"),
    ("harm herself",        "Self-Harm Call"),
    ("overdose",            "Overdose"),
    ("unconscious",         "Unconscious Person"),
    ("unresponsive",        "Unresponsive Person"),
    ("cardiac",             "Cardiac Emergency"),
    ("seizure",             "Seizure"),
    ("cpr",                 "CPR in Progress"),
    ("ambulance",           "Medical Emergency"),
    ("medical",             "Medical Call"),
    ("medic",               "Medical Call"),
    ("mental",              "Mental Health Call"),
    ("psych",               "Mental Health Call"),
    ("ptsd",                "Mental Health Call"),
]

NOISE_PHRASES = [
    "buzzcutting his way to a small fortune",
    "every time he cuts his own hair",
    "sound of jack", "sound of claire",
    "cooking dinner at home",
    "fraud alert from wells fargo",
    "police scanner radio dispatch",
    "all feels right in the world",
    "15-year-old harper", "vintage rock t-shirt",
]

# ── Boston Metro Neighborhoods / Hotspots ────────────────────────────────────
BOSTON_HOTSPOTS = [
    {"name": "Roxbury",       "streets": ["blue hill", "dudley st", "warren st", "humboldt", "dale st", "washington st roxbury"]},
    {"name": "Mattapan",      "streets": ["morton st", "mattapan sq", "river st", "cummins hwy", "blue hill ave mattapan"]},
    {"name": "Dorchester",    "streets": ["bowdoin st", "columbia rd", "geneva ave", "talbot ave", "harvard st dorchester"]},
    {"name": "Hyde Park",     "streets": ["hyde park ave", "fairmount ave", "cleary sq"]},
    {"name": "East Boston",   "streets": ["meridian st", "chelsea st", "maverick sq", "paris st"]},
    {"name": "South Boston",  "streets": ["old colony", "silver st", "east broadway", "west broadway", "old harbor"]},
    {"name": "Jamaica Plain", "streets": ["egleston sq", "jackson sq", "stony brook"]},
    {"name": "Mission Hill",  "streets": ["huntington ave mission", "brigham circle"]},
    {"name": "Lynn",          "streets": ["lynn", "western ave lynn", "union st lynn"]},
    {"name": "Revere",        "streets": ["revere beach", "broadway revere", "american legion"]},
    {"name": "Chelsea",       "streets": ["chelsea", "broadway chelsea", "washington ave chelsea"]},
]

def check_hotspot(text):
    tl = (text or "").lower()
    for zone in BOSTON_HOTSPOTS:
        if any(s in tl for s in zone["streets"]):
            return True, zone["name"]
    return False, None

# ── MSP Barracks / District Detection ────────────────────────────────────────
# Based on address patterns heard on scanner
BARRACKS = {
    "H1": {"name": "Government Center",  "color": "#6366f1"},
    "H2": {"name": "South Boston",       "color": "#ec4899"},
    "H3": {"name": "Brighton",           "color": "#14b8a6"},
    "H4": {"name": "Dedham",             "color": "#f97316"},
    "H6": {"name": "Milton",             "color": "#a855f7"},
    "H7": {"name": "Norwood",            "color": "#22c55e"},
    "H8": {"name": "Weston/Mass Pike",   "color": "#eab308"},
    "A2": {"name": "Newbury/North Shore","color": "#06b6d4"},
    "A4": {"name": "Concord/Metrowest",  "color": "#38bdf8"},
    "A5": {"name": "Revere/East",        "color": "#f43f5e"},
    "A6": {"name": "Medford/North",      "color": "#84cc16"},
    "SE": {"name": "Special Event",      "color": "#ef4444"},
}

# ── Whisper Model ─────────────────────────────────────────────────────────────
_whisper_model = None
def get_whisper_model():
    global _whisper_model
    if _whisper_model is None:
        print("[Whisper] Loading faster-whisper small model...")
        _whisper_model = WhisperModel("small", device="cpu", compute_type="int8")
        print("[Whisper] Model ready")
    return _whisper_model

# ── Transcription ─────────────────────────────────────────────────────────────
def transcribe(audio_bytes):
    tmp_path = None
    for attempt in range(MAX_RETRIES):
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
                f.write(audio_bytes)
                tmp_path = f.name
            model = get_whisper_model()
            segments, _ = model.transcribe(
                tmp_path,
                language="en",
                beam_size=5,
                temperature=0.0,
                vad_filter=True,
                vad_parameters={"min_silence_duration_ms": 300, "threshold": 0.6},
                initial_prompt=(
                    "Massachusetts State Police scanner radio. "
                    "Single digit codes: give me a 2, on a 4, give me a 16. "
                    "Trooper units like H5 583, A2 212, H-PTL-1. "
                    "MSP terms: DMV, BDL, rolling 9, make the flip, direction of flight. "
                    "Massachusetts highway routes, Boston metro addresses and towns."
                ),
            )
            text = " ".join(s.text for s in segments).strip()
            if text:
                words = text.lower().split()
                if len(words) > 6 and len(set(words)) / len(words) < 0.25:
                    print("  [Whisper] Repetition detected — rejecting")
                    return ""
                if any(m in text.lower() for m in NOISE_PHRASES):
                    print("  [Whisper] Known hallucination — rejecting")
                    return ""
            return text
        except Exception as e:
            print(f"  Whisper attempt {attempt+1} failed: {e}")
            time.sleep(2)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try: os.unlink(tmp_path)
                except: pass
    return ""

# ── Audio Capture ─────────────────────────────────────────────────────────────
def capture_bpd_chunk(seconds):
    """Capture audio from BPD RapidSOS via Cloudflare Worker HTTP endpoint."""
    if not BPD_WORKER_URL:
        print("  [BPD] BPD_WORKER_URL not set — skipping")
        return b""
    try:
        url = f"{BPD_WORKER_URL}?channel=BPD_SCAN&http=1&seconds={seconds}"
        r = requests.get(url, timeout=seconds + 15,
                        headers={"User-Agent": "HoodBrief/1.0"})
        if r.status_code == 204:
            return b""  # No audio
        if r.status_code != 200:
            print(f"  [BPD Worker] HTTP {r.status_code}")
            return b""
        return r.content
    except Exception as e:
        print(f"  [BPD Worker] Error: {e}")
        return b""

def capture_chunk(stream_url, seconds):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(
                stream_url, stream=True, timeout=seconds + 10,
                headers={"User-Agent": "Mozilla/5.0 (compatible; HoodBrief/1.0)"},
            )
            if r.status_code == 401:
                print(f"  ⚠ Token expired (401) — update STREAM_URL_* in Railway vars")
                return b""
            if r.status_code == 403:
                print(f"  ⚠ Token expired (403) — update STREAM_URL_* in Railway vars")
                return b""
            if r.status_code != 200:
                print(f"  Audio HTTP {r.status_code} — skipping")
                return b""
            chunks = []
            start = time.time()
            for chunk in r.iter_content(chunk_size=4096):
                chunks.append(chunk)
                if time.time() - start >= seconds:
                    break
            return b"".join(chunks)
        except Exception as e:
            print(f"  Audio capture attempt {attempt+1} failed: {e}")
            time.sleep(3)
    return b""

# ── Parser ────────────────────────────────────────────────────────────────────
BAD_LOCATIONS = [
    "this thing", "claim", "show down", "the area", "the scene",
    "location", "address", "service", "station", "barracks",
    "dispatch", "check", "unit", "trooper", "alpha", "bravo",
    "charlie", "delta", "echo", "foxtrot", "tango", "victor",
    "over the phone", "by phone", "via phone", "on the phone",
    "driver needs", "passenger side",
    "north", "south", "east", "west",
    "residence", "complainant", "front", "inside", "outside",
    "the road", "the street", "roadway", "highway",
]

LOCATION_PATTERNS = [
    # Numbered address + street with suffix
    r"(?:at|on|to|near)\s+(\d+\s+[\w\s]{2,35}?\s+(?:ave(?:nue)?|st(?:reet)?|rd|road|blvd|boulevard|dr(?:ive)?|ln|lane|way|cir(?:cle)?|ct|court|pl(?:ace)?|pkwy|parkway|hwy|highway|pike|turnpike))",
    # Intersection
    r"([\w\s]+(?:ave(?:nue)?|st(?:reet)?|rd|road|blvd|dr(?:ive)?|ln|way)\s+and\s+[\w\s]{3,25})",
    # Numbered address no suffix
    r"(?:at|on|to|near|of)\s+(\d+\s+[A-Z][\w\s]{2,25})",
    # Any numbered address
    r"(\d{3,5}\s+[A-Z][\w]{3,20})",
    # Interstate / highway
    r"\b(interstate\s+\d+|i-\d+|route\s+\d+|rte\s+\d+|mass\s+pike|i-90|i-93|i-95|route\s+128|route\s+1)\b",
    # Bare street name with suffix
    r"\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\s+(?:road|street|avenue|drive|lane|boulevard|way|circle|court|place|parkway|pike))\b",
    # MA towns on scanner
    r"\b(boston|cambridge|somerville|quincy|braintree|dedham|newton|waltham|medford|malden|everett|revere|chelsea|lynn|peabody|salem|beverly|danvers|andover|lowell|lawrence)\b",
]

UNIT_PATTERNS = [
    r"\b(h[\-]?\d+)\b",   # H-troop units
    r"\b(a[\-]?\d+)\b",   # A-troop units
    r"\b(\d{3,4})\s+(?:en\s+route|responding|on\s+scene|copy)\b",
]

def parse_incident(text, city):
    tl = text.lower().strip()
    if len(tl) < 15:
        return {"incident": False}

    noise_hits = sum(1 for p in NOISE_PHRASES if p in tl)
    has_signal = (
        any(re.search(p, tl, re.I) for p in P1_PATTERNS) or
        any(re.search(p, tl, re.I) for p in P2_PATTERNS) or
        any(re.search(p, tl, re.I) for p in MEDICAL_PATTERNS)
    )
    if noise_hits >= 1 and not has_signal:
        return {"incident": False}

    # Priority
    if any(re.search(p, tl, re.I) for p in P1_PATTERNS):
        priority = "p1"
    elif any(re.search(p, tl, re.I) for p in MEDICAL_PATTERNS):
        priority = "medical"
    elif any(re.search(p, tl, re.I) for p in P2_PATTERNS):
        priority = "p2"
    else:
        print("  Routine call (P3) — skipping")
        return {"incident": False}

    if priority == "p1" and len(tl.split()) < 6:
        print("  P1 too short — skipping")
        return {"incident": False}

    # Property crimes can't be Medical
    PROPERTY = ["shoplifting","burglary","larceny","theft","vandal","trespass","stolen"]
    if priority == "medical" and any(k in tl for k in PROPERTY):
        priority = "p1" if any(re.search(p,tl,re.I) for p in P1_PATTERNS) else "p2"

    # Title
    title = None
    for keyword, label in TITLE_MAP:
        if keyword in tl:
            title = label
            break
    if not title:
        title = {"p1":"Priority 1 Call","p2":"Priority 2 Call","medical":"Medical Call"}.get(priority,"Incident")

    # Location
    location = None
    for pattern in LOCATION_PATTERNS:
        m = re.search(pattern, text, re.I)
        if m:
            candidate = m.group(1).strip().title()
            if re.match(r"^\d{3,4}\s+hours?$", candidate, re.I): continue
            if re.match(r"^(19|20)\d{2}\s+\w+$", candidate, re.I): continue
            if re.match(r"^\d+\s+(North|South|East|West)$", candidate, re.I): continue
            if candidate.lower().strip() in BAD_LOCATIONS: continue
            if any(phrase in candidate.lower() for phrase in ["over the phone","by phone","on scene"]): continue
            if len(candidate) > 4:
                location = candidate
                break

    if not location:
        return {"incident": False}

    if location.lower().strip() in BAD_LOCATIONS:
        return {"incident": False}

    # Unit
    unit = None
    for pattern in UNIT_PATTERNS:
        m = re.search(pattern, tl, re.I)
        if m:
            unit = m.group(1).strip().upper()
            break

    return {
        "incident": True,
        "title":    title,
        "location": location,
        "priority": priority,
        "unit":     unit or "",
    }

# ── Geocoding ─────────────────────────────────────────────────────────────────
BOSTON_LANDMARKS = {
    "fenway park":          (42.3467, -71.0972),
    "td garden":            (42.3662, -71.0621),
    "logan airport":        (42.3656, -71.0096),
    "south station":        (42.3520, -71.0552),
    "north station":        (42.3662, -71.0621),
    "faneuil hall":         (42.3600, -71.0560),
    "government center":    (42.3597, -71.0590),
    "copley square":        (42.3496, -71.0773),
    "kenmore square":       (42.3483, -71.0970),
    "harvard square":       (42.3732, -71.1190),
    "central square":       (42.3651, -71.1039),
    "kendall square":       (42.3626, -71.0843),
    "porter square":        (42.3884, -71.1194),
    "dudley square":        (42.3231, -71.0836),
    "mattapan square":      (42.2676, -71.0920),
    "egleston square":      (42.3121, -71.1015),
    "jackson square":       (42.3203, -71.1073),
    "brigham and women":    (42.3356, -71.1067),
    "boston medical":       (42.3355, -71.0726),
    "mass general":         (42.3636, -71.0687),
    "beth israel":          (42.3378, -71.1064),
    "children's hospital":  (42.3378, -71.1064),
    "tufts medical":        (42.3494, -71.0627),
    "revere beach":         (42.4077, -70.9925),
    "lynn common":          (42.4673, -70.9495),
    "chelsea square":       (42.3918, -71.0328),
    "maverick square":      (42.3706, -71.0393),
    "orient heights":       (42.3817, -71.0048),
    "mass pike":            (42.3467, -71.1800),
    "route 128":            (42.2626, -71.0200),
    "i-93":                 (42.3601, -71.0589),
    "i-95":                 (42.3833, -71.2333),
    "tobin bridge":         (42.3986, -71.0617),
    "zakim bridge":         (42.3673, -71.0646),
    "1010":                 (42.3636, -71.0687),
}

def check_landmark(text):
    tl = text.lower()
    for kw, coords in BOSTON_LANDMARKS.items():
        if kw in tl:
            return coords
    return None

def in_boston_metro(lat, lng):
    """Boston metro bounding box — generous to cover all MSP Troop A & H territory."""
    return 41.8 <= lat <= 42.9 and -71.9 <= lng <= -70.5

def geocode_location(location_text):
    if not location_text:
        return None, None

    # Landmark check
    lm = check_landmark(location_text)
    if lm:
        return lm, None

    # MA 911 DB (Supabase) — same prefix-search approach as Memphis
    try:
        normalized = location_text.strip().upper()
        clean = re.sub(r'\s+(BOSTON|CAMBRIDGE|QUINCY|LYNN|REVERE|CHELSEA|MA|MASSACHUSETTS).*$', '', normalized).strip()
        clean = re.sub(
            r'\s+(AVE|ST|RD|BLVD|DR|LN|WAY|CIR|CT|PL|PKWY|HWY|ROAD|'
            r'AVENUE|STREET|DRIVE|LANE|CIRCLE|COURT|PLACE|PARKWAY|HIGHWAY|PIKE|TURNPIKE)$',
            '', clean
        ).strip()
        # Direction normalization
        clean = re.sub(r'\bNORTH\b', 'N', clean)
        clean = re.sub(r'\bSOUTH\b', 'S', clean)
        clean = re.sub(r'\bEAST\b',  'E', clean)
        clean = re.sub(r'\bWEST\b',  'W', clean)

        if ' AND ' in clean:
            parts = [p.strip() for p in clean.split(' AND ', 1)]
            s1 = re.sub(r'^\d+\s+', '', parts[0]).strip()
            s2 = re.sub(r'^\d+\s+', '', parts[1]).strip()

            r1 = ma_db_lookup(s1)
            r2 = ma_db_lookup(s2)

            if r1 and r2:
                label = f"Intersection: {s1.title()} & {s2.title()}"
                print(f"  Geocoded (MA 911 DB intersection): {label}")
                return (r1[0], r1[1]), label
            elif r1:
                return (r1[0], r1[1]), None
            elif r2:
                return (r2[0], r2[1]), None
        else:
            result = ma_db_lookup(clean)
            if result:
                print(f"  Geocoded (MA 911 DB): {clean}")
                return (result[0], result[1]), None
    except Exception as e:
        print(f"  [MA 911 DB] Error: {e}")

    # Google fallback
    if GOOGLE_MAPS_KEY:
        coords = google_geocode(location_text)
        if coords:
            return coords, None

    print(f"  Location not verified — skipping: {location_text}")
    return None, None

def ma_db_lookup(query):
    """
    Prefix range search on ma_addresses Supabase table.
    Boston SAM addresses have lat/lng — use directly.
    Town addresses (no lat/lng) — verify exists then Google geocode.
    Returns (lat, lng, town) or None.
    """
    query = query.strip().upper()
    if not query or len(query) < 3:
        return None
    sentinel = query[:-1] + chr(ord(query[-1]) + 1)
    try:
        from urllib.parse import quote
        q_enc = quote(query, safe='')
        s_enc = quote(sentinel, safe='')
        url = (
            f"{SUPABASE_URL}/rest/v1/ma_addresses"
            f"?address=gte.{q_enc}&address=lt.{s_enc}"
            f"&select=lat%2Clng%2Ctown%2Czip&limit=1&order=address"
        )
        r = requests.get(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }, timeout=10)
        r.raise_for_status()
        rows = r.json()
        if rows:
            row = rows[0]
            lat = row.get("lat")
            lng = row.get("lng")
            town = row.get("town", "")
            # Boston SAM addresses have coordinates — use directly
            if lat and lng:
                lat, lng = float(lat), float(lng)
                if in_boston_metro(lat, lng):
                    return lat, lng, town
            # Town addresses verified but no coords — Google geocode with town
            elif GOOGLE_MAPS_KEY and town:
                coords = google_geocode(f"{query}, {town} MA")
                if coords:
                    return coords[0], coords[1], town
    except Exception as e:
        print(f"  [MA 911 DB] Error: {e}")
    return None

def google_geocode(query):
    if not GOOGLE_MAPS_KEY or not query or len(query) < 4:
        return None
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": f"{query}, Boston MA", "key": GOOGLE_MAPS_KEY},
            timeout=10,
        )
        data = r.json()
        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            lat, lng = float(loc["lat"]), float(loc["lng"])
            # Reject city center fallback
            if abs(lat - 42.3601) < 0.01 and abs(lng - (-71.0589)) < 0.01:
                print(f"  [Google] City center fallback rejected: {query}")
                return None
            if in_boston_metro(lat, lng):
                print(f"  Geocoded (Google): {query} -> {lat}, {lng}")
                return lat, lng
    except Exception as e:
        print(f"  [Google] Error: {e}")
    return None

# ── Supabase ──────────────────────────────────────────────────────────────────
def sb_get(path, params=None):
    from urllib.parse import quote
    base = f"{SUPABASE_URL}/rest/v1/{path}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if not params:
        r = requests.get(base, headers=headers, timeout=15)
        r.raise_for_status()
        return r.json()
    parts = []
    for k, v in params.items():
        parts.append(f"{quote(str(k),safe='')}={quote(str(v),safe='*')}")
    r = requests.get(f"{base}?{'&'.join(parts)}", headers=headers, timeout=15)
    r.raise_for_status()
    return r.json()

def save_incident(parsed, city, transcript_raw, transcript_translated, hotspot, zone, feed_label):
    data = {
        "city":             city,
        "feed":             feed_label,
        "transcript":       transcript_translated[:500],
        "transcript_raw":   transcript_raw[:500],
        "title":            parsed["title"],
        "location":         parsed["location"],
        "lat":              parsed["lat"],
        "lng":              parsed["lng"],
        "unit":             parsed.get("unit",""),
        "priority":         parsed["priority"],
        "gang_hotspot":     hotspot,
        "gang_zone":        zone,
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/boston_scanner_incidents",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "return=minimal",
        },
        json=data,
        timeout=15,
    )
    if r.status_code not in (200, 201, 204):
        print(f"  [Supabase] Error {r.status_code}: {r.text[:150]}")

# ── City Runner ───────────────────────────────────────────────────────────────
def run_feed(feed_key):
    info       = CITIES[feed_key]
    stream_url = info["stream_url"]
    label      = info["label"]
    use_worker = info.get("use_worker", False)
    prev_transcript = ""
    last_saved_key  = ""
    last_saved_time = 0

    print(f"[{label}] Started — capturing {CHUNK_SECONDS}s chunks...")

    while True:
        try:
            if use_worker:
                audio = capture_bpd_chunk(CHUNK_SECONDS)
            else:
                audio = capture_chunk(stream_url, CHUNK_SECONDS)
            if len(audio) < 1000:
                print(f"[{label}] Audio too small — skipping")
                time.sleep(5)
                continue

            transcript_raw = transcribe(audio)
            if not transcript_raw or len(transcript_raw.strip()) < 8:
                print(f"[{label}] No speech detected — skipping")
                prev_transcript = ""
                continue

            print(f"[{label}] Raw: {transcript_raw[:120]}...")
            transcript_translated = translate_codes(transcript_raw)
            if transcript_translated != transcript_raw:
                print(f"[{label}] Translated: {transcript_translated[:120]}...")

            combined = f"{prev_transcript} {transcript_translated}".strip() if prev_transcript else transcript_translated
            prev_transcript = transcript_translated

            parsed = parse_incident(combined, feed_key)
            if not parsed.get("incident"):
                print(f"[{label}] No incident detected — skipping")
                continue

            priority = parsed["priority"]
            location = parsed["location"]

            coords, intersection_label = geocode_location(location)
            if coords is None:
                print(f"[{label}] Location not verifiable — not posting")
                continue

            lat, lng = coords
            parsed["lat"] = lat
            parsed["lng"] = lng
            if intersection_label:
                parsed["location"] = intersection_label

            hotspot, zone = check_hotspot(combined)

            # Dedup
            dedup_key = f"{parsed['location']}|{priority}"
            if dedup_key == last_saved_key and (time.time() - last_saved_time) < 180:
                print(f"[{label}] Duplicate suppressed")
                prev_transcript = ""
                continue

            save_incident(parsed, feed_key, transcript_raw, transcript_translated, hotspot, zone, label)
            last_saved_key  = dedup_key
            last_saved_time = time.time()
            prev_transcript = ""
            print(f"[{label}] ✅ Saved: [{priority.upper()}] {parsed['title']} @ {parsed['location']}")

        except Exception as e:
            print(f"[{label}] Error: {e}")
            time.sleep(5)

# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — MSP Scanner         ║")
    print("║  3 Feeds · Troops A & H · Special Event  ║")
    print("╚══════════════════════════════════════════╝")

    errors = []
    if not SUPABASE_URL: errors.append("SUPABASE_URL not set")
    if not SUPABASE_KEY: errors.append("SUPABASE_KEY not set")
    if errors:
        for e in errors: print(f"  ❌ {e}")
        exit(1)

    threads = []
    for feed_key in CITIES:
        t = threading.Thread(target=run_feed, args=(feed_key,), daemon=True, name=feed_key)
        t.start()
        threads.append(t)
        print(f"  ✓ Started: {CITIES[feed_key]['label']}")

    print("All feeds running.")
    while True:
        time.sleep(60)
        alive = [t.name for t in threads if t.is_alive()]
        dead  = [t.name for t in threads if not t.is_alive()]
        print(f"[Heartbeat] Active: {', '.join(alive) or 'none'} | DEAD: {', '.join(dead) or 'none'}")
