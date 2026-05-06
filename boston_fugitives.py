"""
Hood Brief Boston — Fugitive Scraper
Three sources:
  1. massmostwanted.org   — Metro LEC fugitives
  2. police.boston.gov    — BPD Most Wanted (image posts)
  3. mass.gov/msp-wanted  — MSP Violent Fugitive Apprehension Section
"""

import os, re, time, requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_KEY", "")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; HoodBriefBoston/1.0)"}

# ── Boston metro bounds ───────────────────────────────────────────────────────
def in_boston_metro(lat, lng):
    return 41.8 <= lat <= 42.9 and -71.9 <= lng <= -70.5

# ── Google Geocoding ──────────────────────────────────────────────────────────
def geocode(address):
    if not GOOGLE_MAPS_KEY or not address:
        return None, None
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": f"{address}, Massachusetts", "key": GOOGLE_MAPS_KEY},
            timeout=10,
        )
        data = r.json()
        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            lat, lng = float(loc["lat"]), float(loc["lng"])
            if in_boston_metro(lat, lng):
                return lat, lng
    except Exception as e:
        print(f"  [Geocode] Error: {e}")
    return None, None

# ── Supabase ──────────────────────────────────────────────────────────────────
def save_fugitives(records):
    if not records:
        return
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/boston_fugitives",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=records,
        timeout=20,
    )
    if r.status_code not in (200, 201, 204):
        print(f"  [Supabase] Error {r.status_code}: {r.text[:150]}")

# ── Source 1: massmostwanted.org ──────────────────────────────────────────────
def scrape_massmostwanted():
    print("[MMW] Scraping massmostwanted.org...")
    records = []
    for page in range(1, 4):
        url = f"https://www.massmostwanted.org/?fugitive=true&paged={page}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            cases = soup.select("a[href*='/cases/']")
            if not cases:
                break
            for case in cases:
                href = case.get("href", "")
                text = case.get_text(" ", strip=True)
                # Extract fields from link text
                crime_type = ""
                location = ""
                date_str = ""
                case_id = ""
                photo_url = ""

                m_crime = re.search(r"Crime Type:\s*(.+?)(?:Location:|$)", text)
                m_loc   = re.search(r"Location:\s*(.+?)(?:Crime Date:|$)", text)
                m_date  = re.search(r"Crime Date:\s*(\d{2}/\d{2}/\d{4})", text)
                m_case  = re.search(r"Case ID:\s*(\S+)", text)
                img = case.select_one("img")
                if img:
                    photo_url = img.get("src") or img.get("data-src") or ""

                if m_crime: crime_type = m_crime.group(1).strip()
                if m_loc:   location   = m_loc.group(1).strip()
                if m_date:  date_str   = m_date.group(1).strip()
                if m_case:  case_id    = m_case.group(1).strip()

                if not case_id:
                    continue

                # Geocode location
                lat, lng = geocode(location) if location else (None, None)

                records.append({
                    "source":      "massmostwanted",
                    "case_id":     case_id,
                    "name":        "",
                    "charges":     crime_type,
                    "address":     location,
                    "lat":         lat,
                    "lng":         lng,
                    "photo_url":   photo_url,
                    "source_url":  href,
                    "scraped_at":  datetime.now(timezone.utc).isoformat(),
                })
                print(f"  [MMW] {case_id} — {crime_type} @ {location}")
                time.sleep(0.3)

        except Exception as e:
            print(f"  [MMW] Error page {page}: {e}")
            break

    print(f"[MMW] Found {len(records)} fugitives")
    return records

# ── Source 2: BPD Most Wanted ─────────────────────────────────────────────────
def scrape_bpd_wanted():
    print("[BPD] Scraping police.boston.gov most wanted...")
    records = []
    try:
        url = "https://police.boston.gov/most-wanted/"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        # BPD most wanted page has individual suspect entries
        entries = soup.select(".wanted-person, .suspect-card, article, .wp-block-group")
        if not entries:
            # Fall back to parsing the page content directly
            entries = soup.select("figure, .wp-block-image")

        # Get the most recent post which has the lineup image
        posts = soup.select("article, .post, .entry")
        for post in posts[:5]:
            title = post.select_one("h1,h2,h3")
            img = post.select_one("img")
            link = post.select_one("a[href*='most-wanted']")
            date = post.select_one("time")

            if not img:
                continue

            photo_url = img.get("src") or img.get("data-src") or ""
            post_url = link.get("href", url) if link else url
            date_str = date.get("datetime", "") if date else ""
            title_text = title.get_text(strip=True) if title else "BPD Most Wanted"

            records.append({
                "source":      "bpd",
                "case_id":     f"BPD-{date_str[:10] if date_str else 'unknown'}",
                "name":        title_text,
                "charges":     "See BPD Most Wanted",
                "address":     "Boston, MA",
                "lat":         42.3601,
                "lng":         -71.0589,
                "photo_url":   photo_url,
                "source_url":  post_url,
                "scraped_at":  datetime.now(timezone.utc).isoformat(),
            })
            print(f"  [BPD] {title_text}")

        # Also check the individual most wanted page
        r2 = requests.get("https://police.boston.gov/category/boston-most-wanted/",
                          headers=HEADERS, timeout=15)
        soup2 = BeautifulSoup(r2.text, "html.parser")
        posts2 = soup2.select("article.post, .post-item")
        for post in posts2[:10]:
            img = post.select_one("img")
            link = post.select_one("a")
            title = post.select_one("h2,h3")
            if not (img and link):
                continue
            photo_url = img.get("src","")
            post_url = link.get("href","")
            title_text = title.get_text(strip=True) if title else ""
            if not title_text or "most wanted" not in title_text.lower():
                continue
            # Fetch individual post for names
            case_id = f"BPD-{post_url.split('/')[-2][:20]}"
            if not any(r["case_id"] == case_id for r in records):
                records.append({
                    "source":     "bpd",
                    "case_id":    case_id,
                    "name":       title_text,
                    "charges":    "Warrant — See BPD",
                    "address":    "Boston, MA",
                    "lat":        42.3601,
                    "lng":        -71.0589,
                    "photo_url":  photo_url,
                    "source_url": post_url,
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })
                print(f"  [BPD] {title_text}")

    except Exception as e:
        print(f"  [BPD] Error: {e}")

    print(f"[BPD] Found {len(records)} entries")
    return records

# ── Source 3: MSP Most Wanted ─────────────────────────────────────────────────
def scrape_msp_wanted():
    print("[MSP] Scraping mass.gov MSP Most Wanted...")
    records = []
    try:
        url = "https://www.mass.gov/info-details/state-polices-most-wanted"
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")

        # MSP page has h2 headings with names and adjacent images
        headings = soup.select("h2")
        for h in headings:
            name = h.get_text(strip=True)
            if not name or "do not attempt" in name.lower():
                continue

            # Find photo near this heading
            photo_url = ""
            img = h.find_next("img")
            if img:
                photo_url = img.get("src","")
                if photo_url.startswith("/"):
                    photo_url = "https://www.mass.gov" + photo_url

            # Find PDF link for more info
            pdf_link = h.find_next("a", href=re.compile(r"\.pdf$|/download$"))
            source_url = pdf_link.get("href","") if pdf_link else url

            case_id = f"MSP-{re.sub(r'[^a-z0-9]', '-', name.lower())[:30]}"
            records.append({
                "source":     "msp",
                "case_id":    case_id,
                "name":       name,
                "charges":    "Wanted by Massachusetts State Police",
                "address":    "Massachusetts",
                "lat":        42.4073,
                "lng":        -71.3824,
                "photo_url":  photo_url,
                "source_url": source_url or url,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            print(f"  [MSP] {name}")

    except Exception as e:
        print(f"  [MSP] Error: {e}")

    print(f"[MSP] Found {len(records)} fugitives")
    return records

# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    print("╔══════════════════════════════════════════╗")
    print("║  Hood Brief Boston — Fugitive Scraper    ║")
    print("║  MMW · BPD · MSP                         ║")
    print("╚══════════════════════════════════════════╝")

    all_records = []
    all_records += scrape_massmostwanted()
    all_records += scrape_bpd_wanted()
    all_records += scrape_msp_wanted()

    print(f"\nTotal: {len(all_records)} fugitive records")

    if all_records:
        save_fugitives(all_records)
        print(f"✅ Saved {len(all_records)} fugitives to Supabase")

if __name__ == "__main__":
    run()
