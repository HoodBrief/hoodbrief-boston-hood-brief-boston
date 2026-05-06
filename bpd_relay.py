"""
Hood Brief Boston — BPD Oracle VM Relay
Connects to RapidSOS WebSocket, collects 30s of Opus audio,
wraps in valid OGG container, POSTs to Railway pipeline.
"""
import os, time, threading, json, struct, random
import websocket, requests

CHANNEL_URL  = "wss://radio.rapidsos.com/bff/ws/689bb05ee75d9bc528e81c79"
COOKIE       = os.environ.get("RAPIDSOS_COOKIE", "")
RAILWAY_URL  = os.environ.get("RAILWAY_RELAY_URL", "")
RELAY_SECRET = os.environ.get("RELAY_SECRET", "hoodbrief")
CHUNK_SECS   = 30

# OGG CRC table (polynomial 0x04c11db7)
_CRC_TABLE = []
for _i in range(256):
    _r = _i << 24
    for _ in range(8):
        _r = ((_r << 1) ^ 0x04c11db7) & 0xFFFFFFFF if _r & 0x80000000 else (_r << 1) & 0xFFFFFFFF
    _CRC_TABLE.append(_r)

def ogg_crc(data):
    crc = 0
    for b in data:
        crc = ((crc << 8) ^ _CRC_TABLE[((crc >> 24) & 0xFF) ^ b]) & 0xFFFFFFFF
    return crc

def make_ogg_page(payload, serial, seq, granule=0, flags=0):
    segs = []
    data = payload
    while data:
        s = data[:255]; segs.append(len(s)); data = data[255:]
    lacing = bytes([len(segs)] + segs)
    page = (b'OggS\x00' + bytes([flags]) +
            struct.pack('<q', granule) +
            struct.pack('<I', serial) +
            struct.pack('<I', seq) +
            b'\x00\x00\x00\x00' + lacing + payload)
    crc = ogg_crc(page)
    return page[:22] + struct.pack('<I', crc) + page[26:]

def wrap_ogg(frames):
    serial = random.randint(1, 0xFFFFFF)
    pages  = []; seq = 0
    id_hdr = b'OpusHead\x01\x01\x38\x01\x80\x3e\x00\x00\x00\x00\x00'
    pages.append(make_ogg_page(id_hdr, serial, seq, 0, 0x02)); seq += 1
    vendor  = b'HoodBrief'
    com_hdr = b'OpusTags' + struct.pack('<I', len(vendor)) + vendor + struct.pack('<I', 0)
    pages.append(make_ogg_page(com_hdr, serial, seq, 0, 0)); seq += 1
    granule = 0
    for f in frames:
        if not f: continue
        granule += 960
        pages.append(make_ogg_page(f, serial, seq, granule, 0)); seq += 1
    return b''.join(pages)

def relay(frames):
    ogg = wrap_ogg(frames)
    if not RAILWAY_URL:
        print(f"  [Relay] {len(ogg):,} bytes OGG (no RAILWAY_RELAY_URL)")
        return
    try:
        r = requests.post(RAILWAY_URL, data=ogg, timeout=20, headers={
            "Content-Type":   "audio/ogg",
            "X-Relay-Secret": RELAY_SECRET,
        })
        print(f"  [Relay] Sent {len(ogg):,} bytes OGG -> HTTP {r.status_code}")
    except Exception as e:
        print(f"  [Relay] Error: {e}")

def run():
    print("Hood Brief BPD Relay (OGG/Opus)")
    print(f"Target: {RAILWAY_URL or 'NOT SET'}")
    while True:
        frames = []; start = time.time()
        hdrs = {"Origin": "https://radio.rapidsos.com"}
        if COOKIE:
            hdrs["Cookie"] = f"session={COOKIE}"

        def on_msg(ws, msg):
            if isinstance(msg, bytes) and msg:
                frames.append(msg)
            elif isinstance(msg, str):
                try:
                    m = json.loads(msg)
                    if m.get("action") == "tx_start": print("  [BPD] TX start")
                    elif m.get("action") == "tx_end": print(f"  [BPD] TX end — {len(frames)} frames")
                except: pass
            if time.time() - start >= CHUNK_SECS: ws.close()

        def on_open(ws):
            print("[BPD] Connected — collecting 30s...")
            def hb():
                while True:
                    try: ws.send(json.dumps({"action": "heartbeat"}))
                    except: break
                    time.sleep(10)
            threading.Thread(target=hb, daemon=True).start()

        def on_err(ws, e): print(f"[BPD] Error: {e}")

        def on_close(ws, c, m):
            print(f"[BPD] Closed — {len(frames)} frames")
            if len(frames) > 10: relay(frames)
            else: print("[BPD] Too few frames — skipping")

        ws = websocket.WebSocketApp(CHANNEL_URL, header=hdrs,
            on_open=on_open, on_message=on_msg,
            on_error=on_err, on_close=on_close)
        ws.run_forever()
        time.sleep(2)

if __name__ == "__main__":
    run()
