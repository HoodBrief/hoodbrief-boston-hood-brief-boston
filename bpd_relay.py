"""
Hood Brief Boston — BPD Oracle VM Relay
Connects to all 6 BPD channels individually via RapidSOS WebSocket.
No cookie required — Oracle VM IP is on RapidSOS allowlist.
Wraps Opus frames in valid OGG container, POSTs to Railway.
"""
import os, time, threading, json, struct, random
import websocket, requests

# All 6 BPD channels + scan
CHANNELS = {
    "bpd_ch1": ("wss://radio.rapidsos.com/bff/ws/67b4a918f07fc8198abc4299", "BPD CH1 Special Event"),
    "bpd_ch2": ("wss://radio.rapidsos.com/bff/ws/67b4a94bf07fc8198abc429a", "BPD CH2 Area A"),
    "bpd_ch3": ("wss://radio.rapidsos.com/bff/ws/67ffbacc750036ae85e7c395", "BPD CH3 Roxbury/Mattapan"),
    "bpd_ch4": ("wss://radio.rapidsos.com/bff/ws/67ffbaf8cd281dea86c07f70", "BPD CH4 West Roxbury"),
    "bpd_ch5": ("wss://radio.rapidsos.com/bff/ws/67ffbb16cd281dea86c07f71", "BPD CH5 Back Bay"),
    "bpd_ch6": ("wss://radio.rapidsos.com/bff/ws/67ffbb30f14dc59c46716089", "BPD CH6 South Boston/Dorchester"),
}

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
    pages = []; seq = 0
    # 48kHz OpusHead
    id_hdr = b'OpusHead\x01\x01\x38\x01\x80\xbb\x00\x00\x00\x00\x00'
    pages.append(make_ogg_page(id_hdr, serial, seq, 0, 0x02)); seq += 1
    vendor = b'HoodBrief'
    com_hdr = b'OpusTags' + struct.pack('<I', len(vendor)) + vendor + struct.pack('<I', 0)
    pages.append(make_ogg_page(com_hdr, serial, seq, 0, 0)); seq += 1
    granule = 0
    # Auto-detect samples per frame from total duration
    # 30 seconds at 48kHz = 1,440,000 total samples
    # Divide by number of frames to get samples per frame
    audio_frames = [f for f in frames if f]
    if not audio_frames:
        return b""
    # Use 9600 samples per frame (200ms) as RapidSOS default
    # This is 10x longer than standard Opus 20ms frames
    samples_per_frame = 9600
    for f in audio_frames:
        granule += samples_per_frame
        pages.append(make_ogg_page(f, serial, seq, granule, 0)); seq += 1
    return b''.join(pages) if len(pages) > 2 else b''

def relay(frames, channel_key, label):
    ogg = wrap_ogg(frames)
    if not RAILWAY_URL:
        print(f"  [{label}] {len(ogg):,} bytes (no RAILWAY_RELAY_URL)")
        return
    try:
        r = requests.post(RAILWAY_URL, data=ogg, timeout=20, headers={
            "Content-Type":   "audio/ogg",
            "X-Relay-Secret": RELAY_SECRET,
            "X-Channel":      channel_key,
            "X-Channel-Label": label,
        })
        print(f"  [{label}] Sent {len(ogg):,} bytes OGG -> HTTP {r.status_code}")
    except Exception as e:
        print(f"  [{label}] Error: {e}")

def run_channel(channel_key, url, label):
    """Run a single BPD channel relay loop."""
    print(f"[{label}] Starting...")
    while True:
        frames = []; start = time.time()

        def on_msg(ws, msg):
            if isinstance(msg, bytes) and msg:
                frames.append(msg)
            elif isinstance(msg, str):
                try:
                    m = json.loads(msg)
                    if m.get("action") == "tx_start":
                        print(f"  [{label}] TX start")
                    elif m.get("action") == "tx_end":
                        print(f"  [{label}] TX end — {len(frames)} frames")
                except: pass
            if time.time() - start >= CHUNK_SECS: ws.close()

        def on_open(ws):
            print(f"[{label}] Connected")
            def hb():
                while True:
                    try: ws.send(json.dumps({"action": "heartbeat"}))
                    except: break
                    time.sleep(10)
            threading.Thread(target=hb, daemon=True).start()

        def on_err(ws, e):
            print(f"[{label}] Error: {e}")

        def on_close(ws, c, m):
            print(f"[{label}] Closed — {len(frames)} frames")
            if len(frames) > 10:
                relay(frames, channel_key, label)
            else:
                print(f"  [{label}] Too few frames — skipping")

        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_msg,
            on_error=on_err,
            on_close=on_close,
        )
        ws.run_forever()
        time.sleep(2)

def run():
    print("Hood Brief BPD Relay — 6 Channels (No Cookie)")
    print(f"Target: {RAILWAY_URL or 'NOT SET'}")
    print(f"Channels: {len(CHANNELS)}")

    threads = []
    for key, (url, label) in CHANNELS.items():
        t = threading.Thread(target=run_channel, args=(key, url, label), daemon=True, name=key)
        t.start()
        threads.append(t)
        time.sleep(1)  # stagger starts

    print("All channels running.")
    for t in threads:
        t.join()

if __name__ == "__main__":
    run()
