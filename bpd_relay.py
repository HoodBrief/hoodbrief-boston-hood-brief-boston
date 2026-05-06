"""
Hood Brief Boston — BPD Oracle VM Relay
Connects to RapidSOS WebSocket, collects 30s of audio,
wraps in OGG container, POSTs to Railway pipeline.
"""
import os, time, threading, json, struct, random
import websocket
import requests

CHANNEL_URL  = "wss://radio.rapidsos.com/bff/ws/689bb05ee75d9bc528e81c79"
COOKIE       = os.environ.get("RAPIDSOS_COOKIE", "")
RAILWAY_URL  = os.environ.get("RAILWAY_RELAY_URL", "")
RELAY_SECRET = os.environ.get("RELAY_SECRET", "hoodbrief")
CHUNK_SECS   = 30

def make_ogg_page(payload, serial, seq, granule=0, flags=0):
    """Create a minimal OGG page wrapping payload bytes."""
    magic     = b'OggS'
    version   = b'\x00'
    htype     = bytes([flags])
    granpos   = struct.pack('<q', granule)
    serialno  = struct.pack('<I', serial)
    seqno     = struct.pack('<I', seq)
    # Segment table
    segments  = []
    data      = payload
    while len(data) > 0:
        seg = data[:255]
        segments.append(len(seg))
        data = data[255:]
    lacing    = bytes([len(segments)] + segments)
    # CRC placeholder
    header    = magic + version + htype + granpos + serialno + seqno + b'\x00\x00\x00\x00' + lacing
    # Reconstruct payload in segments
    seg_data  = b''
    remaining = payload
    for s in segments:
        seg_data += remaining[:s]
        remaining = remaining[s:]
    page      = header + seg_data
    # Simple CRC32
    import binascii
    crc = binascii.crc32(page) & 0xFFFFFFFF
    # Insert CRC at offset 22
    page = page[:22] + struct.pack('<I', crc) + page[26:]
    return page

def wrap_in_ogg(opus_chunks):
    """
    Wrap raw Opus frames in an OGG container.
    Produces a valid .ogg file that ffmpeg/Whisper can read.
    """
    serial = random.randint(1, 0xFFFFFF)
    pages  = []
    seq    = 0

    # OGG Opus ID header (required first page)
    # Magic + version + channels + pre-skip + sample rate + gain + mapping
    id_header = (
        b'OpusHead'          # magic
        b'\x01'             # version
        b'\x01'             # channels = 1 (mono)
        b'\x38\x01'         # pre-skip = 312
        b'\x80\x3e\x00\x00' # input sample rate = 16000
        b'\x00\x00'         # output gain = 0
        b'\x00'             # channel mapping = 0 (mono)
    )
    pages.append(make_ogg_page(id_header, serial, seq, granule=0, flags=0x02))
    seq += 1

    # OGG Opus comment header (required second page)
    vendor    = b'HoodBrief'
    comment   = struct.pack('<I', len(vendor)) + vendor + struct.pack('<I', 0)
    com_header= b'OpusTags' + comment
    pages.append(make_ogg_page(com_header, serial, seq, granule=0, flags=0x00))
    seq += 1

    # Audio pages — each Opus frame as its own page
    granule = 0
    for frame in opus_chunks:
        if not frame:
            continue
        # Each Opus frame at 48kHz, ~20ms = 960 samples
        granule += 960
        pages.append(make_ogg_page(frame, serial, seq, granule=granule, flags=0x00))
        seq += 1

    return b''.join(pages)

def relay_chunk(chunks):
    """Wrap chunks in OGG and POST to Railway."""
    if not chunks:
        return
    ogg_data = wrap_in_ogg(chunks)
    total    = len(ogg_data)
    if not RAILWAY_URL:
        print(f"  [Relay] {total:,} bytes (RAILWAY_RELAY_URL not set)")
        return
    try:
        r = requests.post(
            RAILWAY_URL,
            data=ogg_data,
            headers={
                "Content-Type":    "audio/ogg",
                "X-Relay-Secret":  RELAY_SECRET,
                "X-Raw-Chunks":    str(len(chunks)),
            },
            timeout=20,
        )
        print(f"  [Relay] Sent {total:,} bytes OGG ({len(chunks)} frames) → HTTP {r.status_code}")
    except Exception as e:
        print(f"  [Relay] Error: {e}")

def run():
    print("Hood Brief — BPD Relay starting...")
    print(f"Target: {RAILWAY_URL or 'NOT SET'}")

    while True:
        chunks = []
        start  = time.time()
        headers = {"Origin": "https://radio.rapidsos.com"}
        if COOKIE:
            headers["Cookie"] = f"session={COOKIE}"

        def on_message(ws, message):
            if isinstance(message, bytes) and len(message) > 0:
                chunks.append(message)
            elif isinstance(message, str):
                try:
                    msg = json.loads(message)
                    if msg.get("action") == "tx_start":
                        print(f"  [BPD] TX start on ch {msg.get('channelId','')[:8]}")
                    elif msg.get("action") == "tx_end":
                        print(f"  [BPD] TX end — {len(chunks)} frames so far")
                except: pass
            if time.time() - start >= CHUNK_SECS:
                ws.close()

        def on_open(ws):
            print(f"[BPD] Connected — collecting {CHUNK_SECS}s...")
            def hb():
                while True:
                    try: ws.send(json.dumps({"action":"heartbeat"}))
                    except: break
                    time.sleep(10)
            threading.Thread(target=hb, daemon=True).start()

        def on_error(ws, error):
            print(f"[BPD] Error: {error}")

        def on_close(ws, code, msg):
            print(f"[BPD] Closed — {len(chunks)} frames")
            if len(chunks) > 5:
                relay_chunk(chunks)
            else:
                print("[BPD] Too few frames — skipping")

        ws = websocket.WebSocketApp(
            CHANNEL_URL,
            header=headers,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()
        time.sleep(2)

if __name__ == "__main__":
    run()
