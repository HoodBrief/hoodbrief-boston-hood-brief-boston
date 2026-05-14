"""
Hood Brief Boston — BPD Oracle VM Relay
Wraps Opus frames in OGG, decodes to WAV using ffmpeg on the VM,
sends clean WAV to Railway for Whisper transcription.
"""
import os, time, threading, json, struct, subprocess, tempfile, random
import websocket, requests

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

# OGG CRC table
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
    page = (b"OggS\x00" + bytes([flags]) +
            struct.pack("<q", granule) +
            struct.pack("<I", serial) +
            struct.pack("<I", seq) +
            b"\x00\x00\x00\x00" + lacing + payload)
    crc = ogg_crc(page)
    return page[:22] + struct.pack("<I", crc) + page[26:]

def frames_to_wav(frames, label):
    """Wrap Opus frames in OGG, decode to WAV with ffmpeg on the VM."""
    if not frames:
        return None

    serial = random.randint(1, 0xFFFFFF)
    pages = []; seq = 0

    # OpusHead - 48kHz mono
    id_hdr = b"OpusHead\x01\x01\x38\x01\x80\xbb\x00\x00\x00\x00\x00"
    pages.append(make_ogg_page(id_hdr, serial, seq, 0, 0x02)); seq += 1

    # OpusTags
    vendor = b"HoodBrief"
    com_hdr = b"OpusTags" + struct.pack("<I", len(vendor)) + vendor + struct.pack("<I", 0)
    pages.append(make_ogg_page(com_hdr, serial, seq, 0, 0)); seq += 1

    # Audio pages
    # Try to detect frame size from first frame's TOC byte
    # TOC byte bits 7-3 = config; configs 0-3=10ms, 4-7=20ms, 8-11=40ms, 12-13=60ms
    if frames[0]:
        toc = frames[0][0]
        config = toc >> 3
        if config <= 3:
            samples_per_frame = 480   # 10ms at 48kHz
        elif config <= 7:
            samples_per_frame = 960   # 20ms at 48kHz
        elif config <= 11:
            samples_per_frame = 1920  # 40ms at 48kHz
        else:
            samples_per_frame = 2880  # 60ms at 48kHz
    else:
        samples_per_frame = 960

    print(f"  [{label}] TOC config={frames[0][0] >> 3 if frames[0] else '?'} spf={samples_per_frame}")

    granule = 0
    for f in frames:
        if not f: continue
        granule += samples_per_frame
        pages.append(make_ogg_page(f, serial, seq, granule, 0)); seq += 1

    ogg_data = b"".join(pages)

    tmp_ogg = tempfile.mktemp(suffix=".ogg")
    tmp_wav = tempfile.mktemp(suffix=".wav")
    try:
        with open(tmp_ogg, "wb") as f:
            f.write(ogg_data)

        result = subprocess.run([
            "ffmpeg", "-y", "-i", tmp_ogg,
            "-ar", "16000", "-ac", "1",
            tmp_wav
        ], capture_output=True, timeout=15)

        if result.returncode == 0 and os.path.exists(tmp_wav) and os.path.getsize(tmp_wav) > 1000:
            with open(tmp_wav, "rb") as f:
                wav = f.read()
            print(f"  [{label}] WAV: {len(wav):,} bytes from {len(frames)} frames (granule={granule})")
            return wav
        else:
            print(f"  [{label}] ffmpeg error: {result.stderr[-100:]}")
            return None
    except Exception as e:
        print(f"  [{label}] error: {e}")
        return None
    finally:
        for p in [tmp_ogg, tmp_wav]:
            try: os.unlink(p)
            except: pass

def relay(frames, channel_key, label):
    wav = frames_to_wav(frames, label)
    if not wav or not RAILWAY_URL:
        return
    try:
        r = requests.post(RAILWAY_URL, data=wav, timeout=20, headers={
            "Content-Type":    "audio/wav",
            "X-Relay-Secret":  RELAY_SECRET,
            "X-Channel":       channel_key,
            "X-Channel-Label": label,
        })
        print(f"  [{label}] Sent {len(wav):,} bytes WAV -> HTTP {r.status_code}")
    except Exception as e:
        print(f"  [{label}] relay error: {e}")

def run_channel(channel_key, url, label):
    print(f"[{label}] Starting...")
    while True:
        accumulated = []
        chunk_start = time.time()

        def on_msg(ws, msg):
            if isinstance(msg, bytes) and msg:
                if len(msg) > 12 and msg[:2] == b'\x00\x01':
                    opus_frame = msg[12:]
                else:
                    opus_frame = msg
                if opus_frame:
                    accumulated.append(opus_frame)
            elif isinstance(msg, str):
                try:
                    m = json.loads(msg)
                    if m.get("action") == "tx_end":
                        print(f"  [{label}] TX end — {len(accumulated)} frames")
                except: pass
            if time.time() - chunk_start >= CHUNK_SECS:
                ws.close()

        def on_open(ws):
            print(f"[{label}] Connected")
            def hb():
                while True:
                    try: ws.send(json.dumps({"action": "heartbeat"}))
                    except: break
                    time.sleep(10)
            threading.Thread(target=hb, daemon=True).start()

        def on_err(ws, e): print(f"[{label}] Error: {e}")

        def on_close(ws, c, m):
            total = len(accumulated)
            print(f"[{label}] Window done — {total} frames")
            if total > 2:
                relay(accumulated, channel_key, label)
            else:
                print(f"  [{label}] Too few frames — skipping")

        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_msg,
            on_error=on_err, on_close=on_close)
        ws.run_forever()
        time.sleep(1)

def run():
    print("Hood Brief BPD Relay — 6 Channels + ffmpeg WAV")
    print(f"Target: {RAILWAY_URL or 'NOT SET'}")
    threads = []
    for key, (url, label) in CHANNELS.items():
        t = threading.Thread(target=run_channel, args=(key, url, label), daemon=True, name=key)
        t.start()
        threads.append(t)
        time.sleep(1)
    print("All channels running.")
    for t in threads: t.join()

if __name__ == "__main__":
    run()
