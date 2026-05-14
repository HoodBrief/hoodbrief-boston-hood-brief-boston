"""
Hood Brief Boston — BPD Oracle VM Relay
Sends audio per-transmission (tx_start to tx_end) rather than 30s windows.
Skips transmissions under 20 frames (too short for Whisper).
Uses ffmpeg on the VM to decode OGG to WAV.
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
MIN_FRAMES   = 30  # ~300ms minimum — skip shorter bursts

# OGG helpers
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
    segs = []; data = payload
    while data:
        s = data[:255]; segs.append(len(s)); data = data[255:]
    lacing = bytes([len(segs)] + segs)
    page = (b"OggS\x00" + bytes([flags]) + struct.pack("<q", granule) +
            struct.pack("<I", serial) + struct.pack("<I", seq) +
            b"\x00\x00\x00\x00" + lacing + payload)
    crc = ogg_crc(page)
    return page[:22] + struct.pack("<I", crc) + page[26:]

def frames_to_wav(frames, label):
    if not frames:
        return None
    serial = random.randint(1, 0xFFFFFF)
    pages = []; seq = 0
    id_hdr = b"OpusHead\x01\x01\x38\x01\x80\xbb\x00\x00\x00\x00\x00"
    pages.append(make_ogg_page(id_hdr, serial, seq, 0, 0x02)); seq += 1
    vendor = b"HoodBrief"
    com_hdr = b"OpusTags" + struct.pack("<I", len(vendor)) + vendor + struct.pack("<I", 0)
    pages.append(make_ogg_page(com_hdr, serial, seq, 0, 0)); seq += 1
    granule = 0
    for f in frames:
        if not f: continue
        granule += 480  # 10ms at 48kHz (TOC config=0)
        pages.append(make_ogg_page(f, serial, seq, granule, 0)); seq += 1
    ogg_data = b"".join(pages)
    tmp_ogg = tempfile.mktemp(suffix=".ogg")
    tmp_wav = tempfile.mktemp(suffix=".wav")
    try:
        with open(tmp_ogg, "wb") as f:
            f.write(ogg_data)
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", tmp_ogg, "-ar", "16000", "-ac", "1", tmp_wav],
            capture_output=True, timeout=15
        )
        if result.returncode == 0 and os.path.exists(tmp_wav) and os.path.getsize(tmp_wav) > 1000:
            with open(tmp_wav, "rb") as f:
                return f.read()
        print(f"  [{label}] ffmpeg error: {result.stderr[-80:]}")
        return None
    except Exception as e:
        print(f"  [{label}] error: {e}")
        return None
    finally:
        try: os.unlink(tmp_ogg)
        except: pass
        # Keep latest WAV for inspection
        try:
            import shutil
            shutil.copy2(tmp_wav, '/tmp/bpd_sample.wav')
        except: pass
        try: os.unlink(tmp_wav)
        except: pass

def relay(frames, channel_key, label):
    if len(frames) < MIN_FRAMES:
        print(f"  [{label}] Too short ({len(frames)} frames) — skipping")
        return
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
        dur_ms = len(frames) * 10
        print(f"  [{label}] Sent {len(wav):,} bytes WAV ({dur_ms}ms) -> HTTP {r.status_code}")
    except Exception as e:
        print(f"  [{label}] relay error: {e}")

def run_channel(channel_key, url, label):
    print(f"[{label}] Starting...")
    while True:
        current_tx = []
        transmitting = False

        def on_msg(ws, msg):
            nonlocal transmitting, current_tx
            if isinstance(msg, bytes) and msg:
                if len(msg) > 12 and msg[:2] == b'\x00\x01':
                    opus_frame = msg[12:]
                else:
                    opus_frame = msg
                if transmitting and opus_frame:
                    current_tx.append(opus_frame)
            elif isinstance(msg, str):
                try:
                    m = json.loads(msg)
                    if m.get("action") == "tx_start":
                        current_tx = []
                        transmitting = True
                    elif m.get("action") == "tx_end":
                        transmitting = False
                        frames = list(current_tx)
                        current_tx = []
                        print(f"  [{label}] TX end — {len(frames)} frames ({len(frames)*10}ms)")
                        if len(frames) >= MIN_FRAMES:
                            threading.Thread(
                                target=relay,
                                args=(frames, channel_key, label),
                                daemon=True
                            ).start()
                except: pass

        def on_open(ws):
            print(f"[{label}] Connected")
            def hb():
                while True:
                    try: ws.send(json.dumps({"action": "heartbeat"}))
                    except: break
                    time.sleep(10)
            threading.Thread(target=hb, daemon=True).start()

        def on_err(ws, e): print(f"[{label}] Error: {e}")
        def on_close(ws, c, m): print(f"[{label}] Closed — reconnecting")

        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_msg,
            on_error=on_err, on_close=on_close)
        ws.run_forever()
        time.sleep(2)

def run():
    print("Hood Brief BPD Relay — Per-TX mode")
    print(f"Target: {RAILWAY_URL or 'NOT SET'}")
    print(f"Min frames: {MIN_FRAMES} ({MIN_FRAMES*10}ms)")
    threads = []
    for key, (url, label) in CHANNELS.items():
        t = threading.Thread(target=run_channel, args=(key, url, label), daemon=True, name=key)
        t.start(); threads.append(t)
        time.sleep(0.5)
    print("All channels running.")
    for t in threads: t.join()

if __name__ == "__main__":
    run()
