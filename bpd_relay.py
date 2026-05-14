"""
Hood Brief Boston — BPD Oracle VM Relay
Accumulates TX audio across multiple transmissions until 5+ seconds,
then sends combined WAV to Railway for Whisper transcription.
"""
import os, time, threading, json, struct, subprocess, tempfile, random, wave, io
import websocket, requests

CHANNELS = {
    "bpd_ch1": ("wss://radio.rapidsos.com/bff/ws/67b4a918f07fc8198abc4299", "BPD CH1 Special Event"),
    "bpd_ch2": ("wss://radio.rapidsos.com/bff/ws/67b4a94bf07fc8198abc429a", "BPD CH2 Area A"),
    "bpd_ch3": ("wss://radio.rapidsos.com/bff/ws/67ffbacc750036ae85e7c395", "BPD CH3 Roxbury/Mattapan"),
    "bpd_ch4": ("wss://radio.rapidsos.com/bff/ws/67ffbaf8cd281dea86c07f70", "BPD CH4 West Roxbury"),
    "bpd_ch5": ("wss://radio.rapidsos.com/bff/ws/67ffbb16cd281dea86c07f71", "BPD CH5 Back Bay"),
    "bpd_ch6": ("wss://radio.rapidsos.com/bff/ws/67ffbb30f14dc59c46716089", "BPD CH6 South Boston/Dorchester"),
}

RAILWAY_URL    = os.environ.get("RAILWAY_RELAY_URL", "")
RELAY_SECRET   = os.environ.get("RELAY_SECRET", "hoodbrief")
MIN_FRAMES     = 20   # minimum frames per TX to keep (~200ms)
TARGET_SECS    = 30.0  # accumulate 30 seconds of actual speech (~5 min real time)
FLUSH_TIMEOUT  = 300  # force send after 5 minutes regardless

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

def frames_to_pcm(frames):
    """Convert Opus frames to raw PCM bytes via OGG+ffmpeg."""
    if not frames:
        return b""
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
        granule += 480  # 10ms at 48kHz
        pages.append(make_ogg_page(f, serial, seq, granule, 0)); seq += 1
    ogg_data = b"".join(pages)
    tmp_ogg = tempfile.mktemp(suffix=".ogg")
    tmp_pcm = tempfile.mktemp(suffix=".raw")
    try:
        with open(tmp_ogg, "wb") as f:
            f.write(ogg_data)
        result = subprocess.run(
            ["ffmpeg", "-y", "-i", tmp_ogg,
             "-ar", "16000", "-ac", "1", "-f", "s16le", tmp_pcm],
            capture_output=True, timeout=15
        )
        if result.returncode == 0 and os.path.exists(tmp_pcm):
            with open(tmp_pcm, "rb") as f:
                return f.read()
        return b""
    except Exception as e:
        print(f"  frames_to_pcm error: {e}")
        return b""
    finally:
        for p in [tmp_ogg, tmp_pcm]:
            try: os.unlink(p)
            except: pass

def pcm_to_wav(pcm_data, sample_rate=16000):
    """Wrap raw PCM in a WAV container."""
    buf = io.BytesIO()
    with wave.open(buf, 'wb') as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)  # 16-bit
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_data)
    return buf.getvalue()

def send_wav(pcm_data, channel_key, label, total_frames):
    """Send accumulated PCM as WAV to Railway."""
    if not pcm_data or not RAILWAY_URL:
        return
    wav = pcm_to_wav(pcm_data)
    dur_secs = len(pcm_data) / 2 / 16000
    # Save sample for inspection
    try:
        with open("/tmp/bpd_sample.wav", "wb") as f:
            f.write(wav)
    except: pass
    try:
        r = requests.post(RAILWAY_URL, data=wav, timeout=20, headers={
            "Content-Type":    "audio/wav",
            "X-Relay-Secret":  RELAY_SECRET,
            "X-Channel":       channel_key,
            "X-Channel-Label": label,
        })
        print(f"  [{label}] Sent {len(wav):,} bytes WAV ({dur_secs:.1f}s speech) -> HTTP {r.status_code}")
    except Exception as e:
        print(f"  [{label}] relay error: {e}")

def run_channel(channel_key, url, label):
    print(f"[{label}] Starting...")
    while True:
        current_tx   = []       # frames in current transmission
        accumulated_pcm = b""   # concatenated PCM from multiple TXs
        accumulated_frames = 0
        transmitting = False
        last_flush   = time.time()

        def flush():
            nonlocal accumulated_pcm, accumulated_frames, last_flush
            if accumulated_pcm:
                send_wav(accumulated_pcm, channel_key, label, accumulated_frames)
            accumulated_pcm = b""
            accumulated_frames = 0
            last_flush = time.time()

        def on_msg(ws, msg):
            nonlocal transmitting, current_tx, accumulated_pcm, accumulated_frames, last_flush
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
                        n = len(frames)
                        print(f"  [{label}] TX end — {n} frames ({n*10}ms)")
                        if n >= MIN_FRAMES:
                            # Decode this TX to PCM and accumulate
                            pcm = frames_to_pcm(frames)
                            if pcm:
                                accumulated_pcm += pcm
                                accumulated_frames += n
                                speech_secs = len(accumulated_pcm) / 2 / 16000
                                print(f"  [{label}] Accumulated {speech_secs:.1f}s speech")
                                # Send when we have enough speech
                                if speech_secs >= TARGET_SECS:
                                    flush()
                except: pass


        def on_open(ws):
            print(f"[{label}] Connected")
            def hb():
                while True:
                    try: ws.send(json.dumps({"action": "heartbeat"}))
                    except: break
                    time.sleep(10)
            threading.Thread(target=hb, daemon=True).start()
            def timer():
                while True:
                    time.sleep(FLUSH_TIMEOUT)
                    if accumulated_pcm:
                        print(f"  [{label}] Flush timeout — sending {len(accumulated_pcm)/2/16000:.1f}s")
                        flush()
            threading.Thread(target=timer, daemon=True).start()

        def on_err(ws, e): print(f"[{label}] Error: {e}")
        def on_close(ws, c, m):
            print(f"[{label}] Closed — flushing and reconnecting")
            flush()

        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_msg,
            on_error=on_err, on_close=on_close)
        ws.run_forever()
        time.sleep(2)

def run():
    print("Hood Brief BPD Relay — Accumulated TX mode")
    print(f"Target: {RAILWAY_URL or 'NOT SET'}")
    print(f"Target speech: {TARGET_SECS}s | Min frames: {MIN_FRAMES} | Flush timeout: {FLUSH_TIMEOUT}s")
    threads = []
    for key, (url, label) in CHANNELS.items():
        t = threading.Thread(target=run_channel, args=(key, url, label), daemon=True, name=key)
        t.start(); threads.append(t)
        time.sleep(0.5)
    print("All channels running.")
    for t in threads: t.join()

if __name__ == "__main__":
    run()
