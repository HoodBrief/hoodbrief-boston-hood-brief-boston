"""
Hood Brief Boston — BPD Oracle VM Relay
Uses ffmpeg to decode Opus frames directly to WAV on the VM,
bypassing the broken OGG wrapping approach entirely.
Sends clean WAV audio to Railway for Whisper transcription.
"""
import os, time, threading, json, struct, subprocess, tempfile
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

def frames_to_wav(frames, label):
    """Convert raw Opus frames to WAV using ffmpeg on the VM.
    Writes frames as raw data, uses ffmpeg with libopus to decode."""
    if not frames:
        return None

    # Write raw concatenated frames to temp file
    tmp_opus = tempfile.mktemp(suffix=".opus")
    tmp_wav  = tempfile.mktemp(suffix=".wav")

    try:
        with open(tmp_opus, "wb") as f:
            for frame in frames:
                f.write(frame)

        # Try decoding as raw Opus with ffmpeg
        # -f opus forces Opus container format detection
        result = subprocess.run([
            "ffmpeg", "-y",
            "-f", "opus",
            "-i", tmp_opus,
            "-ar", "16000",
            "-ac", "1",
            "-f", "wav",
            tmp_wav
        ], capture_output=True, timeout=15)

        if result.returncode != 0 or not os.path.exists(tmp_wav):
            # Try with OGG container
            tmp_ogg = tempfile.mktemp(suffix=".ogg")
            # Write simple OGG using ffmpeg's concat
            result2 = subprocess.run([
                "ffmpeg", "-y",
                "-f", "data",
                "-i", tmp_opus,
                "-ar", "16000",
                "-ac", "1",
                tmp_wav
            ], capture_output=True, timeout=15)

            if result2.returncode != 0:
                print(f"  [{label}] ffmpeg decode failed")
                print(f"  [{label}] stderr: {result.stderr[-100:]}")
                return None

        if os.path.exists(tmp_wav) and os.path.getsize(tmp_wav) > 1000:
            with open(tmp_wav, "rb") as f:
                wav_data = f.read()
            print(f"  [{label}] WAV: {len(wav_data):,} bytes")
            return wav_data
        return None

    except Exception as e:
        print(f"  [{label}] frames_to_wav error: {e}")
        return None
    finally:
        for p in [tmp_opus, tmp_wav]:
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
                # Strip 12-byte RapidSOS header: [0-1]=type [2-5]=reserved [6-9]=ts [10-11]=seq
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
                        print(f"  [{label}] TX end — {len(accumulated)} total frames")
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
            print(f"[{label}] Sending {total} frames ({CHUNK_SECS}s window)")
            if total > 2:
                relay(accumulated, channel_key, label)
            else:
                print(f"  [{label}] Too few frames — skipping")

        ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_msg,
            on_error=on_err, on_close=on_close)
        ws.run_forever()
        time.sleep(1)

def run():
    print("Hood Brief BPD Relay — 6 Channels + ffmpeg WAV encoding")
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
