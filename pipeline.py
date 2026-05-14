"""
Hood Brief Boston — Pipeline
CKAN open data sync only. No audio transcription.
Runs boston_ckan_updater on a daily schedule.
"""
import threading, time
from boston_ckan_updater import run as ckan_run

def run_ckan():
    try:
        ckan_run()
    except Exception as e:
        print(f"[CKAN] Fatal error: {e}")

print("╔══════════════════════════════════════════╗")
print("║  Hood Brief Boston — Data Pipeline       ║")
print("║  CKAN Daily Sync · Heatmap · Consulates  ║")
print("╚══════════════════════════════════════════╝")

t = threading.Thread(target=run_ckan, daemon=False)
t.start()
t.join()
