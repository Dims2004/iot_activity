"""
data_collection.py — Kumpulkan data training per label aktivitas (15 menit/sesi)

Cara pakai:
    python src/data_collection.py --label DUDUK    --duration 900
    python src/data_collection.py --label BERJALAN --duration 900
    python src/data_collection.py --label BERLARI  --duration 900

Setiap sesi menyimpan ke:
    data/raw/raw_DUDUK_20260407_143022.csv
    dataset/dataset.csv  (append, kolom 'activity' diisi dari --label)
"""
import argparse, csv, json, os, signal, sys, time
from datetime import datetime
from threading import Event

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, CLASSES,
    DATA_RAW_DIR, DATASET_PATH, SESSION_DURATION_SEC
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("data_collection")

collected_rows: list[dict] = []
stop_event = Event()
current_label = ""
start_time    = 0.0
duration_sec  = 0

FIELDNAMES = [
    "received_at", "device_id", "participant_id",
    "accel_stddev", "gyro_stddev", "bpm",
    "activity",     # ← kolom label untuk KNN
    "local_act", "timestamp"
]

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
    else:
        logger.error(f"Gagal connect, rc={rc}")

def on_message(client, userdata, msg):
    global collected_rows, start_time, duration_sec

    elapsed = time.time() - start_time
    if duration_sec > 0 and elapsed >= duration_sec:
        stop_event.set(); return

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        return

    row = parse_sensor_payload(payload)
    if row is None: return

    row["activity"]       = current_label      # label manual dari CLI
    row["participant_id"] = payload.get("participant_id",
                              payload.get("user", "unknown"))

    collected_rows.append(row)
    count     = len(collected_rows)
    remaining = max(0, duration_sec - elapsed)

    if count % 10 == 0:
        logger.info(
            f"[{current_label}] n={count} | "
            f"aStd={row['accel_stddev']:.4f} | "
            f"gStd={row['gyro_stddev']:.2f} | "
            f"BPM={row['bpm']} | sisa={remaining:.0f}s"
        )

def save_raw_session(label: str) -> str:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"raw_{label}_{ts}.csv"
    path     = os.path.join(DATA_RAW_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader(); writer.writerows(collected_rows)
    logger.info(f"Raw disimpan → {path} ({len(collected_rows)} baris)")
    return path

def append_to_dataset():
    """Tambahkan ke dataset gabungan. Hanya baris dengan BPM valid yang masuk."""
    file_exists = os.path.isfile(DATASET_PATH)
    added = 0
    with open(DATASET_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if not file_exists: writer.writeheader()
        for row in collected_rows:
            # Simpan semua baris (BPM=0 pun disimpan, imputasi dilakukan di notebook)
            writer.writerow(row); added += 1
    logger.info(f"Dataset +{added} baris → {DATASET_PATH}")

def main():
    global current_label, start_time, duration_sec

    parser = argparse.ArgumentParser(
        description="Kumpulkan data training per label aktivitas (15 menit)."
    )
    parser.add_argument("--label", "-l", required=True, choices=CLASSES,
                        help=f"Label aktivitas: {CLASSES}")
    parser.add_argument("--duration", "-d", type=int, default=SESSION_DURATION_SEC,
                        help=f"Durasi (detik). Default={SESSION_DURATION_SEC} (15 menit).")
    parser.add_argument("--no-append", action="store_true",
                        help="Jangan tambahkan ke dataset gabungan.")
    args = parser.parse_args()

    current_label = args.label
    duration_sec  = args.duration

    logger.info("=" * 55)
    logger.info(f"  SESI LABELING DATA")
    logger.info(f"  Label    : {current_label}")
    logger.info(f"  Durasi   : {duration_sec} detik ({duration_sec//60} menit)")
    logger.info(f"  Broker   : {MQTT_BROKER}:{MQTT_PORT}")
    logger.info("=" * 55)
    logger.info("Lakukan aktivitas sesuai label setelah terhubung...")

    client = mqtt.Client(client_id=f"{MQTT_CLIENT_ID}_collector")
    client.on_connect = on_connect
    client.on_message = on_message

    def _stop(sig, frame):
        logger.info("Dihentikan manual (Ctrl+C).")
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"Tidak bisa konek: {e}"); sys.exit(1)

    client.loop_start()
    start_time = time.time()

    while not stop_event.is_set():
        elapsed   = time.time() - start_time
        remaining = max(0, duration_sec - elapsed)
        bpm_valid = sum(1 for r in collected_rows if r.get("bpm",0) > 0)
        print(
            f"\r  [{current_label}] Sisa: {remaining:5.1f}s "
            f"| Sampel: {len(collected_rows):4d} "
            f"| BPM valid: {bpm_valid:4d}",
            end="", flush=True
        )
        if duration_sec > 0 and elapsed >= duration_sec:
            break
        time.sleep(0.5)

    print()
    client.loop_stop()
    client.disconnect()

    if not collected_rows:
        logger.warning("Tidak ada data. Periksa koneksi ESP32.")
        sys.exit(1)

    save_raw_session(current_label)
    if not args.no_append:
        append_to_dataset()

    bpm_nonzero = sum(1 for r in collected_rows if r.get("bpm",0) > 0)
    logger.info(f"Selesai! Total: {len(collected_rows)} sampel | BPM valid: {bpm_nonzero}")

if __name__ == "__main__":
    main()