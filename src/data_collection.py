"""
data_collection.py — Subscriber MQTT untuk mengumpulkan data training

Cara pakai:
    # Mode klasik (per-label)
    python src/data_collection.py --label DUDUK    --duration 120
    python src/data_collection.py --label BERJALAN --duration 120
    python src/data_collection.py --label BERLARI  --duration 120

    # Mode peserta (multi-participant)
    python src/data_collection.py --participant P001 --duration 120
    python src/data_collection.py --participant P002 --duration 120
    python src/data_collection.py --participant P003 --label BERLARI --duration 120

Data disimpan ke:
    data/raw/           → sesi per-label (mode klasik)
    data/raw_participant/ → sesi per-peserta (mode peserta)
    dataset/            → dataset gabungan (append setiap sesi)
    dataset_participant/ → dataset gabungan peserta (opsional)
"""

import argparse
import csv
import json
import os
import signal
import sys
import time
from datetime import datetime
from threading import Event

import paho.mqtt.client as mqtt

# Agar bisa import config & utils dari folder src
sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, CLASSES,
    DATA_RAW_DIR, DATASET_PATH
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("data_collection")

# ─────────────────────────────────────────────
#  KONFIGURASI TAMBAHAN UNTUK MODE PESERTA
# ─────────────────────────────────────────────
DATA_RAW_PARTICIPANT_DIR = os.path.join(os.path.dirname(DATA_RAW_DIR), "raw_participant")
DATASET_PARTICIPANT_PATH = os.path.join(os.path.dirname(DATASET_PATH), "dataset_participant.csv")

# Pastikan direktori ada
os.makedirs(DATA_RAW_PARTICIPANT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
#  STATE KOLEKSI
# ─────────────────────────────────────────────
collected_rows: list[dict] = []
stop_event = Event()
current_label = ""
current_participant = ""
is_participant_mode = False
start_time    = 0.0
duration_sec  = 0


# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Terhubung ke broker MQTT {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        logger.info(f"Subscribe ke topic: {TOPIC_SENSOR_DATA}")
    else:
        logger.error(f"Gagal connect, rc={rc}")


def on_message(client, userdata, msg):
    global collected_rows, start_time, duration_sec

    # Cek apakah durasi sudah habis
    elapsed = time.time() - start_time
    if duration_sec > 0 and elapsed >= duration_sec:
        stop_event.set()
        return

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning("Payload bukan JSON yang valid, dilewati.")
        return

    row = parse_sensor_payload(payload)
    if row is None:
        logger.warning(f"Payload tidak valid: {payload}")
        return

    # Mode peserta: gunakan participant_id dari payload jika ada
    if is_participant_mode:
        # Jika payload punya participant_id, gunakan itu
        if "participant_id" in payload and payload["participant_id"]:
            row["participant_id"] = payload["participant_id"]
        else:
            row["participant_id"] = current_participant
        
        row["participant_no"] = payload.get("participant_no", 0)
        row["activity"] = payload.get("local_act", "")
        
        # Jika label ditentukan manual, override
        if current_label:
            row["activity"] = current_label
    else:
        # Mode klasik: tambahkan label manual
        row["activity"] = current_label

    collected_rows.append(row)
    count = len(collected_rows)
    remaining = max(0, duration_sec - elapsed)

    # Progress log tiap 10 sampel
    if count % 10 == 0:
        mode_str = f"[{current_participant}]" if is_participant_mode else f"[{current_label}]"
        logger.info(
            f"{mode_str} Sampel={count} | "
            f"aStd={row['accel_stddev']:.4f} | "
            f"gStd={row['gyro_stddev']:.2f} | "
            f"BPM={row['bpm']} | "
            f"Sisa={remaining:.0f}s"
        )


# ─────────────────────────────────────────────
#  SIMPAN DATA
# ─────────────────────────────────────────────
FIELDNAMES = [
    "received_at", "device_id", "user",
    "accel_stddev", "gyro_stddev", "bpm",
    "local_act", "activity", "timestamp"
]

# Fieldnames untuk mode peserta (tambahan participant info)
PARTICIPANT_FIELDNAMES = [
    "received_at", "device_id", "participant_id", "participant_no",
    "accel_stddev", "gyro_stddev", "bpm",
    "local_act", "activity", "timestamp"
]


def save_raw_session(label: str, participant_id: str = None) -> str:
    """Simpan sesi raw ke direktori yang sesuai."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if is_participant_mode and participant_id:
        # Mode peserta: simpan ke raw_participant/
        filename = f"{participant_id}_{label}_{ts}.csv" if label else f"{participant_id}_{ts}.csv"
        path = os.path.join(DATA_RAW_PARTICIPANT_DIR, filename)
        fieldnames = PARTICIPANT_FIELDNAMES
    else:
        # Mode klasik: simpan ke raw/
        filename = f"raw_{label}_{ts}.csv"
        path = os.path.join(DATA_RAW_DIR, filename)
        fieldnames = FIELDNAMES

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(collected_rows)

    logger.info(f"Data sesi disimpan → {path}  ({len(collected_rows)} baris)")
    return path


def append_to_dataset(participant_id: str = None) -> None:
    """
    Tambahkan data sesi ini ke dataset gabungan.
    Mode peserta menggunakan dataset_participant.csv
    """
    if is_participant_mode and participant_id:
        dataset_path = DATASET_PARTICIPANT_PATH
        fieldnames = PARTICIPANT_FIELDNAMES
    else:
        dataset_path = DATASET_PATH
        fieldnames = FIELDNAMES

    file_exists = os.path.isfile(dataset_path)

    with open(dataset_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(collected_rows)

    logger.info(
        f"Dataset diperbarui → {dataset_path}  "
        f"(+{len(collected_rows)} baris baru)"
    )


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    global current_label, current_participant, start_time, duration_sec, is_participant_mode

    parser = argparse.ArgumentParser(
        description="Kumpulkan data training dari ESP32 via MQTT."
    )
    parser.add_argument(
        "--label", "-l",
        choices=CLASSES,
        help=f"Label aktivitas: {CLASSES} (wajib jika bukan mode peserta)"
    )
    parser.add_argument(
        "--participant", "-p",
        help="ID peserta (misal: P001). Aktifkan mode peserta."
    )
    parser.add_argument(
        "--duration", "-d",
        type=int,
        default=120,
        help="Durasi pengambilan data (detik). Default=120."
    )
    parser.add_argument(
        "--no-append",
        action="store_true",
        help="Jangan tambahkan ke dataset gabungan, simpan raw saja."
    )
    parser.add_argument(
        "--auto-label",
        action="store_true",
        help="Mode peserta: gunakan local_act dari ESP32 sebagai label (tidak perlu --label)"
    )
    args = parser.parse_args()

    # Tentukan mode
    if args.participant:
        is_participant_mode = True
        current_participant = args.participant
        
        if args.auto_label:
            current_label = ""  # akan diambil dari payload ESP32
            label_desc = "AUTO (dari ESP32)"
        elif args.label:
            current_label = args.label
            label_desc = current_label
        else:
            print("Error: Mode peserta membutuhkan --label ATAU --auto-label")
            print("  Contoh: --participant P001 --label DUDUK")
            print("  Contoh: --participant P001 --auto-label")
            sys.exit(1)
    else:
        is_participant_mode = False
        if not args.label:
            print("Error: Mode klasik membutuhkan --label")
            print("  Contoh: --label DUDUK --duration 120")
            sys.exit(1)
        current_label = args.label
        current_participant = ""
        label_desc = current_label

    duration_sec = args.duration

    logger.info("=" * 55)
    if is_participant_mode:
        logger.info(f"  SESI KOLEKSI DATA - MODE PESERTA")
        logger.info(f"  Peserta  : {current_participant}")
        logger.info(f"  Label    : {label_desc}")
    else:
        logger.info(f"  SESI KOLEKSI DATA - MODE KLASIK")
        logger.info(f"  Label    : {current_label}")
    logger.info(f"  Durasi   : {duration_sec} detik")
    logger.info(f"  Broker   : {MQTT_BROKER}:{MQTT_PORT}")
    logger.info("=" * 55)
    
    if is_participant_mode and args.auto_label:
        logger.info("Mode auto-label: aktivitas akan diambil dari local_act ESP32")
    logger.info("Mulai bergerak sesuai label setelah terhubung...")

    # Setup MQTT
    client = mqtt.Client(client_id=f"{MQTT_CLIENT_ID}_collector")
    client.on_connect = on_connect
    client.on_message = on_message

    # Graceful shutdown dengan Ctrl+C
    def _handle_sigint(sig, frame):
        logger.info("Ctrl+C terdeteksi, menghentikan koleksi...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"Tidak bisa konek ke broker: {e}")
        sys.exit(1)

    client.loop_start()
    start_time = time.time()

    # Tampilkan countdown
    while not stop_event.is_set():
        elapsed   = time.time() - start_time
        remaining = max(0, duration_sec - elapsed)
        
        if is_participant_mode:
            prefix = f"[{current_participant}]"
            label_display = f"label={current_label or 'AUTO'}"
        else:
            prefix = f"[{current_label}]"
            label_display = ""
        
        print(
            f"\r  {prefix} {label_display} Sisa: {remaining:5.1f}s "
            f"| Sampel: {len(collected_rows):4d}",
            end="", flush=True
        )
        
        if duration_sec > 0 and elapsed >= duration_sec:
            break
        time.sleep(0.5)

    print()  # newline setelah countdown
    client.loop_stop()
    client.disconnect()

    # Simpan hasil
    if not collected_rows:
        logger.warning("Tidak ada data yang terkumpul. Periksa koneksi ESP32 dan MQTT.")
        sys.exit(1)

    # Tentukan label untuk nama file (mode peserta)
    if is_participant_mode:
        if current_label:
            save_label = current_label
        else:
            # Ambil label mayoritas dari data yang terkumpul
            labels = [row.get("activity", "UNKNOWN") for row in collected_rows]
            if labels:
                save_label = max(set(labels), key=labels.count)
            else:
                save_label = "UNKNOWN"
    else:
        save_label = current_label

    save_raw_session(save_label, current_participant if is_participant_mode else None)

    if not args.no_append:
        append_to_dataset(current_participant if is_participant_mode else None)

    logger.info(f"Selesai! Total sampel terkumpul: {len(collected_rows)}")


if __name__ == "__main__":
    main()