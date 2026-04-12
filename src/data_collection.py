"""
data_collection.py — Kumpulkan data training per label aktivitas (15 menit/sesi)
"""
import argparse, csv, json, os, signal, sys, time
from datetime import datetime
from threading import Event

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, TOPIC_COMMAND,
    CLASSES,
    DATA_RAW_DIR, DATASET_PATH, SESSION_DURATION_SEC
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("data_collection")

collected_rows: list[dict] = []
stop_event    = Event()
current_label = ""
start_time    = 0.0
duration_sec  = 0

FIELDNAMES = [
    "received_at", "device_id", "participant_id",
    "accel_stddev", "gyro_stddev", "bpm",
    "activity", "local_act", "timestamp"
]

# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"Terhubung ke cloud broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        print(f"\n  ✅ MQTT terhubung ke EMQX Cloud!")
        print(f"     Broker: {MQTT_BROKER}:{MQTT_PORT}")
        print(f"     Topic Command: {TOPIC_COMMAND}\n")
    else:
        logger.error(f"Gagal connect MQTT, rc={rc}")

def on_message(client, userdata, msg):
    global collected_rows, start_time, duration_sec

    elapsed = time.time() - start_time
    if duration_sec > 0 and elapsed >= duration_sec:
        stop_event.set()
        return

    if msg.topic != TOPIC_SENSOR_DATA:
        return

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        return

    row = parse_sensor_payload(payload)
    if row is None:
        return

    row["activity"]       = current_label
    row["participant_id"] = payload.get("participant_id", payload.get("user", "unknown"))
    collected_rows.append(row)

    count = len(collected_rows)
    if count % 10 == 0:
        remaining = max(0, duration_sec - elapsed)
        logger.info(f"[{current_label}] n={count} | aStd={row['accel_stddev']:.4f} | "
                   f"gStd={row['gyro_stddev']:.2f} | BPM={row['bpm']}")

# ─────────────────────────────────────────────
#  KIRIM PERINTAH KE ESP32
# ─────────────────────────────────────────────
def send_start(client, label: str, duration: int):
    payload = json.dumps({
        "cmd": "START",
        "participant_id": f"data_collection_{label}",
        "participant_no": 0,
        "total": 1
    })
    
    try:
        result = client.publish(TOPIC_COMMAND, payload, qos=1)
        result.wait_for_publish(timeout=3)
        logger.info(f"START terkirim ke {TOPIC_COMMAND} untuk label: {label}")
        print(f"  📤 Payload: {payload}")
        return True
    except Exception as e:
        logger.error(f"Gagal kirim START: {e}")
        return False

def send_stop(client):
    payload = json.dumps({
        "cmd": "STOP",
        "participant_no": 0
    })
    try:
        result = client.publish(TOPIC_COMMAND, payload, qos=1)
        result.wait_for_publish(timeout=3)
        logger.info(f"STOP terkirim ke {TOPIC_COMMAND}")
        return True
    except Exception as e:
        logger.error(f"Gagal kirim STOP: {e}")
        return False

# ─────────────────────────────────────────────
#  SIMPAN DATA
# ─────────────────────────────────────────────
def save_raw_session(label: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW_DIR, f"raw_{label}_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(collected_rows)
    logger.info(f"Raw disimpan → {path} ({len(collected_rows)} baris)")
    return path

def append_to_dataset():
    file_exists = os.path.isfile(DATASET_PATH)
    added = 0
    with open(DATASET_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for row in collected_rows:
            writer.writerow(row)
            added += 1
    logger.info(f"Dataset +{added} baris → {DATASET_PATH}")

# ─────────────────────────────────────────────
#  MODE INTERAKTIF
# ─────────────────────────────────────────────
def interactive_menu() -> tuple[str, int]:
    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║     AIoT Watch — Pengambilan Data Training       ║")
    print("╚══════════════════════════════════════════════════╝")
    print(f"\n  Broker Cloud: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Topic Command: {TOPIC_COMMAND}")
    print(f"  Output      : {DATA_RAW_DIR}/")
    print()
    print("  Pilih label aktivitas:")
    print("    1. DUDUK")
    print("    2. BERJALAN")
    print("    3. BERLARI")
    print()

    while True:
        pilih = input("  Masukkan pilihan (1/2/3): ").strip().upper()
        if pilih in ("1", "DUDUK"):    label = "DUDUK"; break
        if pilih in ("2", "BERJALAN"): label = "BERJALAN"; break
        if pilih in ("3", "BERLARI"):  label = "BERLARI"; break
        print("  ⚠ Pilihan tidak valid.")

    print()
    print("  Durasi pengambilan data:")
    print("    1. 15 menit (900 detik) ← default")
    print("    2. 10 menit (600 detik)")
    print("    3. 5 menit  (300 detik)")
    print("    4. Masukkan sendiri")

    while True:
        pilih_dur = input("  Pilih durasi [default=1]: ").strip()
        if pilih_dur in ("", "1"): duration = SESSION_DURATION_SEC; break
        if pilih_dur == "2":       duration = 600; break
        if pilih_dur == "3":       duration = 300; break
        if pilih_dur == "4":
            try:
                duration = int(input("  Durasi (detik): "))
                if duration > 0: break
            except ValueError:
                pass
            print("  ⚠ Masukkan angka positif.")
            continue
        print("  ⚠ Pilihan tidak valid.")

    return label, duration

# ─────────────────────────────────────────────
#  STATISTIK AKHIR
# ─────────────────────────────────────────────
def print_summary(label: str, duration: int, no_append: bool):
    total = len(collected_rows)
    bpm_valid = sum(1 for r in collected_rows if r.get("bpm", 0) > 0)
    bpm_pct = round(bpm_valid / total * 100, 1) if total > 0 else 0

    if total > 0:
        avg_accel = sum(r["accel_stddev"] for r in collected_rows) / total
        avg_gyro = sum(r["gyro_stddev"] for r in collected_rows) / total
        avg_bpm = (sum(r["bpm"] for r in collected_rows if r["bpm"] > 0) / bpm_valid if bpm_valid > 0 else 0)
    else:
        avg_accel = avg_gyro = avg_bpm = 0

    print()
    print("  ══════════════════════════════════════════════")
    print(f"  RINGKASAN SESI [{label}]")
    print("  ══════════════════════════════════════════════")
    print(f"  Total sampel  : {total}")
    print(f"  BPM valid     : {bpm_valid} ({bpm_pct}%)")
    print(f"  Rata accel    : {avg_accel:.4f} g")
    print(f"  Rata gyro     : {avg_gyro:.2f} °/s")
    print(f"  Rata BPM      : {avg_bpm:.0f} bpm")

    if total == 0:
        print("\n  ❌ Tidak ada data — periksa:")
        print("     • ESP32 menyala dan terhubung WiFi?")
        print("     • ESP32 terhubung ke broker cloud?")
        print(f"     • Broker: {MQTT_BROKER}:{MQTT_PORT}")
        print("     • Cek serial monitor ESP32")

# ─────────────────────────────────────────────
#  COUNTDOWN DISPLAY
# ─────────────────────────────────────────────
def show_countdown():
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        remaining = max(0, duration_sec - elapsed)
        m, s = int(remaining // 60), int(remaining % 60)
        print(f"\r  [{current_label}] Sisa: {m:02d}:{s:02d} | "
              f"Sampel: {len(collected_rows):4d}",
              end="", flush=True)
        
        if duration_sec > 0 and elapsed >= duration_sec:
            stop_event.set()
            break
        time.sleep(0.5)
    print()

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    global current_label, start_time, duration_sec

    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--label", "-l", choices=CLASSES, default=None)
    parser.add_argument("--duration", "-d", type=int, default=None)
    parser.add_argument("--no-append", action="store_true")
    args = parser.parse_args()

    if args.label is None:
        label, duration = interactive_menu()
    else:
        label = args.label
        duration = args.duration if args.duration else SESSION_DURATION_SEC

    current_label = label
    duration_sec = duration

    print()
    print("  ┌──────────────────────────────────────────────────┐")
    print(f"  │ Label        : {label:<33}│")
    print(f"  │ Durasi       : {duration} detik ({duration//60} menit){(17-len(str(duration)))*' '}│")
    print(f"  │ Broker       : {MQTT_BROKER}:{MQTT_PORT:<25}│")
    print(f"  │ Topic Command: {TOPIC_COMMAND:<25}│")
    print("  └──────────────────────────────────────────────────┘")
    print()
    input("  Tekan ENTER untuk mulai... ")

    # Setup MQTT
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    def _stop(sig, frame):
        print("\n\n  ⏹️ Dihentikan manual.")
        send_stop(client)
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)

    print(f"\n  Menghubungkan ke {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"\n  ❌ Tidak bisa konek: {e}")
        sys.exit(1)

    client.loop_start()
    time.sleep(2)

    print(f"\n  📤 Mengirim START ke topic '{TOPIC_COMMAND}'...")
    send_start(client, label, duration)
    time.sleep(1)

    start_time = time.time()
    print(f"\n  🟢 Merekam [{label}] selama {duration} detik...\n")
    show_countdown()

    print(f"\n  📤 Mengirim STOP ke topic '{TOPIC_COMMAND}'...")
    send_stop(client)
    time.sleep(1)

    client.loop_stop()
    client.disconnect()

    if collected_rows:
        save_raw_session(label)
        if not args.no_append:
            append_to_dataset()
    else:
        logger.warning("Tidak ada data terkumpul.")

    print_summary(label, duration, args.no_append)

if __name__ == "__main__":
    main()