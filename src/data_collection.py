"""
data_collection.py — Kumpulkan data training per partisipan (15 menit/sesi)
Pelabelan aktivitas dilakukan secara MANUAL melalui input pengguna.
"""
import argparse, csv, json, os, signal, sys, time
from datetime import datetime
from threading import Event

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, TOPIC_COMMAND, TOPIC_STATUS,
    CLASSES,
    DATA_RAW_DIR, DATASET_PATH, SESSION_DURATION_SEC
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("data_collection")

collected_rows: list[dict] = []
stop_event    = Event()
current_label = ""
current_participant_id = ""
current_participant_no = 0
start_time    = 0.0
duration_sec  = 0

FIELDNAMES = [
    "received_at", "device_id", "participant_id", "participant_no",
    "accel_stddev", "gyro_stddev", "bpm",
    "activity", "timestamp"
]

# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"Terhubung ke cloud broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        client.subscribe(TOPIC_STATUS)
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

    if msg.topic == TOPIC_SENSOR_DATA:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            return

        row = parse_sensor_payload(payload)
        if row is None:
            return

        # Gunakan participant_id dari payload jika ada
        pid = payload.get("participant_id", current_participant_id)
        pno = payload.get("participant_no", current_participant_no)
        
        row["participant_id"] = pid
        row["participant_no"] = pno
        row["activity"] = current_label  # Label dari input manual
        
        collected_rows.append(row)

        count = len(collected_rows)
        if count % 10 == 0:
            remaining = max(0, duration_sec - elapsed)
            logger.info(f"[{current_label}] P{current_participant_no} n={count} | "
                       f"aStd={row['accel_stddev']:.4f} | "
                       f"gStd={row['gyro_stddev']:.2f} | BPM={row['bpm']}")
    
    elif msg.topic == TOPIC_STATUS:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            if payload.get("status") == "session_complete":
                logger.info(f"ESP32 melaporkan sesi selesai untuk P{payload.get('participant_no')}")
        except:
            pass

# ─────────────────────────────────────────────
#  KIRIM PERINTAH KE ESP32
# ─────────────────────────────────────────────
def send_start(client, participant_id: str, participant_no: int, duration: int):
    payload = json.dumps({
        "cmd": "START",
        "participant_id": participant_id,
        "participant_no": participant_no,
        "total": 999  # Tidak terbatas
    })
    
    try:
        result = client.publish(TOPIC_COMMAND, payload, qos=1)
        result.wait_for_publish(timeout=3)
        logger.info(f"START terkirim ke P{participant_no} [{participant_id}]")
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
def save_raw_session(participant_id: str, participant_no: int, label: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW_DIR, f"P{participant_no:03d}_{participant_id}_{label}_{ts}.csv")
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
#  INPUT DATA PARTISIPAN
# ─────────────────────────────────────────────
def input_participant_info(no: int) -> tuple[str, str]:
    print()
    print("┌──────────────────────────────────────────────────┐")
    print(f"│  👤 PARTISIPAN KE-{no}                              │")
    print("└──────────────────────────────────────────────────┘")
    
    while True:
        pid = input("  ID Partisipan (contoh: SUBJ_001): ").strip()
        if not pid:
            print("  ⚠ ID tidak boleh kosong.")
            continue
        if len(pid) > 30:
            print("  ⚠ Terlalu panjang (maks 30 karakter).")
            continue
        # Bersihkan karakter ilegal
        safe = pid.replace(" ", "_").replace(",", "").replace('"', "").replace("'", "")
        if safe != pid:
            print(f"  ℹ ID diubah: '{safe}'")
        return safe, safe

# ─────────────────────────────────────────────
#  PILIH AKTIVITAS
# ─────────────────────────────────────────────
def select_activity() -> str:
    print()
    print("  Pilih label aktivitas untuk sesi ini:")
    print("    1. DUDUK")
    print("    2. BERJALAN")
    print("    3. BERLARI")
    print()

    while True:
        pilih = input("  Masukkan pilihan (1/2/3): ").strip().upper()
        if pilih in ("1", "DUDUK"):    return "DUDUK"
        if pilih in ("2", "BERJALAN"): return "BERJALAN"
        if pilih in ("3", "BERLARI"):  return "BERLARI"
        print("  ⚠ Pilihan tidak valid.")

# ─────────────────────────────────────────────
#  PILIH DURASI
# ─────────────────────────────────────────────
def select_duration() -> int:
    print()
    print("  Durasi pengambilan data:")
    print("    1. 15 menit (900 detik) ← default")
    print("    2. 10 menit (600 detik)")
    print("    3. 5 menit  (300 detik)")
    print("    4. Masukkan sendiri")

    while True:
        pilih_dur = input("  Pilih durasi [default=1]: ").strip()
        if pilih_dur in ("", "1"): return SESSION_DURATION_SEC
        if pilih_dur == "2":       return 600
        if pilih_dur == "3":       return 300
        if pilih_dur == "4":
            try:
                dur = int(input("  Durasi (detik): "))
                if dur > 0: return dur
            except ValueError:
                pass
            print("  ⚠ Masukkan angka positif.")
            continue
        print("  ⚠ Pilihan tidak valid.")

# ─────────────────────────────────────────────
#  COUNTDOWN DISPLAY
# ─────────────────────────────────────────────
def show_countdown():
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        remaining = max(0, duration_sec - elapsed)
        m, s = int(remaining // 60), int(remaining % 60)
        print(f"\r  [{current_label}] P{current_participant_no} | "
              f"Sisa: {m:02d}:{s:02d} | "
              f"Sampel: {len(collected_rows):4d}",
              end="", flush=True)
        
        if duration_sec > 0 and elapsed >= duration_sec:
            stop_event.set()
            break
        time.sleep(0.5)
    print()

# ─────────────────────────────────────────────
#  STATISTIK AKHIR
# ─────────────────────────────────────────────
def print_summary(participant_id: str, participant_no: int, label: str, duration: int):
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
    print(f"  RINGKASAN SESI P{participant_no} [{participant_id}]")
    print(f"  Aktivitas    : {label}")
    print("  ══════════════════════════════════════════════")
    print(f"  Total sampel : {total}")
    print(f"  BPM valid    : {bpm_valid} ({bpm_pct}%)")
    print(f"  Rata accel   : {avg_accel:.4f} g")
    print(f"  Rata gyro    : {avg_gyro:.2f} °/s")
    print(f"  Rata BPM     : {avg_bpm:.0f} bpm")

    if total == 0:
        print("\n  ❌ Tidak ada data — periksa:")
        print("     • ESP32 menyala dan terhubung WiFi?")
        print("     • ESP32 terhubung ke broker cloud?")
        print(f"     • Broker: {MQTT_BROKER}:{MQTT_PORT}")

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    global current_label, current_participant_id, current_participant_no
    global start_time, duration_sec, collected_rows

    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--participant", "-p", type=str, help="ID Partisipan")
    parser.add_argument("--label", "-l", choices=CLASSES, help="Label aktivitas")
    parser.add_argument("--duration", "-d", type=int, help="Durasi dalam detik")
    parser.add_argument("--no-append", action="store_true")
    args = parser.parse_args()

    # Input partisipan
    if args.participant:
        participant_id = args.participant
        participant_no = 1  # Default jika tidak ada history
    else:
        participant_id, participant_no = input_participant_info(1)

    # Input aktivitas
    if args.label:
        label = args.label
    else:
        label = select_activity()

    # Input durasi
    if args.duration:
        duration = args.duration
    else:
        duration = select_duration()

    current_label = label
    current_participant_id = participant_id
    current_participant_no = participant_no
    duration_sec = duration
    collected_rows = []

    print()
    print("  ┌──────────────────────────────────────────────────┐")
    print(f"  │ Partisipan    : P{participant_no} [{participant_id:<20}]│")
    print(f"  │ Aktivitas     : {label:<33}│")
    print(f"  │ Durasi        : {duration} detik ({duration//60} menit){(17-len(str(duration)))*' '}│")
    print(f"  │ Broker        : {MQTT_BROKER}:{MQTT_PORT:<25}│")
    print(f"  │ Topic Command : {TOPIC_COMMAND:<25}│")
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
    send_start(client, participant_id, participant_no, duration)
    time.sleep(1)

    start_time = time.time()
    print(f"\n  🟢 Merekam [{label}] untuk P{participant_no} [{participant_id}] selama {duration} detik...\n")
    show_countdown()

    print(f"\n  📤 Mengirim STOP ke topic '{TOPIC_COMMAND}'...")
    send_stop(client)
    time.sleep(1)

    client.loop_stop()
    client.disconnect()

    if collected_rows:
        save_raw_session(participant_id, participant_no, label)
        if not args.no_append:
            append_to_dataset()
    else:
        logger.warning("Tidak ada data terkumpul.")

    print_summary(participant_id, participant_no, label, duration)

if __name__ == "__main__":
    main()