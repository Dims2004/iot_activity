"""
data_collection.py — Kumpulkan data training per label aktivitas (15 menit/sesi)

Cara pakai:
    # Mode interaktif (dijalankan langsung tanpa argumen):
    python src/data_collection.py

    # Mode CLI (dengan argumen):
    python src/data_collection.py --label DUDUK
    python src/data_collection.py --label BERJALAN --duration 900
    python src/data_collection.py --label BERLARI  --duration 300

Output:
    data/raw/raw_DUDUK_20260407_143022.csv
    dataset/dataset.csv  (append)
"""
import argparse, csv, json, os, signal, sys, time
from datetime import datetime
from threading import Event

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, TOPIC_COMMAND,  # Tambahkan TOPIC_COMMAND
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
    "activity",    # ← label untuk KNN
    "local_act", "timestamp"
]

# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        print(f"\n  ✅ MQTT terhubung. Siap mengirim perintah ke ESP32.\n")
    else:
        logger.error(f"Gagal connect MQTT, rc={rc}")

def on_message(client, userdata, msg):
    global collected_rows, start_time, duration_sec

    elapsed = time.time() - start_time
    if duration_sec > 0 and elapsed >= duration_sec:
        stop_event.set()
        return

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except json.JSONDecodeError:
        return

    row = parse_sensor_payload(payload)
    if row is None:
        return

    row["activity"]       = current_label
    row["participant_id"] = payload.get("participant_id",
                              payload.get("user", "unknown"))
    collected_rows.append(row)

    count = len(collected_rows)
    if count % 10 == 0:
        remaining = max(0, duration_sec - elapsed)
        bpm_ok    = sum(1 for r in collected_rows if r.get("bpm", 0) > 0)
        logger.info(
            f"[{current_label}] n={count} | "
            f"aStd={row['accel_stddev']:.4f} | "
            f"gStd={row['gyro_stddev']:.2f} | "
            f"BPM={row['bpm']} | sisa={remaining:.0f}s"
        )

# ─────────────────────────────────────────────
#  KIRIM PERINTAH KE ESP32
# ─────────────────────────────────────────────
def send_command(client, command: str, label: str = "", duration: int = 0):
    """
    Kirim perintah ke ESP32 melalui MQTT.
    command: "START", "STOP", "STATUS"
    """
    cmd_payload = {
        "command": command,
        "label": label,
        "duration": duration,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        client.publish(TOPIC_COMMAND, json.dumps(cmd_payload))
        logger.info(f"Perintah terkirim: {command} - Label: {label}, Durasi: {duration}s")
        return True
    except Exception as e:
        logger.error(f"Gagal mengirim perintah: {e}")
        return False

# ─────────────────────────────────────────────
#  SIMPAN DATA
# ─────────────────────────────────────────────
def save_raw_session(label: str) -> str:
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
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
#  MODE INTERAKTIF — tampilkan menu pilihan
# ─────────────────────────────────────────────
def interactive_menu() -> tuple[str, int]:
    """
    Tampilkan menu pilihan jika script dijalankan tanpa argumen.
    Return (label, duration_detik).
    """
    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║     AIoT Watch — Pengambilan Data Training       ║")
    print("╚══════════════════════════════════════════════════╝")
    print(f"\n  Broker MQTT : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Output      : {DATA_RAW_DIR}/")
    print()
    print("  Pilih label aktivitas:")
    print("    1. DUDUK")
    print("    2. BERJALAN")
    print("    3. BERLARI")
    print()

    # Pilih label
    while True:
        pilih = input("  Masukkan pilihan (1/2/3) atau nama langsung: ").strip().upper()
        if pilih in ("1", "DUDUK"):    label = "DUDUK";    break
        if pilih in ("2", "BERJALAN"): label = "BERJALAN"; break
        if pilih in ("3", "BERLARI"):  label = "BERLARI";  break
        print("  ⚠  Pilihan tidak valid. Coba lagi.")

    # Pilih durasi
    print()
    print(f"  Durasi pengambilan data:")
    print(f"    1. 15 menit  (900 detik)  ← default")
    print(f"    2. 10 menit  (600 detik)")
    print(f"    3.  5 menit  (300 detik)  ← untuk uji coba")
    print(f"    4. Masukkan sendiri (detik)")
    print()

    while True:
        pilih_dur = input("  Pilih durasi (1/2/3/4) [default=1]: ").strip()
        if pilih_dur in ("", "1"): duration = SESSION_DURATION_SEC; break
        if pilih_dur == "2":       duration = 600;                  break
        if pilih_dur == "3":       duration = 300;                  break
        if pilih_dur == "4":
            try:
                duration = int(input("  Masukkan durasi (detik): ").strip())
                if duration > 0: break
            except ValueError:
                pass
            print("  ⚠  Masukkan angka positif.")
            continue
        print("  ⚠  Pilihan tidak valid.")

    return label, duration

# ─────────────────────────────────────────────
#  STATISTIK AKHIR
# ─────────────────────────────────────────────
def print_summary(label: str, duration: int, no_append: bool):
    total     = len(collected_rows)
    bpm_valid = sum(1 for r in collected_rows if r.get("bpm", 0) > 0)
    bpm_pct   = round(bpm_valid / total * 100, 1) if total > 0 else 0

    # Hitung rata-rata nilai sensor
    if total > 0:
        avg_accel = sum(r["accel_stddev"] for r in collected_rows) / total
        avg_gyro  = sum(r["gyro_stddev"]  for r in collected_rows) / total
        avg_bpm   = (sum(r["bpm"] for r in collected_rows if r["bpm"] > 0) / bpm_valid
                     if bpm_valid > 0 else 0)
    else:
        avg_accel = avg_gyro = avg_bpm = 0

    print()
    print("  ══════════════════════════════════════════════")
    print(f"  RINGKASAN SESI  [{label}]")
    print("  ══════════════════════════════════════════════")
    print(f"  Total sampel  : {total}")
    print(f"  BPM valid     : {bpm_valid} ({bpm_pct}%)")
    print(f"  Rata accel    : {avg_accel:.4f} g")
    print(f"  Rata gyro     : {avg_gyro:.2f} °/s")
    print(f"  Rata BPM      : {avg_bpm:.0f} bpm (dari yang valid)")
    print()

    if total == 0:
        print("  ❌ Tidak ada data — periksa:")
        print("     • ESP32 sudah menyala dan terkoneksi WiFi?")
        print("     • MQTT broker berjalan?")
        print(f"     • Broker: {MQTT_BROKER}:{MQTT_PORT}")
        return

    if not no_append:
        print(f"  ✅ Data sudah ditambahkan ke dataset/dataset.csv")
    print(f"  ✅ Raw CSV disimpan di data/raw/")
    print()
    print("  Langkah berikutnya:")
    remaining = [c for c in CLASSES if c != label]
    for cls in remaining:
        print(f"    python src/data_collection.py --label {cls}")
    print("  Atau jalankan tanpa argumen untuk menu interaktif.")
    print()

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    global current_label, start_time, duration_sec

    os.makedirs(DATA_RAW_DIR, exist_ok=True)

    # ── Parse argumen ────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description="Kumpulkan data training per label aktivitas.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Contoh:\n"
            "  python src/data_collection.py               ← mode interaktif\n"
            "  python src/data_collection.py -l DUDUK      ← langsung rekam\n"
            "  python src/data_collection.py -l BERLARI -d 300\n"
        )
    )
    parser.add_argument(
        "--label", "-l",
        choices=CLASSES,
        default=None,       # ← tidak wajib, None = mode interaktif
        help=f"Label aktivitas: {CLASSES}\n(Opsional — jika tidak diisi, muncul menu interaktif)"
    )
    parser.add_argument(
        "--duration", "-d",
        type=int,
        default=None,
        help=f"Durasi (detik). Default={SESSION_DURATION_SEC} (15 menit)."
    )
    parser.add_argument(
        "--no-append",
        action="store_true",
        help="Jangan tambahkan ke dataset gabungan (simpan raw saja)."
    )
    args = parser.parse_args()

    # ── Tentukan label & durasi ──────────────────────────────
    if args.label is None:
        # Mode interaktif
        label, duration = interactive_menu()
    else:
        label    = args.label
        duration = args.duration if args.duration is not None else SESSION_DURATION_SEC

    current_label = label
    duration_sec  = duration

    # ── Konfirmasi sebelum mulai ─────────────────────────────
    print()
    print("  ┌──────────────────────────────────────────────┐")
    print(f"  │  Label   : {label:<34}│")
    print(f"  │  Durasi  : {duration} detik ({duration//60} menit {duration%60} detik){' '*(17-len(str(duration)))}│")
    print(f"  │  Broker  : {MQTT_BROKER}:{MQTT_PORT:<25}│")
    print("  └──────────────────────────────────────────────┘")
    print()

    try:
        mulai = input("  Tekan ENTER untuk mulai, Ctrl+C untuk batal: ")
    except KeyboardInterrupt:
        print("\n  Dibatalkan.")
        sys.exit(0)

    # ── Setup MQTT ───────────────────────────────────────────
    client = mqtt.Client(client_id=f"{MQTT_CLIENT_ID}_collector")
    client.on_connect = on_connect
    client.on_message = on_message

    def _stop(sig, frame):
        print("\n  Dihentikan manual (Ctrl+C).")
        # Kirim perintah STOP ke ESP32
        if 'client' in locals():
            send_command(client, "STOP")
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)

    print(f"\n  Menghubungkan ke {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"\n  ❌ Tidak bisa konek ke MQTT broker: {e}")
        print(f"     Pastikan broker berjalan di {MQTT_BROKER}:{MQTT_PORT}")
        sys.exit(1)

    client.loop_start()
    
    # Tunggu sebentar agar koneksi stabil
    time.sleep(1)
    
    # ── Kirim perintah START ke ESP32 ──────────────────────────
    print(f"\n  📤 Mengirim perintah START ke ESP32...")
    if send_command(client, "START", label, duration):
        print(f"  ✅ Perintah START terkirim. OLED akan menampilkan pesan.")
    else:
        print(f"  ⚠️  Gagal mengirim perintah START. Pastikan ESP32 terhubung.")
    
    # Beri waktu ESP32 memproses perintah
    time.sleep(0.5)
    
    start_time = time.time()

    # ── Loop countdown display ───────────────────────────────
    print(f"\n  Merekam [{label}]... Tekan Ctrl+C untuk berhenti lebih awal.")
    print()

    while not stop_event.is_set():
        elapsed   = time.time() - start_time
        remaining = max(0, duration_sec - elapsed)
        bpm_ok    = sum(1 for r in collected_rows if r.get("bpm", 0) > 0)
        m, s      = int(remaining // 60), int(remaining % 60)

        print(
            f"\r  [{label}]  Sisa: {m:02d}:{s:02d}  |  "
            f"Sampel: {len(collected_rows):4d}  |  "
            f"BPM valid: {bpm_ok:3d}   ",
            end="", flush=True
        )

        if duration_sec > 0 and elapsed >= duration_sec:
            stop_event.set()
            break

        time.sleep(0.5)

    print()  # newline setelah countdown
    
    # Kirim perintah STOP setelah selesai
    print(f"\n  📤 Mengirim perintah STOP ke ESP32...")
    send_command(client, "STOP")
    
    client.loop_stop()
    client.disconnect()

    # ── Simpan data ──────────────────────────────────────────
    if collected_rows:
        save_raw_session(label)
        if not args.no_append:
            append_to_dataset()
    else:
        logger.warning("Tidak ada data yang terkumpul.")

    # ── Tampilkan ringkasan ──────────────────────────────────
    print_summary(label, duration, args.no_append)


if __name__ == "__main__":
    main()