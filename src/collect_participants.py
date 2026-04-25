import json, os, sys, csv, signal, time, threading, shutil
from collections import Counter
from datetime import datetime
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT,
    TOPIC_SENSOR_DATA, TOPIC_STATUS, TOPIC_COMMAND,
    DATA_RAW_DIR, DATASET_PATH, DATASET_DIR,
    CLASSES, SESSION_DURATION_SEC
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("collect_participants")

SESSION_DURATION = SESSION_DURATION_SEC
SUMMARY_PATH     = os.path.join(DATASET_DIR, "sessions_summary.csv")
BACKUP_DIR       = os.path.join(DATASET_DIR, "backup")

SUMMARY_FIELDS = [
    "participant_no","participant_id","timestamp",
    "final_activity","final_bpm","avg_bpm",
    "count_duduk","count_berjalan","count_berlari",
    "total_samples","bpm_valid_pct","durasi_detik","sumber_hasil"
]
DATASET_FIELDS = [
    "received_at","participant_no","participant_id",
    "accel_stddev","gyro_stddev","bpm",
    "activity","local_act","timestamp"
]
RAW_FIELDS = [
    "received_at","participant_no","participant_id",
    "accel_stddev","gyro_stddev","bpm","local_act","timestamp"
]

class State:
    def __init__(self):
        self.current_no      = 0
        self.current_id      = ""
        self.session_active  = False
        self.session_done    = threading.Event()
        self.session_result  = {}
        self.raw_rows        = []
        self.aborted         = False
        self.restart_flag    = False
        self.stop_requested  = False
        self.session_start   = 0.0

state = State()

# ─────────────────────────────────────────────
#  HITUNG HASIL DARI RAW (fallback)
# ─────────────────────────────────────────────
def compute_result_from_raw(raw_rows):
    if not raw_rows:
        return {"final_activity":"","final_bpm":0,"avg_bpm":0,
                "count_duduk":0,"count_berjalan":0,"count_berlari":0,"total_samples":0}
    act_counts = Counter()
    bpm_vals   = []
    for row in raw_rows:
        act = str(row.get("local_act","")).upper().strip()
        if act in CLASSES:
            act_counts[act] += 1
        bpm = int(row.get("bpm", 0) or 0)
        if 40 < bpm < 220:
            bpm_vals.append(bpm)
    final_activity = act_counts.most_common(1)[0][0] if act_counts else "DUDUK"
    final_bpm      = int(sum(bpm_vals) / len(bpm_vals)) if bpm_vals else 0
    return {
        "final_activity": final_activity,
        "final_bpm":      final_bpm,
        "avg_bpm":        final_bpm,
        "count_duduk":    act_counts.get("DUDUK",    0),
        "count_berjalan": act_counts.get("BERJALAN", 0),
        "count_berlari":  act_counts.get("BERLARI",  0),
        "total_samples":  len(raw_rows),
    }

# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        client.subscribe(TOPIC_STATUS)
        print(f"\n  ✅ Terhubung ke EMQX Cloud!")
    else:
        logger.error(f"MQTT gagal rc={rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    # ── Data sensor → simpan ke buffer ─────────────────────
    if msg.topic == TOPIC_SENSOR_DATA and state.session_active:
        row = parse_sensor_payload(data)
        if row:
            row["participant_no"] = state.current_no
            row["participant_id"] = state.current_id
            row["local_act"]      = data.get("local_act", "")
            state.raw_rows.append(row)

    # ── Status akhir sesi dari ESP32 ────────────────────────
    elif msg.topic == TOPIC_STATUS and state.session_active:

        # ══ PERBAIKAN UTAMA ══════════════════════════════════
        # Validasi KETAT: pesan harus punya SEMUA field ini
        # untuk dianggap sebagai sinyal selesai sesi.
        # Ping online ("status":"online") tidak punya field ini
        # sehingga TIDAK akan memicu penghentian sesi.
        # ═════════════════════════════════════════════════════
        required_fields = ["final_activity", "count_duduk",
                           "count_berjalan", "count_berlari", "total_samples"]
        is_valid_result = all(f in data for f in required_fields)

        if not is_valid_result:
            logger.debug(
                f"STATUS diterima tapi bukan hasil sesi "
                f"(mungkin ping). Field ada: {list(data.keys())}"
            )
            return

        # Validasi participant_no harus cocok
        p_no = data.get("participant_no")
        if p_no is None:
            logger.debug("STATUS tanpa participant_no, diabaikan.")
            return

        p_no = int(p_no)
        if p_no != state.current_no:
            logger.warning(
                f"STATUS P{p_no} diterima tapi sesi aktif adalah P{state.current_no}. "
                f"Diabaikan (mungkin kiriman lama / delay)."
            )
            return

        # Validasi durasi minimum — abaikan jika terlalu singkat
        elapsed = time.time() - state.session_start
        MIN_VALID_DURATION = 60   # minimal 60 detik agar dianggap valid
        if elapsed < MIN_VALID_DURATION:
            logger.warning(
                f"STATUS P{p_no} diterima terlalu cepat "
                f"({elapsed:.1f}s < {MIN_VALID_DURATION}s minimum). "
                f"Kemungkinan STATUS sesi SEBELUMNYA yang terlambat datang. Diabaikan."
            )
            return

        # Semua validasi lolos → sesi selesai
        bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0)) > 0]
        state.session_result = {
            "final_activity": data.get("final_activity", ""),
            "final_bpm":      int(data.get("final_bpm", 0) or 0),
            "avg_bpm":        int(sum(bpm_vals)/len(bpm_vals)) if bpm_vals else 0,
            "count_duduk":    int(data.get("count_duduk",    0)),
            "count_berjalan": int(data.get("count_berjalan", 0)),
            "count_berlari":  int(data.get("count_berlari",  0)),
            "total_samples":  int(data.get("total_samples",  len(state.raw_rows))),
        }
        logger.info(
            f"[STATUS VALID] P{p_no} selesai: "
            f"akt={state.session_result['final_activity']} "
            f"elapsed={elapsed:.1f}s"
        )
        state.session_active = False
        state.session_done.set()

# ─────────────────────────────────────────────
#  WATCHDOG — hanya timeout setelah durasi penuh + buffer
# ─────────────────────────────────────────────
def session_timer_watchdog():
    """
    Watchdog aktif hanya SETELAH SESSION_DURATION + 15 detik.
    Tidak akan memotong sesi sebelum waktunya.
    """
    # Tunggu durasi penuh + 15 detik buffer
    time.sleep(SESSION_DURATION + 15)

    if state.session_active:
        logger.warning(
            f"[WATCHDOG] P{state.current_no} timeout setelah "
            f"{SESSION_DURATION + 15}s — fallback Python."
        )
        state.session_active = False
        state.session_done.set()

# ─────────────────────────────────────────────
#  KIRIM PERINTAH
# ─────────────────────────────────────────────
def send_start(client, participant_id, participant_no):
    payload = json.dumps({
        "cmd":            "START",
        "participant_id": participant_id,
        "participant_no": participant_no,
        "total":          999
    })
    r = client.publish(TOPIC_COMMAND, payload, qos=1)
    r.wait_for_publish(timeout=3)
    logger.info(f"START → P{participant_no} [{participant_id}]")

def send_stop(client, participant_no):
    payload = json.dumps({"cmd": "STOP", "participant_no": participant_no})
    try:
        r = client.publish(TOPIC_COMMAND, payload, qos=1)
        r.wait_for_publish(timeout=3)
        logger.info(f"STOP → P{participant_no}")
    except Exception as e:
        logger.error(f"Gagal kirim STOP: {e}")

# ─────────────────────────────────────────────
#  SIMPAN DATA
# ─────────────────────────────────────────────
def save_raw_session(participant_id, participant_no):
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW_DIR,
                        f"P{participant_no:03d}_{participant_id}_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(state.raw_rows)
    logger.info(f"Raw → {path} ({len(state.raw_rows)} baris)")

def append_to_dataset():
    if not state.raw_rows:
        return
    file_exists = os.path.isfile(DATASET_PATH)
    added = 0
    with open(DATASET_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DATASET_FIELDS, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        for row in state.raw_rows:
            w.writerow({
                "received_at":    row.get("received_at", ""),
                "participant_no": row.get("participant_no", ""),
                "participant_id": row.get("participant_id", ""),
                "accel_stddev":   row.get("accel_stddev", ""),
                "gyro_stddev":    row.get("gyro_stddev", ""),
                "bpm":            row.get("bpm", 0),
                "activity":       row.get("local_act", ""),
                "local_act":      row.get("local_act", ""),
                "timestamp":      row.get("timestamp", ""),
            })
            added += 1
    logger.info(f"Dataset +{added} baris → {DATASET_PATH}")

def save_summary(participant_no, participant_id, result, duration_sec, sumber):
    file_exists = os.path.isfile(SUMMARY_PATH)
    bpm_vals    = [int(r.get("bpm", 0)) for r in state.raw_rows
                   if int(r.get("bpm", 0)) > 0]
    bpm_pct     = round(len(bpm_vals)/len(state.raw_rows)*100, 1) \
                  if state.raw_rows else 0

    with open(SUMMARY_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow({
            "participant_no":  participant_no,
            "participant_id":  participant_id,
            "timestamp":       datetime.now().isoformat(),
            "final_activity":  result.get("final_activity", ""),
            "final_bpm":       result.get("final_bpm", 0),
            "avg_bpm":         result.get("avg_bpm", 0),
            "count_duduk":     result.get("count_duduk", 0),
            "count_berjalan":  result.get("count_berjalan", 0),
            "count_berlari":   result.get("count_berlari", 0),
            "total_samples":   result.get("total_samples", 0),
            "bpm_valid_pct":   bpm_pct,
            "durasi_detik":    round(duration_sec, 1),
            "sumber_hasil":    sumber,
        })

# ─────────────────────────────────────────────
#  RESTART
# ─────────────────────────────────────────────
def do_restart():
    print()
    print("  ┌────────────────────────────────────────────────────┐")
    print("  │               ⚠  KONFIRMASI RESTART               │")
    print("  │  Semua data di-BACKUP lalu dihapus. Mulai dari P1 │")
    print("  └────────────────────────────────────────────────────┘")
    if input("  Ketik YES untuk konfirmasi: ").strip() != "YES":
        print("  ↩ Dibatalkan.\n")
        return False

    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f"backup_{ts}")
    os.makedirs(dst, exist_ok=True)
    moved = []

    for path in [DATASET_PATH, SUMMARY_PATH]:
        if os.path.isfile(path):
            shutil.copy2(path, os.path.join(dst, os.path.basename(path)))
            os.remove(path)
            moved.append(path)

    raw_bk = os.path.join(dst, "raw")
    os.makedirs(raw_bk, exist_ok=True)
    if os.path.isdir(DATA_RAW_DIR):
        for fname in [f for f in os.listdir(DATA_RAW_DIR) if f.endswith(".csv")]:
            src = os.path.join(DATA_RAW_DIR, fname)
            shutil.copy2(src, os.path.join(raw_bk, fname))
            os.remove(src)
            moved.append(fname)

    print(f"\n  ✅ Backup → {dst} ({len(moved)} file)")
    print(f"  🔄 Mulai ulang dari Peserta 1...\n")
    logger.info(f"RESTART: backup ke {dst}")
    return True

# ─────────────────────────────────────────────
#  TIMER DISPLAY
# ─────────────────────────────────────────────
def show_session_timer(participant_no, participant_id):
    start = time.time()
    while state.session_active and not state.stop_requested:
        elapsed   = time.time() - start
        remaining = max(0, SESSION_DURATION - elapsed)
        m, s      = int(remaining // 60), int(remaining % 60)
        bpm_ok    = sum(1 for r in state.raw_rows if int(r.get("bpm", 0)) > 0)
        act       = state.raw_rows[-1].get("local_act", "?") if state.raw_rows else "?"
        print(
            f"\r  ⏱  P{participant_no} [{participant_id}] | "
            f"Deteksi: {act:<8} | "
            f"Sisa: {m:02d}:{s:02d} | "
            f"Sampel: {len(state.raw_rows):4d} | "
            f"BPM: {bpm_ok:3d}   ",
            end="", flush=True
        )
        time.sleep(0.5)
    print()

# ─────────────────────────────────────────────
#  INPUT PESERTA
# ─────────────────────────────────────────────
def input_participant(no, completed_ids):
    print(f"\n{'─'*57}")
    print(f"  👤 PESERTA KE-{no}")
    print(f"  Sebelumnya: {', '.join(completed_ids[-5:]) if completed_ids else '-'}")
    print(f"{'─'*57}")
    print(f"  Perintah: [r]estart | [s]kip | [q]uit\n")

    while True:
        try:
            raw = input(f"  ID Peserta {no}: ").strip()
        except (EOFError, KeyboardInterrupt):
            state.aborted = True
            return None

        lower = raw.lower()
        if lower in ("q", "quit"):
            state.aborted = True
            return None
        if lower in ("s", "skip"):
            print(f"  ⏭ Peserta ke-{no} dilewati.\n")
            return "__SKIP__"
        if lower in ("r", "restart"):
            if do_restart():
                state.restart_flag = True
                return None
            continue
        if not raw:
            print("  ⚠ ID tidak boleh kosong.")
            continue
        if len(raw) > 30:
            print("  ⚠ Terlalu panjang (maks 30 karakter).")
            continue
        safe = raw.replace(" ", "_").replace(",", "").replace('"', "").replace("'", "")
        if safe != raw:
            print(f"  ℹ ID diubah: '{safe}'")
        return safe

# ─────────────────────────────────────────────
#  JALANKAN SATU SESI
# ─────────────────────────────────────────────
def run_session(client, participant_no, participant_id):
    state.current_no     = participant_no
    state.current_id     = participant_id
    state.session_active = True
    state.stop_requested = False
    state.session_done.clear()
    state.session_result = {}
    state.raw_rows       = []
    state.session_start  = time.time()

    send_start(client, participant_id, participant_no)

    print(f"\n  🟢 P{participant_no} [{participant_id}] mulai — {SESSION_DURATION//60} menit")
    print(f"  Tekan Ctrl+C untuk berhenti darurat\n")

    # Thread: tampilan timer
    threading.Thread(
        target=show_session_timer,
        args=(participant_no, participant_id),
        daemon=True
    ).start()

    # Thread: watchdog (hanya aktif setelah durasi penuh)
    threading.Thread(
        target=session_timer_watchdog,
        daemon=True
    ).start()

    # Tunggu sinyal selesai
    state.session_done.wait()
    state.session_active = False
    duration = time.time() - state.session_start

    if state.stop_requested:
        print(f"\n  🛑 Mengirim STOP ke ESP32...")
        send_stop(client, participant_no)
        time.sleep(1)

    time.sleep(0.6)  # tunggu timer thread print newline

    # Tentukan hasil
    if state.session_result and state.session_result.get("final_activity"):
        result = state.session_result
        sumber = "esp32"
    else:
        result = compute_result_from_raw(state.raw_rows)
        sumber = "python"
        logger.info(f"Fallback Python: {result}")

    if not state.raw_rows:
        print(f"\n  ❌ Tidak ada data P{participant_no}. Cek ESP32.\n")
        return False

    save_raw_session(participant_id, participant_no)
    append_to_dataset()
    save_summary(participant_no, participant_id, result, duration, sumber)

    bpm_vals = [int(r.get("bpm", 0)) for r in state.raw_rows if int(r.get("bpm", 0)) > 0]
    bpm_pct  = round(len(bpm_vals)/len(state.raw_rows)*100, 1) if state.raw_rows else 0
    src      = "📡 ESP32" if sumber == "esp32" else "🐍 Python"

    print(f"\n  ✅ P{participant_no} [{participant_id}] selesai! [{src}]")
    print(f"     Aktivitas : {result.get('final_activity', 'N/A')}")
    print(f"     BPM rata  : {result.get('avg_bpm', 0)} bpm | valid: {bpm_pct}%")
    print(f"     D:{result.get('count_duduk',0)}  J:{result.get('count_berjalan',0)}  "
          f"L:{result.get('count_berlari',0)}  | {result.get('total_samples',0)} sampel")
    print(f"     Durasi    : {duration:.1f}s ({duration/60:.1f} menit)\n")
    return True

# ─────────────────────────────────────────────
#  RINGKASAN
# ─────────────────────────────────────────────
def print_final_summary(completed_count):
    print(f"\n{'═'*62}")
    print(f"  SELESAI — {completed_count} sesi terekam")
    print(f"  Dataset   : {DATASET_PATH}")
    print(f"  Ringkasan : {SUMMARY_PATH}")
    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            print(f"\n  {'No':<5} {'ID':<15} {'Aktivitas':<12} {'BPM':>5} {'Durasi':>10}")
            print(f"  {'─'*52}")
            for r in rows[-10:]:
                dur = float(r.get("durasi_detik", 0))
                print(f"  {r['participant_no']:<5} {r['participant_id']:<15} "
                      f"{r['final_activity']:<12} {r['final_bpm']:>5}  "
                      f"{dur/60:.1f}m")
        except Exception:
            pass
    print(f"\n  Langkah berikutnya:")
    print(f"    1. Buka notebook 01_eksplorasi_data.ipynb")
    print(f"    2. Buka notebook 02_training_model.ipynb")
    print(f"    3. python src/server_knn.py")

# ─────────────────────────────────────────────
#  SIGINT
# ─────────────────────────────────────────────
def handle_sigint(sig, frame):
    print("\n\n  Ctrl+C — menghentikan...")
    state.aborted        = True
    state.session_active = False
    state.session_done.set()

# ─────────────────────────────────────────────
#  LOOP UTAMA
# ─────────────────────────────────────────────
def run_collection_loop(client):
    start_from    = 1
    completed_ids = []

    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if rows:
                start_from    = max(int(r["participant_no"]) for r in rows) + 1
                completed_ids = [r["participant_id"] for r in rows]
                print(f"\n  ℹ Melanjutkan dari sesi ke-{start_from} ({len(rows)} terekam)")
        except Exception:
            pass

    print(f"\n  ✅ MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  ℹ Durasi: {SESSION_DURATION//60} menit/sesi | Peserta: tidak terbatas")
    print(f"  Perintah: [r]estart | [s]kip | [q]uit\n")

    input("  Tekan ENTER untuk mulai... ")

    completed_count = start_from - 1
    no = start_from

    while True:
        if state.aborted:
            break

        pid = input_participant(no, completed_ids)

        if pid is None:
            if state.restart_flag:
                state.restart_flag = False
                return True
            break
        if pid == "__SKIP__":
            no += 1
            continue

        run_session(client, no, pid)
        if state.aborted:
            break
        if state.restart_flag:
            state.restart_flag = False
            return True

        completed_count += 1
        completed_ids.append(pid)

        print(f"  📊 Total sesi terekam: {completed_count}")

        if not state.aborted:
            print(f"\n  ⏳ Jeda 10 detik sebelum peserta berikutnya...")
            for i in range(10, 0, -1):
                if state.aborted:
                    break
                print(f"\r  Lanjut dalam {i}s...", end="", flush=True)
                time.sleep(1)
            print()

        no += 1

    print_final_summary(completed_count)
    return False

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    os.makedirs(DATASET_DIR,  exist_ok=True)
    os.makedirs(BACKUP_DIR,   exist_ok=True)
    signal.signal(signal.SIGINT, handle_sigint)

    print("╔═══════════════════════════════════════════════════════╗")
    print("║    AIoT Watch — Pengambilan Data Partisipan           ║")
    print(f"║    Setiap sesi: {SESSION_DURATION//60} menit | Peserta: tidak terbatas ║")
    print("╚═══════════════════════════════════════════════════════╝")

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"\n  ❌ Tidak bisa konek ke {MQTT_BROKER}:{MQTT_PORT}: {e}")
        sys.exit(1)

    client.loop_start()
    time.sleep(1)

    while True:
        state.aborted = state.restart_flag = state.stop_requested = False
        should_restart = run_collection_loop(client)
        if not should_restart:
            break
        print("\n" + "═"*60)
        print("  🔄 RESTART — Mulai dari Sesi 1...")
        print("═"*60 + "\n")

    client.loop_stop()
    client.disconnect()
    print("\n  Selesai. Sampai jumpa! 👋")


if __name__ == "__main__":
    main()