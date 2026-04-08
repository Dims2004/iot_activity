"""
collect_participants.py — Pengambilan data partisipan tanpa batas, 15 menit/sesi

Cara pakai:
    python src/collect_participants.py

Perintah saat input:
    r / restart  → backup semua data, mulai dari P1
    s / skip     → lewati peserta saat ini
    q / quit     → keluar
"""
import json, os, sys, csv, signal, time, threading, shutil
from collections import Counter
from datetime import datetime
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT,
    TOPIC_SENSOR_DATA, TOPIC_STATUS,
    DATA_RAW_DIR, DATASET_PATH, DATASET_DIR,
    CLASSES, SESSION_DURATION_SEC
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("collect_participants")

SESSION_DURATION = SESSION_DURATION_SEC   # 15 menit = 900 detik
TOPIC_CONTROL    = "control/session"
SUMMARY_PATH     = os.path.join(DATASET_DIR, "sessions_summary.csv")
BACKUP_DIR       = os.path.join(DATASET_DIR, "backup")

SUMMARY_FIELDS = [
    "participant_no","participant_id","activity_label","timestamp",
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
        self.activity_label  = ""   # label yang diminta operator
        self.session_active  = False
        self.session_done    = threading.Event()
        self.session_result  = {}
        self.raw_rows        = []
        self.aborted         = False
        self.restart_flag    = False

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
        if act in CLASSES: act_counts[act] += 1
        bpm = int(row.get("bpm",0) or 0)
        if 40 < bpm < 220: bpm_vals.append(bpm)

    final_activity = act_counts.most_common(1)[0][0] if act_counts else state.activity_label
    final_bpm      = int(sum(bpm_vals)/len(bpm_vals)) if bpm_vals else 0
    return {
        "final_activity": final_activity,
        "final_bpm":      final_bpm,
        "avg_bpm":        final_bpm,
        "count_duduk":    act_counts.get("DUDUK",0),
        "count_berjalan": act_counts.get("BERJALAN",0),
        "count_berlari":  act_counts.get("BERLARI",0),
        "total_samples":  len(raw_rows),
    }

# ─────────────────────────────────────────────
#  MQTT
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"MQTT terhubung {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        client.subscribe(TOPIC_STATUS)
    else:
        logger.error(f"MQTT gagal rc={rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    if msg.topic == TOPIC_SENSOR_DATA and state.session_active:
        row = parse_sensor_payload(data)
        if row:
            row["participant_no"] = state.current_no
            row["participant_id"] = state.current_id
            row["local_act"]      = data.get("local_act","")
            state.raw_rows.append(row)

    elif msg.topic == TOPIC_STATUS and state.session_active:
        p_no = int(data.get("participant_no", state.current_no))
        if p_no == state.current_no:
            bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0)) > 0]
            state.session_result = {
                "final_activity": data.get("final_activity",""),
                "final_bpm":      int(data.get("final_bpm",0) or 0),
                "avg_bpm":        int(sum(bpm_vals)/len(bpm_vals)) if bpm_vals else 0,
                "count_duduk":    int(data.get("count_duduk",0)),
                "count_berjalan": int(data.get("count_berjalan",0)),
                "count_berlari":  int(data.get("count_berlari",0)),
                "total_samples":  int(data.get("total_samples", len(state.raw_rows))),
            }
            state.session_active = False
            state.session_done.set()

# ─────────────────────────────────────────────
#  WATCHDOG
# ─────────────────────────────────────────────
def session_timer_watchdog():
    time.sleep(SESSION_DURATION + 10)
    if state.session_active:
        logger.warning(f"[WATCHDOG] P{state.current_no} timeout — fallback Python.")
        state.session_active = False
        state.session_done.set()

# ─────────────────────────────────────────────
#  KIRIM START
# ─────────────────────────────────────────────
def send_start(client, participant_id, participant_no):
    payload = json.dumps({
        "cmd":            "START",
        "participant_id": participant_id,
        "participant_no": participant_no,
        "total":          999   # tidak terbatas
    })
    r = client.publish(TOPIC_CONTROL, payload, qos=1)
    r.wait_for_publish(timeout=3)
    logger.info(f"START → P{participant_no} [{participant_id}]")

# ─────────────────────────────────────────────
#  SIMPAN DATA
# ─────────────────────────────────────────────
def save_raw_session(participant_id, participant_no):
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW_DIR,
                        f"P{participant_no:03d}_{participant_id}_{ts}.csv")
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS, extrasaction="ignore")
        w.writeheader(); w.writerows(state.raw_rows)
    logger.info(f"Raw → {path} ({len(state.raw_rows)} baris)")

def append_to_dataset(activity_label: str):
    if not state.raw_rows: return
    file_exists = os.path.isfile(DATASET_PATH)
    added = 0
    with open(DATASET_PATH,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DATASET_FIELDS, extrasaction="ignore")
        if not file_exists: w.writeheader()
        for row in state.raw_rows:
            # Gunakan activity_label dari operator (bukan local_act ESP32)
            # karena ini pengambilan data terstruktur
            w.writerow({
                "received_at":    row.get("received_at",""),
                "participant_no": row.get("participant_no",""),
                "participant_id": row.get("participant_id",""),
                "accel_stddev":   row.get("accel_stddev",""),
                "gyro_stddev":    row.get("gyro_stddev",""),
                "bpm":            row.get("bpm",0),
                "activity":       activity_label,
                "local_act":      row.get("local_act",""),
                "timestamp":      row.get("timestamp",""),
            }); added += 1
    logger.info(f"Dataset +{added} baris label={activity_label} → {DATASET_PATH}")

def save_summary(participant_no, participant_id, activity_label,
                 result, duration_sec, sumber):
    file_exists = os.path.isfile(SUMMARY_PATH)
    bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0))>0]
    bpm_valid_pct = round(len(bpm_vals)/len(state.raw_rows)*100, 1) if state.raw_rows else 0

    with open(SUMMARY_PATH,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        if not file_exists: w.writeheader()
        w.writerow({
            "participant_no":   participant_no,
            "participant_id":   participant_id,
            "activity_label":   activity_label,
            "timestamp":        datetime.now().isoformat(),
            "final_activity":   result.get("final_activity",""),
            "final_bpm":        result.get("final_bpm",0),
            "avg_bpm":          result.get("avg_bpm",0),
            "count_duduk":      result.get("count_duduk",0),
            "count_berjalan":   result.get("count_berjalan",0),
            "count_berlari":    result.get("count_berlari",0),
            "total_samples":    result.get("total_samples",0),
            "bpm_valid_pct":    bpm_valid_pct,
            "durasi_detik":     round(duration_sec,1),
            "sumber_hasil":     sumber,
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
        print("  ↩  Dibatalkan.\n"); return False

    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f"backup_{ts}")
    os.makedirs(dst, exist_ok=True)
    moved = []

    for path in [DATASET_PATH, SUMMARY_PATH]:
        if os.path.isfile(path):
            shutil.copy2(path, os.path.join(dst, os.path.basename(path)))
            os.remove(path); moved.append(path)

    raw_bk = os.path.join(dst, "raw"); os.makedirs(raw_bk, exist_ok=True)
    if os.path.isdir(DATA_RAW_DIR):
        for fname in [f for f in os.listdir(DATA_RAW_DIR) if f.endswith(".csv")]:
            src = os.path.join(DATA_RAW_DIR, fname)
            shutil.copy2(src, os.path.join(raw_bk, fname))
            os.remove(src); moved.append(fname)

    print(f"\n  ✅ Backup → {dst} ({len(moved)} file)")
    print(f"  🔄 Mulai ulang dari Peserta 1...\n")
    logger.info(f"RESTART: backup ke {dst}")
    return True

# ─────────────────────────────────────────────
#  INPUT PESERTA
# ─────────────────────────────────────────────
def input_participant(no, completed_ids):
    print(f"\n{'─'*57}")
    print(f"  Peserta ke-{no}")
    print(f"  Sebelumnya: {', '.join(completed_ids[-5:]) if completed_ids else '-'}")
    print(f"{'─'*57}")
    print(f"  Perintah: [r]estart | [s]kip | [q]uit")

    while True:
        try:
            raw = input(f"  ID Peserta {no}: ").strip()
        except (EOFError, KeyboardInterrupt):
            state.aborted = True; return None, None

        lower = raw.lower()
        if lower in ("q","quit","exit"):
            state.aborted = True; return None, None
        if lower in ("s","skip"):
            print(f"  ⏭  Peserta ke-{no} dilewati.\n"); return "__SKIP__", None
        if lower in ("r","restart"):
            if do_restart(): state.restart_flag = True; return None, None
            continue
        if not raw: print("  ⚠  ID kosong."); continue
        if len(raw) > 20: print("  ⚠  Terlalu panjang."); continue
        safe = raw.replace(" ","_").replace(",","").replace('"',"").replace("'","")
        if safe != raw: print(f"  ℹ  ID diubah: '{safe}'")

        # Tanya activity label
        print(f"\n  Label aktivitas untuk {safe}:")
        for i, cls in enumerate(CLASSES, 1):
            print(f"    {i}. {cls}")
        while True:
            try:
                pilih = input("  Pilih (1/2/3) atau ketik langsung: ").strip().upper()
            except (EOFError, KeyboardInterrupt):
                state.aborted = True; return None, None
            if pilih in ("1","DUDUK"):    return safe, "DUDUK"
            if pilih in ("2","BERJALAN"): return safe, "BERJALAN"
            if pilih in ("3","BERLARI"):  return safe, "BERLARI"
            print("  ⚠  Masukkan 1, 2, atau 3.")

def show_session_timer(participant_no, participant_id, activity_label):
    start = time.time()
    while state.session_active:
        elapsed   = time.time() - start
        remaining = max(0, SESSION_DURATION - elapsed)
        m, s      = int(remaining//60), int(remaining%60)
        bpm_ok    = sum(1 for r in state.raw_rows if int(r.get("bpm",0)) > 0)
        print(
            f"\r  ⏱  P{participant_no} [{participant_id}|{activity_label}] "
            f"Sisa: {m:02d}:{s:02d} | Sampel: {len(state.raw_rows):4d} "
            f"| BPM valid: {bpm_ok:3d}   ",
            end="", flush=True
        )
        time.sleep(0.5)
    print()

# ─────────────────────────────────────────────
#  JALANKAN SATU SESI
# ─────────────────────────────────────────────
def run_session(client, participant_no, participant_id, activity_label):
    state.current_no     = participant_no
    state.current_id     = participant_id
    state.activity_label = activity_label
    state.session_active = True
    state.session_done.clear()
    state.session_result = {}
    state.raw_rows       = []
    session_start        = time.time()

    send_start(client, participant_id, participant_no)
    print(f"\n  🟢 P{participant_no} [{participant_id}] — {activity_label} | 15 menit\n")

    threading.Thread(target=show_session_timer,
                     args=(participant_no, participant_id, activity_label),
                     daemon=True).start()
    threading.Thread(target=session_timer_watchdog, daemon=True).start()

    state.session_done.wait()
    state.session_active = False
    duration = time.time() - session_start
    time.sleep(0.6)

    # Tentukan hasil
    if state.session_result and state.session_result.get("final_activity"):
        result = state.session_result; sumber = "esp32"
    else:
        result = compute_result_from_raw(state.raw_rows); sumber = "python"

    if not state.raw_rows:
        print(f"\n  ❌ Tidak ada data P{participant_no}. Cek ESP32.\n")
        return False

    save_raw_session(participant_id, participant_no)
    append_to_dataset(activity_label)
    save_summary(participant_no, participant_id, activity_label,
                 result, duration, sumber)

    bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0))>0]
    bpm_pct  = round(len(bpm_vals)/len(state.raw_rows)*100, 1) if state.raw_rows else 0
    src      = "📡 ESP32" if sumber=="esp32" else "🐍 Python"
    print(f"\n  ✅ P{participant_no} [{participant_id}|{activity_label}] selesai! [{src}]")
    print(f"     Aktivitas : {result.get('final_activity','N/A')}")
    print(f"     BPM avg   : {result.get('avg_bpm',0)} bpm | valid: {bpm_pct}%")
    print(f"     D:{result.get('count_duduk',0)}  J:{result.get('count_berjalan',0)}  L:{result.get('count_berlari',0)}  | {result.get('total_samples',0)} sampel")
    return True

# ─────────────────────────────────────────────
#  RINGKASAN
# ─────────────────────────────────────────────
def print_final_summary(completed_count):
    print(f"\n{'═'*62}")
    print(f"  SELESAI — {completed_count} sesi terekam")
    print(f"{'═'*62}")
    print(f"  Dataset   : {DATASET_PATH}")
    print(f"  Ringkasan : {SUMMARY_PATH}")
    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH,"r",encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            print(f"\n  {'No':<5} {'ID':<14} {'Label':<10} {'Akt':<10} {'BPM':>5} {'BPM%':>6}  Sumber")
            print(f"  {'─'*65}")
            for r in rows:
                print(f"  {r['participant_no']:<5} {r['participant_id']:<14} "
                      f"{r['activity_label']:<10} {r['final_activity']:<10} "
                      f"{r['final_bpm']:>5} {r.get('bpm_valid_pct','?'):>5}%  "
                      f"{r.get('sumber_hasil','')}")
        except Exception:
            pass
    print(f"\n  Langkah berikutnya:")
    print(f"    1. notebook 01_eksplorasi_data.ipynb")
    print(f"    2. notebook 02_training_model.ipynb")
    print(f"    3. python src/server_knn.py")

def handle_sigint(sig, frame):
    print("\n\n  Ctrl+C diterima — menghentikan sesi...")
    state.aborted = state.session_active = True
    state.session_done.set()

# ─────────────────────────────────────────────
#  LOOP UTAMA
# ─────────────────────────────────────────────
def run_collection_loop(client):
    # Hitung peserta yang sudah ada
    start_from = 1
    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH,"r",encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if rows:
                start_from = max(int(r["participant_no"]) for r in rows) + 1
                print(f"\n  ℹ  Melanjutkan dari sesi ke-{start_from} ({len(rows)} sudah terekam)")
        except Exception:
            pass

    print(f"\n  ✅ MQTT terhubung {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  ℹ  Durasi per sesi: {SESSION_DURATION//60} menit")
    print(f"  ℹ  Jumlah peserta: TIDAK TERBATAS")
    print(f"  ℹ  Fitur: Accel StdDev + Gyro StdDev + BPM")
    print(f"\n  Perintah: [r]estart | [s]kip | [q]uit\n")

    input("  Tekan ENTER untuk mulai... ")

    completed_count = start_from - 1
    no = start_from

    while True:
        if state.aborted: break

        pid, label = input_participant(no, [])

        if pid is None:
            if state.restart_flag:
                state.restart_flag = False; return True
            break
        if pid == "__SKIP__":
            no += 1; continue

        success = run_session(client, no, pid, label)
        if state.aborted: break
        if state.restart_flag:
            state.restart_flag = False; return True

        if success:
            completed_count += 1

        print(f"\n  📊 Total sesi terekam: {completed_count}")

        if not state.aborted:
            print(f"\n  ⏳ Jeda 10 detik...")
            for i in range(10, 0, -1):
                if state.aborted: break
                print(f"\r  Lanjut dalam {i}s...", end="", flush=True)
                time.sleep(1)
            print()

        no += 1

    print_final_summary(completed_count)
    return False

def main():
    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    os.makedirs(DATASET_DIR,  exist_ok=True)
    os.makedirs(BACKUP_DIR,   exist_ok=True)
    signal.signal(signal.SIGINT, handle_sigint)

    print("╔═══════════════════════════════════════════════════════╗")
    print("║    AIoT Watch — Pengambilan Data (Tidak Terbatas)     ║")
    print(f"║    Setiap sesi: {SESSION_DURATION//60} menit | Fitur: Accel+Gyro+BPM    ║")
    print("╚═══════════════════════════════════════════════════════╝")

    client = mqtt.Client(client_id=f"collector_{int(time.time())}")
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"\n  ❌ Tidak bisa konek ke {MQTT_BROKER}:{MQTT_PORT}: {e}")
        sys.exit(1)

    client.loop_start(); time.sleep(1)

    while True:
        state.aborted = state.restart_flag = False
        should_restart = run_collection_loop(client)
        if not should_restart: break
        print("\n" + "═"*60)
        print("  🔄 RESTART — Mulai dari Sesi 1...")
        print("═"*60 + "\n")

    client.loop_stop(); client.disconnect()
    print("\n  Selesai. Sampai jumpa! 👋")

if __name__ == "__main__":
    main()