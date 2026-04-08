"""
collect_participants.py — Orkestrator pengambilan data 30 peserta

Cara pakai:
    python src/collect_participants.py

Perintah tersedia saat program berjalan:
    ENTER        → lanjut ke peserta berikutnya (input ID)
    r / restart  → reset semua data dan mulai dari peserta 1 lagi
    s / skip     → lewati peserta saat ini (tidak ada di sesi aktif)
    q / quit     → keluar dan simpan ringkasan
"""
import json, os, sys, csv, signal, time, threading, shutil
from collections import Counter
from datetime import datetime
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT,
    TOPIC_SENSOR_DATA, TOPIC_STATUS,
    DATA_RAW_DIR, DATASET_PATH, DATASET_DIR
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("collect_participants")

# ─────────────────────────────────────────────
#  KONFIGURASI
# ─────────────────────────────────────────────
TOTAL_PESERTA    = 30
SESSION_DURATION = 5 * 60          # 5 menit
TOPIC_CONTROL    = "control/session"
SUMMARY_PATH     = os.path.join(DATASET_DIR, "sessions_summary.csv")
BACKUP_DIR       = os.path.join(DATASET_DIR, "backup")

SUMMARY_FIELDS = [
    "participant_no","participant_id","timestamp",
    "final_activity","final_bpm",
    "count_duduk","count_berjalan","count_berlari",
    "total_samples","durasi_detik","sumber_hasil"
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

# ─────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────
class State:
    def __init__(self):
        self.current_no     = 0
        self.current_id     = ""
        self.session_start  = 0.0
        self.session_active = False
        self.session_done   = threading.Event()
        self.session_result = {}
        self.raw_rows       = []
        self.aborted        = False
        self.restart_flag   = False   # ← diset True saat user ketik 'restart'

state = State()

# ─────────────────────────────────────────────
#  HITUNG HASIL DARI RAW (fallback)
# ─────────────────────────────────────────────
def compute_result_from_raw(raw_rows):
    if not raw_rows:
        return {"final_activity":"","final_bpm":0,
                "count_duduk":0,"count_berjalan":0,"count_berlari":0,"total_samples":0}
    act_counts = Counter()
    bpm_vals   = []
    for row in raw_rows:
        act = str(row.get("local_act","")).upper().strip()
        if act in ("DUDUK","BERJALAN","BERLARI"):
            act_counts[act] += 1
        bpm = int(row.get("bpm",0) or 0)
        if 40 < bpm < 200:
            bpm_vals.append(bpm)
    final_activity = act_counts.most_common(1)[0][0] if act_counts else "DUDUK"
    final_bpm      = int(sum(bpm_vals)/len(bpm_vals)) if bpm_vals else 0
    return {
        "final_activity": final_activity,
        "final_bpm":      final_bpm,
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
            state.session_result = {
                "final_activity": data.get("final_activity",""),
                "final_bpm":      int(data.get("final_bpm",0) or 0),
                "count_duduk":    int(data.get("count_duduk",0)),
                "count_berjalan": int(data.get("count_berjalan",0)),
                "count_berlari":  int(data.get("count_berlari",0)),
                "total_samples":  int(data.get("total_samples", len(state.raw_rows))),
            }
            state.session_active = False
            state.session_done.set()
            logger.info(f"[STATUS] P{p_no} act={data.get('final_activity')} bpm={data.get('final_bpm')}")

# ─────────────────────────────────────────────
#  WATCHDOG: paksa selesai jika ESP32 tidak kirim status
# ─────────────────────────────────────────────
def session_timer_watchdog():
    time.sleep(SESSION_DURATION + 10)
    if state.session_active:
        logger.warning(f"[WATCHDOG] Timeout P{state.current_no} — fallback Python.")
        state.session_active = False
        state.session_done.set()

# ─────────────────────────────────────────────
#  KIRIM START ke ESP32
# ─────────────────────────────────────────────
def send_start(client, participant_id, participant_no):
    payload = json.dumps({
        "cmd":            "START",
        "participant_id": participant_id,
        "participant_no": participant_no,
        "total":          TOTAL_PESERTA
    })
    r = client.publish(TOPIC_CONTROL, payload, qos=1)
    r.wait_for_publish(timeout=3)
    logger.info(f"START dikirim → P{participant_no} [{participant_id}]")

# ─────────────────────────────────────────────
#  SIMPAN RAW
# ─────────────────────────────────────────────
def save_raw_session(participant_id, participant_no):
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW_DIR, f"P{participant_no:02d}_{participant_id}_{ts}.csv")
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS, extrasaction="ignore")
        w.writeheader(); w.writerows(state.raw_rows)
    logger.info(f"Raw → {path} ({len(state.raw_rows)} baris)")

# ─────────────────────────────────────────────
#  TAMBAHKAN KE DATASET
# ─────────────────────────────────────────────
def append_to_dataset():
    if not state.raw_rows: return
    file_exists = os.path.isfile(DATASET_PATH)
    added = 0
    with open(DATASET_PATH,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DATASET_FIELDS, extrasaction="ignore")
        if not file_exists: w.writeheader()
        for row in state.raw_rows:
            act = str(row.get("local_act","")).upper().strip()
            if act not in ("DUDUK","BERJALAN","BERLARI"): continue
            w.writerow({
                "received_at":    row.get("received_at",""),
                "participant_no": row.get("participant_no",""),
                "participant_id": row.get("participant_id",""),
                "accel_stddev":   row.get("accel_stddev",""),
                "gyro_stddev":    row.get("gyro_stddev",""),
                "bpm":            row.get("bpm",0),
                "activity":       act,
                "local_act":      act,
                "timestamp":      row.get("timestamp",""),
            }); added += 1
    logger.info(f"Dataset +{added} baris → {DATASET_PATH}")

# ─────────────────────────────────────────────
#  SIMPAN RINGKASAN
# ─────────────────────────────────────────────
def save_summary(participant_no, participant_id, result, duration_sec, sumber):
    file_exists = os.path.isfile(SUMMARY_PATH)
    with open(SUMMARY_PATH,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        if not file_exists: w.writeheader()
        w.writerow({
            "participant_no":  participant_no,
            "participant_id":  participant_id,
            "timestamp":       datetime.now().isoformat(),
            "final_activity":  result.get("final_activity",""),
            "final_bpm":       result.get("final_bpm",0),
            "count_duduk":     result.get("count_duduk",0),
            "count_berjalan":  result.get("count_berjalan",0),
            "count_berlari":   result.get("count_berlari",0),
            "total_samples":   result.get("total_samples",0),
            "durasi_detik":    round(duration_sec,1),
            "sumber_hasil":    sumber,
        })
    logger.info(f"Summary P{participant_no} [{sumber}] akt={result.get('final_activity')} bpm={result.get('final_bpm')}")

# ─────────────────────────────────────────────
#  BACKUP & RESTART — hapus semua data dan mulai dari 0
# ─────────────────────────────────────────────
def do_restart():
    """
    Backup semua data yang sudah ada ke folder backup/,
    lalu hapus sessions_summary.csv, dataset.csv, dan semua raw CSV
    sehingga pengambilan dimulai dari peserta 1 lagi.
    """
    print()
    print("  ╔═══════════════════════════════════════════════════════╗")
    print("  ║                    ⚠  KONFIRMASI RESTART              ║")
    print("  ║                                                       ║")
    print("  ║  Semua data yang sudah diambil akan di-BACKUP         ║")
    print("  ║  lalu dihapus. Pengambilan mulai dari P1 lagi.        ║")
    print("  ╚═══════════════════════════════════════════════════════╝")
    print()

    # PERBAIKAN: Case-insensitive, terima "YES" atau "yes" atau "Yes"
    konfirmasi = input("  Ketik  YES  (case-insensitive) untuk konfirmasi restart: ").strip().upper()
    if konfirmasi != "YES":
        print("  ↩  Restart dibatalkan.\n")
        return False

    # ── Buat folder backup dengan timestamp ──────────────────
    ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dst = os.path.join(BACKUP_DIR, f"backup_{ts}")
    os.makedirs(backup_dst, exist_ok=True)

    moved = []

    # Backup dataset.csv
    if os.path.isfile(DATASET_PATH):
        dst = os.path.join(backup_dst, "dataset.csv")
        shutil.copy2(DATASET_PATH, dst)
        os.remove(DATASET_PATH)
        moved.append("dataset.csv")

    # Backup sessions_summary.csv
    if os.path.isfile(SUMMARY_PATH):
        dst = os.path.join(backup_dst, "sessions_summary.csv")
        shutil.copy2(SUMMARY_PATH, dst)
        os.remove(SUMMARY_PATH)
        moved.append("sessions_summary.csv")

    # Backup semua raw CSV
    if os.path.isdir(DATA_RAW_DIR):
        raw_backup = os.path.join(backup_dst, "raw")
        os.makedirs(raw_backup, exist_ok=True)
        raw_files = [f for f in os.listdir(DATA_RAW_DIR) if f.endswith(".csv")]
        for fname in raw_files:
            src = os.path.join(DATA_RAW_DIR, fname)
            shutil.copy2(src, os.path.join(raw_backup, fname))
            os.remove(src)
            moved.append(fname)

    print(f"\n  ✅ Backup berhasil disimpan ke → {backup_dst}")
    print(f"     ({len(moved)} file di-backup)")
    print(f"  ✅ Semua data lama telah dihapus.")
    print(f"  🔄 Mulai ulang pengambilan data dari Peserta 1...\n")

    logger.info(f"RESTART: backup ke {backup_dst}, {len(moved)} file dipindah.")
    return True

# ─────────────────────────────────────────────
#  TIMER DISPLAY
# ─────────────────────────────────────────────
def show_session_timer(participant_no, participant_id):
    start = time.time()
    while state.session_active:
        elapsed   = time.time() - start
        remaining = max(0, SESSION_DURATION - elapsed)
        m, s = int(remaining//60), int(remaining%60)
        print(f"\r  ⏱  P{participant_no} [{participant_id}] Sisa: {m:02d}:{s:02d} | Sampel: {len(state.raw_rows):4d}   ",
              end="", flush=True)
        time.sleep(0.5)
    print()

# ─────────────────────────────────────────────
#  INPUT PESERTA (dengan perintah khusus)
# ─────────────────────────────────────────────
def input_participant(no, completed_ids):
    """
    Meminta ID peserta. Mendukung perintah:
      r / restart → mulai ulang dari 0
      s / skip    → lewati nomor ini
      q / quit    → keluar program
    """
    print(f"\n{'─'*57}")
    print(f"  Peserta ke-{no} dari {TOTAL_PESERTA}")
    print(f"  Sebelumnya: {', '.join(completed_ids[-5:]) if completed_ids else '-'}")
    print(f"{'─'*57}")
    print(f"  Perintah: [r]estart | [s]kip | [q]uit")

    while True:
        try:
            raw = input(f"  ID Peserta {no} (atau perintah): ").strip()
        except (EOFError, KeyboardInterrupt):
            state.aborted = True
            return None

        lower = raw.lower()

        # ── Perintah quit ───────────────────────────────────
        if lower in ("q", "quit", "exit"):
            print("  👋 Keluar dari program...")
            state.aborted = True
            return None

        # ── Perintah skip ───────────────────────────────────
        if lower in ("s", "skip"):
            print(f"  ⏭  Peserta ke-{no} dilewati.\n")
            return "__SKIP__"

        # ── Perintah restart ────────────────────────────────
        if lower in ("r", "restart"):
            berhasil = do_restart()
            if berhasil:
                state.restart_flag = True
                return None   # sinyal restart ke loop utama
            # PERBAIKAN: Jika restart dibatalkan, lanjut minta ID lagi
            continue

        # ── Input ID normal ─────────────────────────────────
        if not raw:
            print("  ⚠  ID tidak boleh kosong."); continue
        if raw in completed_ids:
            print(f"  ⚠  ID '{raw}' sudah dipakai."); continue
        if len(raw) > 20:
            print(f"  ⚠  Terlalu panjang (maks 20 karakter)."); continue

        safe = raw.replace(" ","_").replace(",","").replace('"',"").replace("'","")
        if safe != raw:
            print(f"  ℹ  ID diubah: '{safe}'")
        return safe

# ─────────────────────────────────────────────
#  JALANKAN SATU SESI
# ─────────────────────────────────────────────
def run_session(client, participant_no, participant_id):
    state.current_no     = participant_no
    state.current_id     = participant_id
    state.session_active = True
    state.session_done.clear()
    state.session_result = {}
    state.raw_rows       = []
    state.session_start  = time.time()

    send_start(client, participant_id, participant_no)
    print(f"\n  🟢 Sesi P{participant_no} [{participant_id}] dimulai. ESP32 countdown 3s → rekam 5 menit.\n")

    threading.Thread(target=show_session_timer,     args=(participant_no, participant_id), daemon=True).start()
    threading.Thread(target=session_timer_watchdog, daemon=True).start()

    state.session_done.wait()
    state.session_active = False
    duration = time.time() - state.session_start
    time.sleep(0.6)

    # Tentukan hasil
    if state.session_result and state.session_result.get("final_activity"):
        result = state.session_result; sumber = "esp32"
    else:
        result = compute_result_from_raw(state.raw_rows); sumber = "python"

    if not state.raw_rows:
        print(f"\n  ❌ Tidak ada data untuk P{participant_no}. Cek koneksi ESP32.")
        return False

    save_raw_session(participant_id, participant_no)
    append_to_dataset()
    save_summary(participant_no, participant_id, result, duration, sumber)

    src = "📡 ESP32" if sumber=="esp32" else "🐍 Python (fallback)"
    act = result.get("final_activity","N/A"); bpm = result.get("final_bpm",0)
    icon = "✅" if sumber=="esp32" else "⚠ "
    print(f"\n  {icon} P{participant_no} [{participant_id}] selesai! [{src}]")
    print(f"     Aktivitas : {act}")
    print(f"     BPM       : {bpm} bpm")
    print(f"     Duduk:{result.get('count_duduk',0)}  Jalan:{result.get('count_berjalan',0)}  Lari:{result.get('count_berlari',0)}  | {result.get('total_samples',0)} sampel")
    print(f"     Durasi    : {int(duration//60)}:{int(duration%60):02d}")
    return True

# ─────────────────────────────────────────────
#  RINGKASAN AKHIR
# ─────────────────────────────────────────────
def print_final_summary(completed):
    print(f"\n{'═'*60}")
    print(f"  SELESAI — {len(completed)}/{TOTAL_PESERTA} peserta terekam")
    print(f"{'═'*60}")
    print(f"  Dataset   : {DATASET_PATH}")
    print(f"  Ringkasan : {SUMMARY_PATH}")
    if os.path.isfile(SUMMARY_PATH):
        try:
            import csv as _csv
            with open(SUMMARY_PATH,"r",encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
            print(f"\n  {'No':<4} {'ID':<14} {'Aktivitas':<12} {'BPM':>5}  {'D':>4} {'J':>4} {'L':>4}  Sumber")
            print(f"  {'─'*62}")
            for r in rows:
                print(f"  {r['participant_no']:<4} {r['participant_id']:<14} "
                      f"{r['final_activity']:<12} {r['final_bpm']:>5}  "
                      f"{r['count_duduk']:>4} {r['count_berjalan']:>4} {r['count_berlari']:>4}  "
                      f"{r.get('sumber_hasil','')}")
        except Exception:
            pass
    print(f"\n  Backup tersedia di: {BACKUP_DIR}/")
    print(f"\n  Langkah berikutnya:")
    print(f"    1. notebook 01_eksplorasi_data.ipynb")
    print(f"    2. notebook 02_training_model.ipynb")
    print(f"    3. python src/server_knn.py")

# ─────────────────────────────────────────────
#  SIGINT
# ─────────────────────────────────────────────
def handle_sigint(sig, frame):
    print("\n\n  Ctrl+C — menghentikan sesi aktif...")
    state.aborted = state.session_active = True
    state.session_done.set()

# ─────────────────────────────────────────────
#  LOOP UTAMA (mendukung restart)
# ─────────────────────────────────────────────
def run_collection_loop(client):
    """
    Loop pengambilan data. Dipanggil ulang setelah restart.
    Return True → restart, False → selesai/quit.
    """
    start_from    = 1
    completed_ids = []

    # Cek apakah ada sesi sebelumnya (lanjut atau sudah selesai)
    if os.path.isfile(SUMMARY_PATH):
        try:
            import csv as _csv
            with open(SUMMARY_PATH,"r",encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
            if rows:
                last_no = max(int(r["participant_no"]) for r in rows)
                start_from    = last_no + 1
                completed_ids = [r["participant_id"] for r in rows]
                print(f"\n  ℹ  Melanjutkan dari peserta ke-{start_from} ({len(rows)} sudah terekam)")
        except Exception:
            pass

    if start_from > TOTAL_PESERTA:
        print("\n  ✅ Semua peserta sudah selesai!")
        print("  Gunakan perintah  r  atau  restart  untuk mulai ulang dari P1.\n")
        # Tampilkan menu pilihan
        while True:
            cmd = input("  Ketik [r]estart atau [q]uit: ").strip().lower()
            if cmd in ("r","restart"):
                berhasil = do_restart()
                if berhasil:
                    return True   # sinyal restart ke main()
            elif cmd in ("q","quit",""):
                print_final_summary(completed_ids)
                return False

    # Pesan awal
    print(f"\n  ✅ MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
    print("  ℹ  Hasil: ESP32 (utama) | Python hitung (fallback jika ESP32 tidak kirim balik)")
    print()
    print(f"  Perintah yang bisa diketik saat input ID peserta:")
    print(f"    r / restart  → backup semua data & mulai dari P1 lagi")
    print(f"    s / skip     → lewati peserta saat ini")
    print(f"    q / quit     → keluar program")
    print()

    input("  Tekan ENTER untuk mulai... ")

    for no in range(start_from, TOTAL_PESERTA + 1):
        if state.aborted: break

        pid = input_participant(no, completed_ids)

        # Perintah quit
        if pid is None:
            if state.restart_flag:
                state.restart_flag = False
                return True   # sinyal restart
            break  # quit

        # Perintah skip
        if pid == "__SKIP__":
            continue

        run_session(client, no, pid)
        if state.aborted: break

        # Cek apakah user mengetik restart di sesi berikutnya
        if state.restart_flag:
            state.restart_flag = False
            return True

        completed_ids.append(pid)

        remaining_sesi = TOTAL_PESERTA - no
        pct = no / TOTAL_PESERTA * 100
        bar = "█" * int(30*no/TOTAL_PESERTA) + "░" * (30 - int(30*no/TOTAL_PESERTA))
        print(f"\n  [{bar}] {no}/{TOTAL_PESERTA} ({pct:.0f}%) | ETA: ±{remaining_sesi*5} menit")

        if no < TOTAL_PESERTA and not state.aborted:
            print(f"\n  ⏳ Jeda 10 detik sebelum peserta berikutnya...")
            for i in range(10, 0, -1):
                if state.aborted: break
                print(f"\r  Lanjut dalam {i}s... (ENTER untuk skip)", end="", flush=True)
                time.sleep(1)
            print()

    print_final_summary(completed_ids)
    return False   # tidak restart

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    os.makedirs(DATASET_DIR,  exist_ok=True)
    os.makedirs(BACKUP_DIR,   exist_ok=True)
    signal.signal(signal.SIGINT, handle_sigint)

    print("╔═══════════════════════════════════════════════════════╗")
    print("║      AIoT Watch — Pengambilan Data Multi Peserta      ║")
    print(f"║      Total: {TOTAL_PESERTA} peserta × 5 menit                      ║")
    print("╚═══════════════════════════════════════════════════════╝")
    print(f"\n  Broker MQTT : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Dataset     : {DATASET_PATH}")
    print(f"  Ringkasan   : {SUMMARY_PATH}")
    print(f"  Backup      : {BACKUP_DIR}/")

    # Setup MQTT (sekali, tidak perlu reconnect saat restart)
    # PERBAIKAN: Fix deprecation warning
    client = mqtt.Client(client_id=f"collector_{int(time.time())}", callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"\n  ❌ Tidak bisa konek ke {MQTT_BROKER}:{MQTT_PORT}: {e}")
        sys.exit(1)

    client.loop_start()
    time.sleep(1)

    # Loop dengan dukungan restart
    while True:
        state.aborted = False
        should_restart = run_collection_loop(client)
        if not should_restart:
            break
        # Jika restart → reset state dan ulang loop
        print("\n" + "═"*60)
        print("  🔄 RESTART — Memulai pengambilan dari Peserta 1...")
        print("═"*60 + "\n")
        state.aborted        = False
        state.restart_flag   = False
        state.session_active = False
        state.session_done.clear()

    client.loop_stop()
    client.disconnect()
    print("\n  Program selesai. Sampai jumpa! 👋")

if __name__ == "__main__":
    main()