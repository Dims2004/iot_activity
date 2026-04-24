"""
collect_participants.py — Pengambilan data partisipan via cloud broker
Sekarang TIDAK perlu memilih label aktivitas — aktivitas dideteksi otomatis oleh ESP32 & KNN
Tambahan: Perintah STOP untuk menghentikan sesi lebih awal
FIXED: Sesi tidak berhenti tiba-tiba sebelum waktunya
"""
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
SUMMARY_PATH = os.path.join(DATASET_DIR, "sessions_summary.csv")
BACKUP_DIR = os.path.join(DATASET_DIR, "backup")

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
        self.current_no = 0
        self.current_id = ""
        self.session_active = False
        self.session_done = threading.Event()
        self.session_result = {}
        self.raw_rows = []
        self.aborted = False
        self.restart_flag = False
        self.stop_requested = False
        self.session_start_time = 0  # Track session start time

state = State()

def compute_result_from_raw(raw_rows):
    """Hitung hasil akhir dari raw data (fallback jika ESP32 tidak mengirim status)"""
    if not raw_rows:
        return {"final_activity":"","final_bpm":0,"avg_bpm":0,
                "count_duduk":0,"count_berjalan":0,"count_berlari":0,"total_samples":0}
    act_counts = Counter()
    bpm_vals = []
    for row in raw_rows:
        act = str(row.get("local_act","")).upper().strip()
        if act in CLASSES: act_counts[act] += 1
        bpm = int(row.get("bpm",0) or 0)
        if 40 < bpm < 220: bpm_vals.append(bpm)

    final_activity = act_counts.most_common(1)[0][0] if act_counts else "DUDUK"
    final_bpm = int(sum(bpm_vals)/len(bpm_vals)) if bpm_vals else 0
    return {
        "final_activity": final_activity,
        "final_bpm": final_bpm,
        "avg_bpm": final_bpm,
        "count_duduk": act_counts.get("DUDUK",0),
        "count_berjalan": act_counts.get("BERJALAN",0),
        "count_berlari": act_counts.get("BERLARI",0),
        "total_samples": len(raw_rows),
    }

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"MQTT terhubung ke cloud broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        client.subscribe(TOPIC_STATUS)
        print(f"\n  ✅ Terhubung ke EMQX Cloud!")
        print(f"     Topic Command: {TOPIC_COMMAND}")
    else:
        logger.error(f"MQTT gagal rc={rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        logger.debug(f"Received on {msg.topic}: {data}")
    except Exception as e:
        logger.error(f"JSON parse error: {e}")
        return

    if msg.topic == TOPIC_SENSOR_DATA and state.session_active:
        row = parse_sensor_payload(data)
        if row:
            row["participant_no"] = state.current_no
            row["participant_id"] = state.current_id
            row["local_act"] = data.get("local_act","")
            state.raw_rows.append(row)
            logger.debug(f"Data added, total={len(state.raw_rows)}")

    elif msg.topic == TOPIC_STATUS and state.session_active:
        p_no = int(data.get("participant_no", state.current_no))
        logger.info(f"Received STATUS from P{p_no}: {data.get('final_activity', '?')}")
        
        if p_no == state.current_no:
            bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0)) > 0]
            state.session_result = {
                "final_activity": data.get("final_activity",""),
                "final_bpm": int(data.get("final_bpm",0) or 0),
                "avg_bpm": int(sum(bpm_vals)/len(bpm_vals)) if bpm_vals else 0,
                "count_duduk": int(data.get("count_duduk",0)),
                "count_berjalan": int(data.get("count_berjalan",0)),
                "count_berlari": int(data.get("count_berlari",0)),
                "total_samples": int(data.get("total_samples", len(state.raw_rows))),
            }
            state.session_active = False
            state.session_done.set()
            logger.info(f"Session {p_no} completed by ESP32 signal")

def smart_watchdog():
    """Watchdog yang lebih cerdas - tidak memotong sesi terlalu awal"""
    # Tunggu minimal 30 detik untuk melihat apakah ada data
    time.sleep(30)
    
    # Jika tidak ada data sama sekali setelah 30 detik, beri peringatan
    if not state.raw_rows and state.session_active:
        logger.warning(f"Tidak ada data dari ESP32 setelah 30 detik untuk P{state.current_no}")
        print(f"\n  ⚠ Tidak menerima data dari ESP32! Cek koneksi ESP32.")
    
    # Lanjutkan watchdog normal - tunggu sampai durasi selesai + 10 detik
    remaining_time = max(0, SESSION_DURATION - 30)
    time.sleep(remaining_time + 10)
    
    # Jika sesi masih aktif setelah durasi normal, akhiri
    if state.session_active:
        logger.warning(f"[WATCHDOG] P{state.current_no} timeout — force ending session.")
        state.session_active = False
        state.session_done.set()

def send_start(client, participant_id, participant_no):
    """Kirim perintah START ke ESP32"""
    payload = json.dumps({
        "cmd": "START",
        "participant_id": participant_id,
        "participant_no": participant_no,
        "total": 999
    })
    r = client.publish(TOPIC_COMMAND, payload, qos=1)
    r.wait_for_publish(timeout=3)
    logger.info(f"START → P{participant_no} [{participant_id}] via {TOPIC_COMMAND}")

def send_stop(client, participant_no):
    """Kirim perintah STOP ke ESP32 untuk menghentikan sesi lebih awal"""
    payload = json.dumps({
        "cmd": "STOP",
        "participant_no": participant_no
    })
    try:
        r = client.publish(TOPIC_COMMAND, payload, qos=1)
        r.wait_for_publish(timeout=3)
        logger.info(f"STOP → P{participant_no} via {TOPIC_COMMAND}")
        return True
    except Exception as e:
        logger.error(f"Gagal kirim STOP: {e}")
        return False

def save_raw_session(participant_id, participant_no):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW_DIR, f"P{participant_no:03d}_{participant_id}_{ts}.csv")
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(state.raw_rows)
    logger.info(f"Raw → {path} ({len(state.raw_rows)} baris)")

def append_to_dataset():
    """Simpan data ke dataset.csv dengan activity dari hasil klasifikasi (local_act)"""
    if not state.raw_rows: return
    file_exists = os.path.isfile(DATASET_PATH)
    added = 0
    with open(DATASET_PATH,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DATASET_FIELDS, extrasaction="ignore")
        if not file_exists: w.writeheader()
        for row in state.raw_rows:
            w.writerow({
                "received_at": row.get("received_at",""),
                "participant_no": row.get("participant_no",""),
                "participant_id": row.get("participant_id",""),
                "accel_stddev": row.get("accel_stddev",""),
                "gyro_stddev": row.get("gyro_stddev",""),
                "bpm": row.get("bpm",0),
                "activity": row.get("local_act",""),
                "local_act": row.get("local_act",""),
                "timestamp": row.get("timestamp",""),
            })
            added += 1
    logger.info(f"Dataset +{added} baris → {DATASET_PATH}")

def save_summary(participant_no, participant_id, result, duration_sec, sumber):
    file_exists = os.path.isfile(SUMMARY_PATH)
    bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0))>0]
    bpm_valid_pct = round(len(bpm_vals)/len(state.raw_rows)*100, 1) if state.raw_rows else 0

    with open(SUMMARY_PATH,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        if not file_exists: w.writeheader()
        w.writerow({
            "participant_no": participant_no,
            "participant_id": participant_id,
            "timestamp": datetime.now().isoformat(),
            "final_activity": result.get("final_activity",""),
            "final_bpm": result.get("final_bpm",0),
            "avg_bpm": result.get("avg_bpm",0),
            "count_duduk": result.get("count_duduk",0),
            "count_berjalan": result.get("count_berjalan",0),
            "count_berlari": result.get("count_berlari",0),
            "total_samples": result.get("total_samples",0),
            "bpm_valid_pct": bpm_valid_pct,
            "durasi_detik": round(duration_sec,1),
            "sumber_hasil": sumber,
        })

def do_restart():
    print()
    print("  ┌────────────────────────────────────────────────────┐")
    print("  │               ⚠  KONFIRMASI RESTART               │")
    print("  │  Semua data di-BACKUP lalu dihapus. Mulai dari P1 │")
    print("  └────────────────────────────────────────────────────┘")
    if input("  Ketik YES untuk konfirmasi: ").strip() != "YES":
        print("  ↩ Dibatalkan.\n")
        return False

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
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

def input_participant(no, completed_ids):
    """Input hanya ID peserta, TIDAK perlu memilih label aktivitas"""
    print(f"\n{'─'*57}")
    print(f"  👤 PESERTA KE-{no}")
    print(f"  Sebelumnya: {', '.join(completed_ids[-5:]) if completed_ids else '-'}")
    print(f"{'─'*57}")
    print(f"  ℹ️  Aktivitas akan DIDETEKSI OTOMATIS oleh ESP32 & KNN")
    print(f"  Perintah: [r]estart | [s]kip | [q]uit | [t]stop (hentikan sesi berjalan)\n")

    while True:
        try:
            raw = input(f"  Masukkan ID Peserta {no}: ").strip()
        except (EOFError, KeyboardInterrupt):
            state.aborted = True
            return None

        lower = raw.lower()
        if lower in ("q","quit","exit"):
            state.aborted = True
            return None
        if lower in ("s","skip"):
            print(f"  ⏭ Peserta ke-{no} dilewati.\n")
            return "__SKIP__"
        if lower in ("r","restart"):
            if do_restart():
                state.restart_flag = True
                return None
            continue
        if lower in ("t","stop"):
            if state.session_active:
                print(f"\n  🛑 Menghentikan sesi berjalan...")
                state.stop_requested = True
                state.session_active = False
                state.session_done.set()
                return None
            else:
                print(f"  ⚠ Tidak ada sesi aktif yang sedang berjalan.")
                continue
        if not raw:
            print("  ⚠ ID tidak boleh kosong.")
            continue
        if len(raw) > 30:
            print("  ⚠ ID terlalu panjang (maks 30 karakter).")
            continue
        
        safe = raw.replace(" ","_").replace(",","").replace('"',"").replace("'","")
        if safe != raw:
            print(f"  ℹ ID diubah menjadi: '{safe}'")
        
        print(f"\n  ✅ Peserta '{safe}' terdaftar")
        print(f"  📌 Instruksi: Lakukan aktivitas NORMAL (duduk/jalan/lari) selama 15 menit")
        print(f"  🤖 ESP32 akan mendeteksi aktivitas secara otomatis!")
        print(f"  💡 Ketik 'stop' atau 't' kapan saja untuk menghentikan sesi lebih awal\n")
        return safe

def show_session_timer(participant_no, participant_id, client):
    """Tampilkan timer dan monitor input STOP dari user"""
    import sys
    start = time.time()
    
    def check_stop_input():
        while state.session_active and not state.stop_requested:
            try:
                if sys.stdin.isatty():
                    import select
                    if select.select([sys.stdin], [], [], 0.1)[0]:
                        cmd = sys.stdin.read(1).strip().lower()
                        if cmd in ('t', 's'):
                            if cmd == 't':
                                print(f"\n  🛑 Perintah STOP diterima!")
                                state.stop_requested = True
                                state.session_active = False
                                state.session_done.set()
            except:
                pass
            time.sleep(0.5)
    
    stop_thread = threading.Thread(target=check_stop_input, daemon=True)
    stop_thread.start()
    
    last_activity = ""
    last_log_time = start
    
    while state.session_active and not state.stop_requested:
        elapsed = time.time() - start
        remaining = max(0, SESSION_DURATION - elapsed)
        m, s = int(remaining//60), int(remaining%60)
        bpm_ok = sum(1 for r in state.raw_rows if int(r.get("bpm",0)) > 0)
        
        current_activity = "?"
        if state.raw_rows:
            last_row = state.raw_rows[-1]
            current_activity = last_row.get("local_act", "?")
        
        # Log status setiap 30 detik
        if time.time() - last_log_time > 30:
            last_log_time = time.time()
            logger.info(f"Session P{participant_no}: samples={len(state.raw_rows)}, "
                       f"activity={current_activity}, remaining={m:02d}:{s:02d}")
        
        print(f"\r  ⏱  P{participant_no} [{participant_id}] | "
              f"Deteksi: {current_activity:<8} | "
              f"Sisa: {m:02d}:{s:02d} | "
              f"Sampel: {len(state.raw_rows):4d} | "
              f"BPM: {bpm_ok:3d}   | (ketik 't' untuk stop)", end="", flush=True)
        time.sleep(0.5)
    print()

def run_session(client, participant_no, participant_id):
    state.current_no = participant_no
    state.current_id = participant_id
    state.session_active = True
    state.stop_requested = False
    state.session_done.clear()
    state.session_result = {}
    state.raw_rows = []
    state.session_start_time = time.time()
    session_start = state.session_start_time

    send_start(client, participant_id, participant_no)
    print(f"\n  🟢 Sesi P{participant_no} [{participant_id}] dimulai!")
    print(f"     Durasi: {SESSION_DURATION//60} menit ({SESSION_DURATION} detik)")
    print(f"     Aktivitas akan dideteksi otomatis oleh ESP32")
    print(f"     💡 Ketik 'stop' atau 't' untuk menghentikan sesi lebih awal\n")

    # Start timer display thread
    threading.Thread(target=show_session_timer,
                     args=(participant_no, participant_id, client),
                     daemon=True).start()
    
    # Start smart watchdog (tidak akan memotong sesi sebelum waktunya)
    threading.Thread(target=smart_watchdog, daemon=True).start()

    # Wait for session to complete (either by ESP32, user stop, or watchdog)
    state.session_done.wait()
    
    session_active_duration = time.time() - session_start
    logger.info(f"Session ended. Duration: {session_active_duration:.1f}s, "
               f"Stop requested: {state.stop_requested}, "
               f"Has result: {bool(state.session_result)}")
    
    state.session_active = False
    
    # Kirim STOP ke ESP32 jika dihentikan lebih awal
    if state.stop_requested:
        print(f"\n  🛑 Mengirim perintah STOP ke ESP32...")
        send_stop(client, participant_no)
        time.sleep(1)
    
    duration = time.time() - session_start
    
    # Jika durasi terlalu pendek (< 60 detik) dan bukan karena stop manual, beri peringatan
    if duration < 60 and not state.stop_requested and not state.aborted:
        logger.warning(f"Session too short ({duration:.1f}s)! Possible communication issue.")
        print(f"\n  ⚠ Peringatan: Sesi berakhir terlalu cepat ({duration:.1f} detik)!")
        print(f"     Seharusnya {SESSION_DURATION} detik. Cek koneksi ESP32.")
    
    time.sleep(0.6)

    # Tentukan hasil
    if state.session_result and state.session_result.get("final_activity"):
        result = state.session_result
        sumber = "esp32"
        logger.info(f"Using ESP32 result: {result}")
    else:
        result = compute_result_from_raw(state.raw_rows)
        sumber = "python"
        logger.info(f"Using Python fallback result: {result}")

    if not state.raw_rows:
        print(f"\n  ❌ Tidak ada data dari P{participant_no}. Cek ESP32.\n")
        return False

    save_raw_session(participant_id, participant_no)
    append_to_dataset()
    save_summary(participant_no, participant_id, result, duration, sumber)

    bpm_vals = [int(r.get("bpm",0)) for r in state.raw_rows if int(r.get("bpm",0))>0]
    bpm_pct = round(len(bpm_vals)/len(state.raw_rows)*100, 1) if state.raw_rows else 0
    src = "📡 ESP32" if sumber=="esp32" else "🐍 Python"
    
    status = "DIHENTIKAN AWAL" if state.stop_requested else "SELESAI"
    print(f"\n  ✅ P{participant_no} [{participant_id}] {status}! [{src}]")
    print(f"     Aktivitas Dominan : {result.get('final_activity','N/A')}")
    print(f"     Rata-rata BPM     : {result.get('avg_bpm',0)} bpm | valid: {bpm_pct}%")
    print(f"     Count D:{result.get('count_duduk',0)}  J:{result.get('count_berjalan',0)}  L:{result.get('count_berlari',0)}")
    print(f"     Total sampel      : {result.get('total_samples',0)}")
    print(f"     Durasi sesi       : {duration:.1f} detik ({duration/60:.1f} menit)\n")
    return True

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
            if rows:
                print(f"\n  {'No':<5} {'ID':<15} {'Aktivitas Dominan':<18} {'Rata BPM':>8} {'Valid%':>8} {'Durasi':>10}")
                print(f"  {'─'*70}")
                for r in rows[-10:]:
                    durasi = float(r.get('durasi_detik', 0))
                    durasi_str = f"{durasi/60:.1f}m" if durasi > 0 else "-"
                    print(f"  {r['participant_no']:<5} {r['participant_id']:<15} "
                          f"{r['final_activity']:<18} {r['avg_bpm']:>8} bpm "
                          f"{r.get('bpm_valid_pct','?'):>6}% {durasi_str:>10}")
        except Exception as e:
            print(f"  (gagal baca ringkasan: {e})")

def handle_sigint(sig, frame):
    print("\n\n  Ctrl+C diterima — menghentikan sesi...")
    state.aborted = state.session_active = True
    state.session_done.set()

def run_collection_loop(client):
    start_from = 1
    completed_ids = []
    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH,"r",encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if rows:
                start_from = max(int(r["participant_no"]) for r in rows) + 1
                completed_ids = [r["participant_id"] for r in rows]
                print(f"\n  ℹ Melanjutkan dari sesi ke-{start_from} ({len(rows)} sudah terekam)")
        except Exception:
            pass

    print(f"\n  ✅ MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  ℹ Topic Command: {TOPIC_COMMAND}")
    print(f"  ℹ Durasi per sesi: {SESSION_DURATION//60} menit")
    print(f"  ℹ Jumlah peserta: TIDAK TERBATAS")
    print(f"  ℹ Aktivitas DIDETEKSI OTOMATIS oleh ESP32 & KNN")
    print(f"\n  Perintah: [r]estart | [s]kip | [q]uit | [t]stop (hentikan sesi berjalan)\n")

    input("  Tekan ENTER untuk mulai... ")

    completed_count = start_from - 1
    no = start_from

    while True:
        if state.aborted: break

        pid = input_participant(no, completed_ids)

        if pid is None:
            if state.restart_flag:
                state.restart_flag = False
                return True
            break
        if pid == "__SKIP__":
            no += 1
            continue

        success = run_session(client, no, pid)
        if state.aborted: break
        if state.restart_flag:
            state.restart_flag = False
            return True

        if success:
            completed_count += 1
            completed_ids.append(pid)

        print(f"\n  📊 Total sesi terekam: {completed_count}")

        if not state.aborted and not state.stop_requested:
            print(f"\n  ⏳ Jeda 10 detik sebelum peserta berikutnya...")
            for i in range(10, 0, -1):
                if state.aborted: break
                print(f"\r  Lanjut dalam {i}s...", end="", flush=True)
                time.sleep(1)
            print()
        else:
            state.stop_requested = False

        no += 1

    print_final_summary(completed_count)
    return False

def main():
    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    os.makedirs(DATASET_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    signal.signal(signal.SIGINT, handle_sigint)

    print("╔═══════════════════════════════════════════════════════╗")
    print("║    AIoT Watch — Pengambilan Data Partisipan           ║")
    print("║    ✨ Aktivitas DIDETEKSI OTOMATIS oleh ESP32 & KNN   ║")
    print(f"║    Setiap sesi: {SESSION_DURATION//60} menit                     ║")
    print("║    💡 Ketik 'stop' atau 't' untuk hentikan sesi      ║")
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
        if not should_restart: break
        print("\n" + "═"*60)
        print("  🔄 RESTART — Mulai dari Sesi 1...")
        print("═"*60 + "\n")

    client.loop_stop()
    client.disconnect()
    print("\n  Selesai. Sampai jumpa! 👋")

if __name__ == "__main__":
    main()