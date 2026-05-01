"""
collect_participants.py — Pengambilan data training per partisipan
=================================================================
Alur:
  1. Operator masukkan ID partisipan secara manual
  2. Operator pilih label aktivitas (DUDUK / BERJALAN / BERLARI)
  3. Kirim START ke ESP32 via MQTT (hanya untuk memberi tahu ID partisipan)
  4. Terima data sensor dari ESP32 selama 15 menit
  5. Setiap baris data diberi label sesuai pilihan operator
  6. Simpan ke raw CSV + dataset.csv + sessions_summary.csv
  7. Lanjut ke peserta berikutnya

Catatan penting:
  - ESP32 TIDAK mengirim label aktivitas apa pun
  - Pelabelan 100% dilakukan di sini berdasarkan input manual operator
  - Satu sesi = satu aktivitas (operator HARUS memilih satu aktivitas per sesi)
  - Satu partisipan bisa punya beberapa sesi (misal: duduk 15 mnt, lalu berjalan 15 mnt)
"""

import json, os, sys, csv, signal, time, threading, shutil
from datetime import datetime
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT,
    TOPIC_SENSOR_DATA, TOPIC_COMMAND,
    DATA_RAW_DIR, DATASET_PATH, DATASET_DIR,
    CLASSES, SESSION_DURATION_SEC
)
from utils import get_logger, parse_sensor_payload

logger = get_logger("collect_participants")

# SESSION_DURATION_SEC dari config dipakai sebagai fallback saja.
# Durasi aktual dipilih operator via select_duration() tiap sesi.
DURATION_OPTIONS = [
    (15, 15 * 60),   # (menit, detik)
    (10, 10 * 60),
    ( 5,  5 * 60),
]
DEFAULT_DURATION_SEC = SESSION_DURATION_SEC  # fallback jika diperlukan
SUMMARY_PATH     = os.path.join(DATASET_DIR, "sessions_summary.csv")
BACKUP_DIR       = os.path.join(DATASET_DIR, "backup")

# ─────────────────────────────────────────────
#  FIELD DEFINITIONS
# ─────────────────────────────────────────────
SUMMARY_FIELDS = [
    "session_no", "participant_no", "participant_id",
    "activity", "timestamp",
    "total_samples", "bpm_valid", "bpm_valid_pct",
    "avg_accel_stddev", "avg_gyro_stddev", "avg_bpm",
    "durasi_detik"
]
DATASET_FIELDS = [
    "received_at", "participant_no", "participant_id",
    "accel_stddev", "gyro_stddev", "bpm",
    "activity",      # label manual dari operator
    "timestamp"
]
RAW_FIELDS = [
    "received_at", "participant_no", "participant_id",
    "accel_stddev", "gyro_stddev", "bpm",
    "activity",      # label manual
    "timestamp"
]


# ─────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────
class State:
    def __init__(self):
        self.current_no      = 0       # nomor partisipan
        self.current_id      = ""      # ID partisipan
        self.current_label   = ""      # label aktivitas sesi ini (MANUAL)
        self.session_no      = 0       # nomor sesi global (bertambah tiap sesi)
        self.session_active  = False
        self.session_done    = threading.Event()
        self.raw_rows        = []      # buffer data sensor sesi ini
        self.aborted         = False
        self.restart_flag    = False
        self.stop_requested  = False
        self.session_start   = 0.0

state = State()


# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        print(f"\n  ✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}!")
    else:
        logger.error(f"MQTT gagal rc={rc}")
        print(f"\n  ❌ Gagal terhubung MQTT (rc={rc})")


def on_message(client, userdata, msg):
    """
    Terima data sensor dari ESP32.
    Label aktivitas TIDAK diambil dari payload — seluruhnya dari input manual.
    """
    if msg.topic != TOPIC_SENSOR_DATA:
        return
    if not state.session_active:
        return

    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    row = parse_sensor_payload(data)
    if row is None:
        return

    # Tambahkan info partisipan + label manual
    row["participant_no"] = state.current_no
    row["participant_id"] = state.current_id
    row["activity"]       = state.current_label  # ← label dari input operator
    # Hapus field local_act jika ada (tidak relevan)
    row.pop("local_act", None)
    row.pop("user",      None)

    state.raw_rows.append(row)


# ─────────────────────────────────────────────
#  KIRIM PERINTAH KE ESP32
# ─────────────────────────────────────────────
def send_start(client, participant_id: str, participant_no: int,
               duration: int = SESSION_DURATION):
    """
    Kirim START ke ESP32.
    - participant_id / participant_no : disertakan di setiap payload sensor.
    - duration (detik)               : diteruskan ke ESP32 untuk countdown OLED.
    """
    payload = json.dumps({
        "cmd":            "START",
        "participant_id": participant_id,
        "participant_no": participant_no,
        "duration":       duration,      # ESP32 pakai ini untuk countdown OLED
    })
    try:
        r = client.publish(TOPIC_COMMAND, payload, qos=1)
        r.wait_for_publish(timeout=3)
        logger.info(f"START → P{participant_no} [{participant_id}]")
        return True
    except Exception as e:
        logger.error(f"Gagal kirim START: {e}")
        return False


def send_stop(client, participant_no: int):
    """Kirim STOP ke ESP32 agar ESP32 kosongkan participant_id."""
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
def save_raw_session(participant_id: str, participant_no: int, activity: str) -> str:
    """Simpan raw data sesi ke file CSV tersendiri."""
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"P{participant_no:03d}_{participant_id}_{activity}_{ts}.csv"
    path  = os.path.join(DATA_RAW_DIR, fname)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RAW_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(state.raw_rows)
    logger.info(f"Raw → {path} ({len(state.raw_rows)} baris)")
    return path


def append_to_dataset():
    """Tambahkan baris sesi ini ke dataset.csv utama."""
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
                "activity":       row.get("activity", ""),   # label manual
                "timestamp":      row.get("timestamp", ""),
            })
            added += 1
    logger.info(f"Dataset +{added} baris → {DATASET_PATH}")


def save_summary(participant_no: int, participant_id: str,
                 activity: str, duration_actual: float) -> None:
    """Simpan ringkasan sesi ke sessions_summary.csv."""
    state.session_no += 1
    rows = state.raw_rows

    total   = len(rows)
    bpm_ok  = [int(r.get("bpm", 0)) for r in rows if int(r.get("bpm", 0)) > 0]
    bpm_pct = round(len(bpm_ok) / total * 100, 1) if total > 0 else 0
    avg_bpm = round(sum(bpm_ok) / len(bpm_ok), 1) if bpm_ok else 0

    avg_accel = (round(sum(r["accel_stddev"] for r in rows) / total, 4)
                 if total > 0 else 0)
    avg_gyro  = (round(sum(r["gyro_stddev"]  for r in rows) / total, 2)
                 if total > 0 else 0)

    file_exists = os.path.isfile(SUMMARY_PATH)
    with open(SUMMARY_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        if not file_exists:
            w.writeheader()
        w.writerow({
            "session_no":      state.session_no,
            "participant_no":  participant_no,
            "participant_id":  participant_id,
            "activity":        activity,
            "timestamp":       datetime.now().isoformat(),
            "total_samples":   total,
            "bpm_valid":       len(bpm_ok),
            "bpm_valid_pct":   bpm_pct,
            "avg_accel_stddev": avg_accel,
            "avg_gyro_stddev":  avg_gyro,
            "avg_bpm":          avg_bpm,
            "durasi_detik":     round(duration_actual, 1),
        })
    logger.info(f"Summary sesi #{state.session_no} disimpan → {SUMMARY_PATH}")


# ─────────────────────────────────────────────
#  TAMPILAN TIMER SESI (thread)
# ─────────────────────────────────────────────
def show_session_timer(participant_no: int, participant_id: str, activity: str, duration_sec: int):
    start = time.time()
    while state.session_active and not state.stop_requested:
        elapsed   = time.time() - start
        remaining = max(0, duration_sec - elapsed)
        m, s      = int(remaining // 60), int(remaining % 60)
        n         = len(state.raw_rows)
        bpm_ok    = sum(1 for r in state.raw_rows if int(r.get("bpm", 0)) > 0)
        last_bpm  = state.raw_rows[-1].get("bpm", 0) if state.raw_rows else 0

        print(
            f"\r  ⏱  [{activity}] P{participant_no} [{participant_id}] | "
            f"Sisa: {m:02d}:{s:02d} | "
            f"Sampel: {n:4d} | "
            f"BPM: {last_bpm if last_bpm > 0 else '--':<4}   ",
            end="", flush=True
        )
        time.sleep(0.5)
    print()


# ─────────────────────────────────────────────
#  WATCHDOG TIMER (thread)
#  Hentikan sesi setelah durasi penuh + 5 detik buffer
# ─────────────────────────────────────────────
def session_watchdog():
    time.sleep(SESSION_DURATION + 5)
    if state.session_active:
        logger.info(f"[WATCHDOG] Sesi selesai setelah {SESSION_DURATION}s")
        state.session_active = False
        state.session_done.set()


# ─────────────────────────────────────────────
#  INPUT INTERAKTIF
# ─────────────────────────────────────────────
def input_participant(no: int, completed_ids: list) -> str | None:
    """
    Input ID partisipan.
    Perintah: r=restart, s=skip, q=quit
    """
    print(f"\n{'─'*60}")
    print(f"  👤 PESERTA KE-{no}")
    if completed_ids:
        print(f"  Sebelumnya : {', '.join(completed_ids[-5:])}")
    print(f"{'─'*60}")
    print(f"  Perintah   : [r]estart | [s]kip | [q]uit\n")

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
            print(f"  ⏭  Peserta ke-{no} dilewati.\n")
            return "__SKIP__"
        if lower in ("r", "restart"):
            if do_restart():
                state.restart_flag = True
                return None
            continue
        if not raw:
            print("  ⚠  ID tidak boleh kosong.")
            continue
        if len(raw) > 30:
            print("  ⚠  Terlalu panjang (maks 30 karakter).")
            continue
        safe = raw.replace(" ", "_").replace(",", "").replace('"', "").replace("'", "")
        if safe != raw:
            print(f"  ℹ  ID diubah: '{safe}'")
        return safe


def select_activity(participant_id: str) -> str | None:
    """
    Pilih label aktivitas untuk sesi ini.
    Operator HARUS memilih satu aktivitas sebelum pengambilan data dimulai.
    """
    print()
    print(f"  ┌──────────────────────────────────────────────────┐")
    print(f"  │  🏃 PILIH AKTIVITAS untuk [{participant_id}]        ")
    print(f"  ├──────────────────────────────────────────────────┤")
    print(f"  │  1. DUDUK    — Peserta dalam posisi duduk diam   │")
    print(f"  │  2. BERJALAN — Peserta berjalan kaki             │")
    print(f"  │  3. BERLARI  — Peserta berlari                   │")
    print(f"  │  q. Batal / skip peserta ini                     │")
    print(f"  └──────────────────────────────────────────────────┘")
    print()

    while True:
        try:
            pilih = input("  Pilih aktivitas (1/2/3 atau q): ").strip().upper()
        except (EOFError, KeyboardInterrupt):
            return None

        if pilih in ("1", "DUDUK"):    return "DUDUK"
        if pilih in ("2", "BERJALAN"): return "BERJALAN"
        if pilih in ("3", "BERLARI"):  return "BERLARI"
        if pilih in ("Q", "QUIT"):     return None
        print("  ⚠  Pilihan tidak valid. Masukkan 1, 2, 3, atau q.")


def select_duration(participant_id: str, activity: str) -> int | None:
    """
    Operator memilih durasi sesi sebelum pengambilan data dimulai.
    Mengembalikan durasi dalam DETIK, atau None jika batal.
    """
    print()
    print(f"  ┌──────────────────────────────────────────────────┐")
    print(f"  │  ⏱  PILIH DURASI SESI                           │")
    print(f"  │  Peserta  : [{participant_id}]                   ")
    print(f"  │  Aktivitas: {activity:<37}│")
    print(f"  ├──────────────────────────────────────────────────┤")
    for i, (mnt, _sec) in enumerate(DURATION_OPTIONS, start=1):
        label = f"{mnt} menit ({mnt*60} detik)"
        print(f"  │  {i}. {label:<44}│")
    print(f"  │  q. Batal                                        │")
    print(f"  └──────────────────────────────────────────────────┘")
    print()

    valid = {str(i): sec for i, (_m, sec) in enumerate(DURATION_OPTIONS, start=1)}
    valid_mins = {str(m): sec for m, sec in DURATION_OPTIONS}

    while True:
        try:
            pilih = input(
                f"  Pilih durasi (1–{len(DURATION_OPTIONS)} atau q): "
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            return None

        if pilih in ("q", "quit"):
            return None
        if pilih in valid:
            sec = valid[pilih]
            mnt = sec // 60
            print(f"  ✅ Durasi dipilih: {mnt} menit ({sec} detik)")
            return sec
        print(f"  ⚠  Pilihan tidak valid. Masukkan 1–{len(DURATION_OPTIONS)} atau q.")


def confirm_session(participant_id: str, participant_no: int, activity: str, duration_sec: int) -> bool:
    """Konfirmasi sebelum sesi dimulai."""
    print()
    print(f"  ┌──────────────────────────────────────────────────┐")
    print(f"  │  KONFIRMASI SESI                                 │")
    print(f"  │  Peserta  : P{participant_no} [{participant_id:<20}]│")
    print(f"  │  Aktivitas: {activity:<37}│")
    print(f"  │  Durasi   : {duration_sec//60} menit ({duration_sec} detik){'':>23}│")
    print(f"  │  Broker   : {MQTT_BROKER}:{MQTT_PORT:<25}│")
    print(f"  └──────────────────────────────────────────────────┘")
    print()
    print(f"  ⚠  Pastikan peserta siap melakukan aktivitas: {activity}")
    try:
        ans = input("  Tekan ENTER untuk mulai, atau 'c' untuk batal: ").strip().lower()
        return ans != "c"
    except (EOFError, KeyboardInterrupt):
        return False


# ─────────────────────────────────────────────
#  RINGKASAN SESI
# ─────────────────────────────────────────────
def print_session_summary(participant_id: str, participant_no: int,
                           activity: str, duration: float) -> None:
    rows  = state.raw_rows
    total = len(rows)
    bpm_ok = [int(r.get("bpm", 0)) for r in rows if int(r.get("bpm", 0)) > 0]
    bpm_pct = round(len(bpm_ok) / total * 100, 1) if total > 0 else 0
    avg_bpm = round(sum(bpm_ok) / len(bpm_ok), 1) if bpm_ok else 0
    avg_accel = round(sum(r["accel_stddev"] for r in rows) / total, 4) if total > 0 else 0
    avg_gyro  = round(sum(r["gyro_stddev"]  for r in rows) / total, 2) if total > 0 else 0

    print()
    print(f"  {'═'*54}")
    print(f"  RINGKASAN SESI P{participant_no} [{participant_id}]")
    print(f"  Aktivitas   : {activity} (label manual)")
    print(f"  {'═'*54}")
    print(f"  Total sampel : {total}")
    print(f"  BPM valid    : {len(bpm_ok)} ({bpm_pct}%)")
    print(f"  Rata accel   : {avg_accel:.4f} g")
    print(f"  Rata gyro    : {avg_gyro:.2f} °/s")
    print(f"  Rata BPM     : {avg_bpm:.0f} bpm")
    print(f"  Durasi aktual: {duration:.1f}s ({duration/60:.1f} menit)")
    print(f"  {'─'*54}")

    if total == 0:
        print(f"\n  ❌ Tidak ada data! Periksa:")
        print(f"     • ESP32 menyala dan terhubung WiFi?")
        print(f"     • ESP32 terhubung ke broker: {MQTT_BROKER}?")
        print(f"     • Cek Serial Monitor ESP32")
    else:
        print(f"\n  ✅ Sesi tersimpan. Label: {activity}")
        print(f"     Raw    : {DATA_RAW_DIR}/")
        print(f"     Dataset: {DATASET_PATH}")


# ─────────────────────────────────────────────
#  RESTART (hapus semua data, mulai dari P1)
# ─────────────────────────────────────────────
def do_restart() -> bool:
    print()
    print("  ┌──────────────────────────────────────────────────┐")
    print("  │               ⚠  KONFIRMASI RESTART             │")
    print("  │  Semua data di-BACKUP lalu dihapus. Mulai P1    │")
    print("  └──────────────────────────────────────────────────┘")
    if input("  Ketik YES untuk konfirmasi: ").strip() != "YES":
        print("  ↩  Dibatalkan.\n")
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

    state.session_no = 0
    print(f"\n  ✅ Backup → {dst} ({len(moved)} file)")
    print(f"  🔄 Mulai ulang dari Peserta 1...\n")
    logger.info(f"RESTART: backup ke {dst}")
    return True


# ─────────────────────────────────────────────
#  JALANKAN SATU SESI
# ─────────────────────────────────────────────
def run_session(client, participant_no: int, participant_id: str, activity: str, duration_sec: int) -> bool:
    """
    Jalankan satu sesi pengambilan data.
    activity: label yang dipilih operator secara manual.
    Mengembalikan True jika data berhasil dikumpulkan.
    """
    # Reset state sesi
    state.current_no    = participant_no
    state.current_id    = participant_id
    state.current_label = activity
    state.session_active  = True
    state.stop_requested  = False
    state.session_done.clear()
    state.raw_rows        = []
    state.session_start   = time.time()

    # Kirim START ke ESP32 — sertakan durasi untuk countdown OLED
    send_start(client, participant_id, participant_no, duration=duration_sec)
    time.sleep(1)  # beri waktu ESP32 memproses

    print(f"\n  🟢 Merekam [{activity}] untuk P{participant_no} [{participant_id}]"
          f" selama {duration_sec//60} menit ({duration_sec}s)...")
    print(f"  Tekan Ctrl+C untuk berhenti darurat\n")

    # Thread: tampilan timer
    threading.Thread(
        target=show_session_timer,
        args=(participant_no, participant_id, activity, duration_sec),
        daemon=True
    ).start()

    # Thread: watchdog timer — pakai durasi sesi yang dipilih operator
    def _watchdog():
        time.sleep(duration_sec + 5)
        if state.session_active:
            logger.info(f"[WATCHDOG] Sesi selesai setelah {duration_sec}s")
            state.session_active = False
            state.session_done.set()
    threading.Thread(target=_watchdog, daemon=True).start()

    # Tunggu sampai watchdog selesai
    state.session_done.wait()
    state.session_active = False
    duration = time.time() - state.session_start

    if state.stop_requested:
        print(f"\n  🛑 Mengirim STOP ke ESP32...")

    # Kirim STOP ke ESP32 (bersihkan participant_id)
    send_stop(client, participant_no)
    time.sleep(0.6)

    if not state.raw_rows:
        print(f"\n  ❌ Tidak ada data P{participant_no}. Cek ESP32.\n")
        return False

    # Simpan data
    save_raw_session(participant_id, participant_no, activity)
    append_to_dataset()
    save_summary(participant_no, participant_id, activity, duration)

    print_session_summary(participant_id, participant_no, activity, duration)
    return True


# ─────────────────────────────────────────────
#  RINGKASAN AKHIR
# ─────────────────────────────────────────────
def print_final_summary(completed_count: int) -> None:
    print(f"\n{'═'*62}")
    print(f"  SELESAI — {completed_count} sesi terekam")
    print(f"  Dataset   : {DATASET_PATH}")
    print(f"  Ringkasan : {SUMMARY_PATH}")

    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if rows:
                print(f"\n  {'Sesi':<6} {'No':<5} {'ID':<15} {'Aktivitas':<12}"
                      f" {'Sampel':>7} {'Durasi':>8}")
                print(f"  {'─'*60}")
                for r in rows[-10:]:
                    dur = float(r.get("durasi_detik", 0))
                    print(f"  {r.get('session_no',''):<6} "
                          f"{r['participant_no']:<5} "
                          f"{r['participant_id']:<15} "
                          f"{r['activity']:<12} "
                          f"{r.get('total_samples','')!s:>7} "
                          f"{dur/60:.1f}m")
        except Exception:
            pass

    # Distribusi per kelas
    if os.path.isfile(DATASET_PATH):
        try:
            counts = {}
            with open(DATASET_PATH, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    act = row.get("activity", "")
                    counts[act] = counts.get(act, 0) + 1
            total_ds = sum(counts.values())
            print(f"\n  Distribusi dataset ({total_ds} total baris):")
            for cls in ["DUDUK", "BERJALAN", "BERLARI"]:
                n   = counts.get(cls, 0)
                pct = round(n / total_ds * 100, 1) if total_ds > 0 else 0
                bar = "█" * int(pct / 5)
                print(f"    {cls:<10}: {n:6d} ({pct:5.1f}%) {bar}")
        except Exception:
            pass

    print(f"\n  Langkah berikutnya:")
    print(f"    1. Buka notebook 01_eksplorasi_data.ipynb")
    print(f"    2. Buka notebook 02_training_model.ipynb")
    print(f"    3. python src/server_knn.py")
    print(f"{'═'*62}\n")


# ─────────────────────────────────────────────
#  SIGINT HANDLER
# ─────────────────────────────────────────────
def handle_sigint(sig, frame):
    print("\n\n  Ctrl+C — menghentikan sesi...")
    state.aborted        = True
    state.session_active = False
    state.session_done.set()


# ─────────────────────────────────────────────
#  LOOP UTAMA PENGUMPULAN DATA
# ─────────────────────────────────────────────
def run_collection_loop(client) -> bool:
    """
    Loop utama: iterasi peserta satu per satu.
    Setiap peserta bisa punya lebih dari satu sesi (aktivitas berbeda).
    Mengembalikan True jika harus restart, False jika selesai normal.
    """
    start_from    = 1
    completed_ids = []

    # Lanjutkan dari sesi terakhir jika ada summary
    if os.path.isfile(SUMMARY_PATH):
        try:
            with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if rows:
                state.session_no = max(int(r.get("session_no", 0)) for r in rows)
                last_no          = max(int(r["participant_no"]) for r in rows)
                start_from       = last_no + 1
                completed_ids    = list(dict.fromkeys(r["participant_id"] for r in rows))
                print(f"\n  ℹ  Melanjutkan dari peserta ke-{start_from} "
                      f"({len(rows)} sesi terekam, sesi terakhir #{state.session_no})")
        except Exception:
            pass

    print(f"\n  ✅ MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
    dur_labels = "/".join(str(m) for m, _ in DURATION_OPTIONS)
    print(f"  ℹ  Durasi sesi    : pilihan {dur_labels} menit (dipilih per sesi)")
    print(f"  ℹ  Pelabelan aktivitas: MANUAL oleh operator")
    print(f"  ℹ  Perintah: [r]estart | [s]kip | [q]uit\n")

    input("  Tekan ENTER untuk mulai...")

    completed_count = 0
    no = start_from

    while True:
        if state.aborted:
            break

        # ── Input ID Partisipan ──
        pid = input_participant(no, completed_ids)

        if pid is None:
            if state.restart_flag:
                state.restart_flag = False
                return True
            break
        if pid == "__SKIP__":
            no += 1
            continue

        # ── Sesi untuk partisipan ini ──────────────────────
        # Satu partisipan bisa punya beberapa sesi (aktivitas berbeda)
        session_count_for_participant = 0
        while True:
            if state.aborted:
                break

            # Pilih aktivitas
            activity = select_activity(pid)
            if activity is None:
                print(f"  ↩  Selesai untuk peserta [{pid}].\n")
                break

            # Pilih durasi sesi
            dur_sec = select_duration(pid, activity)
            if dur_sec is None:
                print(f"  ↩  Pemilihan durasi dibatalkan.\n")
                continue

            # Konfirmasi
            if not confirm_session(pid, no, activity, dur_sec):
                print(f"  ↩  Sesi dibatalkan.\n")
                continue

            # Jalankan sesi
            success = run_session(client, no, pid, activity, dur_sec)
            if state.aborted:
                break

            if success:
                session_count_for_participant += 1
                completed_count += 1
                print(f"\n  📊 Total sesi terekam: {completed_count} | "
                      f"Sesi untuk [{pid}]: {session_count_for_participant}")

            if state.restart_flag:
                state.restart_flag = False
                return True

            # Tanya apakah peserta ini punya aktivitas lain
            print()
            try:
                lagi = input(
                    f"  Tambah sesi aktivitas lain untuk [{pid}]? (y/n): "
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                state.aborted = True
                break

            if lagi != "y":
                break

        if state.aborted:
            break

        if pid not in completed_ids:
            completed_ids.append(pid)

        # Jeda sebelum peserta berikutnya
        if not state.aborted:
            print(f"\n  ⏳ Jeda 5 detik sebelum peserta berikutnya...")
            for i in range(5, 0, -1):
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
    dur_labels = "/".join(str(m) for m, _ in DURATION_OPTIONS)
    print(f"║    Durasi pilihan: {dur_labels} menit | Pelabelan: MANUAL  ║")
    print("║    ESP32 hanya kirim data sensor (tanpa label)        ║")
    print("╚═══════════════════════════════════════════════════════╝")

    # Koneksi MQTT
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"\n  Menghubungkan ke {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"\n  ❌ Tidak bisa konek: {e}")
        sys.exit(1)

    client.loop_start()
    time.sleep(2)  # tunggu koneksi stabil

    # Loop utama
    while True:
        state.aborted = state.restart_flag = state.stop_requested = False
        should_restart = run_collection_loop(client)
        if not should_restart:
            break
        print("\n" + "═"*60)
        print("  🔄 RESTART — Mulai dari Peserta 1...")
        print("═"*60 + "\n")

    client.loop_stop()
    client.disconnect()
    print("\n  Selesai. Sampai jumpa! 👋")


if __name__ == "__main__":
    main()