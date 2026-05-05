"""
test_realtime.py — Uji Coba Real-Time KNN (TANPA menyimpan data apapun)
========================================================================
Alur:
  1. Pastikan server_knn.py sudah berjalan di terminal lain
  2. Jalankan: python src/test_realtime.py
  3. Input ID subjek uji (bebas, hanya untuk identifikasi tampilan)
  4. Pilih durasi sesi (5 / 10 / 15 menit)
  5. ESP32 mengirim sensor → server_knn mengklasifikasi → hasil tampil di sini
  6. Pantau juga di dashboard web (index.html)
  7. Sesi selesai → tampil ringkasan akurasi (dibandingkan label asli jika diisi)
  8. TIDAK ada file yang disimpan ke disk

Prasyarat:
  - server_knn.py aktif (models/ harus sudah ada)
  - ESP32 terhubung ke broker MQTT
  - Dashboard web (index.html) bisa dibuka bersamaan untuk monitoring visual
"""

import json, os, sys, signal, time, threading
from datetime import datetime
from collections import Counter

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT,
    TOPIC_SENSOR_DATA, TOPIC_COMMAND, TOPIC_CLASSIFICATION,
)
from utils import get_logger

logger = get_logger("test_realtime")

# ─────────────────────────────────────────────
#  PILIHAN DURASI SESI
# ─────────────────────────────────────────────
DURATION_OPTIONS = [
    (15, 15 * 60),
    (10, 10 * 60),
    ( 5,  5 * 60),
]

# ─────────────────────────────────────────────
#  WARNA TERMINAL (ANSI)
# ─────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    BLUE   = "\033[94m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"

ACTIVITY_COLOR = {
    "DUDUK":    C.BLUE,
    "BERJALAN": C.GREEN,
    "BERLARI":  C.RED,
}
ACTIVITY_ICON = {
    "DUDUK":    "🪑",
    "BERJALAN": "🚶",
    "BERLARI":  "🏃",
}

# ─────────────────────────────────────────────
#  STATE SESI (hanya di memori, tidak disimpan)
# ─────────────────────────────────────────────
class SessionState:
    def __init__(self):
        self.subject_id      = ""
        self.duration_sec    = 0
        self.session_active  = False
        self.session_done    = threading.Event()
        self.aborted         = False

        # Statistik real-time (tidak disimpan ke file)
        self.total_sensor    = 0      # jumlah paket sensor diterima
        self.total_knn       = 0      # jumlah hasil KNN diterima
        self.knn_counts      = Counter()     # {"DUDUK": N, "BERJALAN": N, "BERLARI": N}
        self.confidences     = []            # list float confidence
        self.bpm_readings    = []            # list BPM valid (>0)
        self.accel_readings  = []
        self.gyro_readings   = []

        # Label asli (opsional, untuk hitung akurasi manual)
        self.true_label      = ""

        # Tracking waktu
        self.session_start   = 0.0
        self.last_knn_time   = 0.0
        self.last_activity   = ""
        self.last_confidence = 0.0
        self.last_bpm        = 0

state = SessionState()

# ─────────────────────────────────────────────
#  MQTT CALLBACKS
# ─────────────────────────────────────────────
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"MQTT terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        client.subscribe(TOPIC_CLASSIFICATION)
        print(f"\n  {C.GREEN}✅ Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}{C.RESET}")
    else:
        print(f"\n  {C.RED}❌ Gagal terhubung MQTT (rc={rc}){C.RESET}")


def on_disconnect(client, userdata, rc, properties=None):
    if rc != 0:
        logger.warning(f"Terputus dari broker (rc={rc})")


def on_message(client, userdata, msg):
    if not state.session_active:
        return

    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    # ── Paket sensor mentah dari ESP32 ──────────────────────
    if msg.topic == TOPIC_SENSOR_DATA:
        state.total_sensor += 1
        bpm = int(data.get("bpm", 0))
        if bpm > 0:
            state.bpm_readings.append(bpm)
            state.last_bpm = bpm
        accel = float(data.get("accel_stddev", 0))
        gyro  = float(data.get("gyro_stddev", 0))
        if accel > 0:
            state.accel_readings.append(accel)
        if gyro > 0:
            state.gyro_readings.append(gyro)

    # ── Hasil klasifikasi dari server_knn ───────────────────
    elif msg.topic == TOPIC_CLASSIFICATION:
        activity   = (data.get("activity", "")).upper()
        confidence = float(data.get("confidence", 0))
        bpm        = int(data.get("bpm", 0))

        if activity not in ("DUDUK", "BERJALAN", "BERLARI"):
            return

        state.total_knn       += 1
        state.knn_counts[activity] += 1
        state.confidences.append(confidence)
        state.last_activity   = activity
        state.last_confidence = confidence
        state.last_knn_time   = time.time()
        if bpm > 0:
            state.last_bpm = bpm


# ─────────────────────────────────────────────
#  KIRIM START / STOP KE ESP32
# ─────────────────────────────────────────────
def send_start(client, subject_id: str, duration_sec: int):
    payload = json.dumps({
        "cmd":            "START",
        "participant_id": subject_id,
        "participant_no": 0,         # 0 = sesi uji, bukan data training
        "duration":       duration_sec,
    })
    try:
        r = client.publish(TOPIC_COMMAND, payload, qos=1)
        r.wait_for_publish(timeout=3)
        logger.info(f"START uji → [{subject_id}] durasi={duration_sec}s")
    except Exception as e:
        logger.error(f"Gagal kirim START: {e}")


def send_stop(client):
    payload = json.dumps({"cmd": "STOP"})
    try:
        r = client.publish(TOPIC_COMMAND, payload, qos=1)
        r.wait_for_publish(timeout=3)
        logger.info("STOP uji dikirim")
    except Exception as e:
        logger.error(f"Gagal kirim STOP: {e}")


# ─────────────────────────────────────────────
#  DISPLAY LIVE (thread)
# ─────────────────────────────────────────────
_KNN_WARN_AFTER = 8   # detik tanpa KNN → tampilkan peringatan

def show_live_display():
    """
    Thread: refresh tampilan terminal setiap 0.5 detik.
    Menampilkan hasil KNN real-time + countdown waktu.
    """
    spin = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
    tick = 0

    while state.session_active and not state.aborted:
        elapsed   = time.time() - state.session_start
        remaining = max(0.0, state.duration_sec - elapsed)
        rm = int(remaining // 60)
        rs = int(remaining % 60)

        # Dominan sementara
        dominant = state.knn_counts.most_common(1)
        dom_act  = dominant[0][0] if dominant else "--"
        dom_col  = ACTIVITY_COLOR.get(dom_act, C.WHITE)
        dom_icon = ACTIVITY_ICON.get(dom_act, "❓")

        # Confidence rata-rata
        avg_conf = (sum(state.confidences) / len(state.confidences) * 100
                    if state.confidences else 0)

        # Status KNN
        time_since_knn = time.time() - state.last_knn_time
        knn_status = (f"{C.GREEN}● KNN aktif{C.RESET}"
                      if time_since_knn < _KNN_WARN_AFTER
                      else f"{C.YELLOW}⚠ Menunggu KNN...{C.RESET}")

        # Aktifitas terakhir
        last_col  = ACTIVITY_COLOR.get(state.last_activity, C.DIM)
        last_icon = ACTIVITY_ICON.get(state.last_activity, "  ")

        # Bar distribusi KNN
        total_knn = max(state.total_knn, 1)
        bars = {}
        for act in ["DUDUK", "BERJALAN", "BERLARI"]:
            n    = state.knn_counts.get(act, 0)
            pct  = n / total_knn * 100
            fill = int(pct / 5)          # max 20 blok
            col  = ACTIVITY_COLOR.get(act, C.WHITE)
            bars[act] = (n, pct, col, "█" * fill + "░" * (20 - fill))

        # Rata-rata BPM
        avg_bpm = (sum(state.bpm_readings) / len(state.bpm_readings)
                   if state.bpm_readings else 0)

        spinner = spin[tick % len(spin)]
        tick   += 1

        # ── Clear + Print ─────────────────────────────────────
        lines = []
        lines.append(f"\033[2J\033[H")  # clear screen
        lines.append(f"{C.BOLD}{'─'*62}{C.RESET}")
        lines.append(f"  {C.CYAN}{C.BOLD}AIoT Watch — Testing Real-Time KNN{C.RESET}  {knn_status}")
        lines.append(f"  Subjek : {C.BOLD}{state.subject_id}{C.RESET}   "
                     f"Sensor: {C.GREEN}{state.total_sensor}{C.RESET} paket   "
                     f"KNN: {C.GREEN}{state.total_knn}{C.RESET} hasil")
        lines.append(f"  Broker : {MQTT_BROKER}:{MQTT_PORT}   "
                     f"{C.DIM}(pantau di dashboard web){C.RESET}")
        lines.append(f"{C.BOLD}{'─'*62}{C.RESET}")
        lines.append("")

        # Countdown besar
        lines.append(f"  ⏱  {C.BOLD}Sisa Waktu:{C.RESET}  "
                     f"{C.YELLOW}{C.BOLD}{rm:02d}:{rs:02d}{C.RESET}  "
                     f"{C.DIM}/ {state.duration_sec//60:02d}:00{C.RESET}  "
                     f"  {spinner}")

        # Progress bar waktu
        elapsed_pct = min(1.0, elapsed / state.duration_sec)
        prog_fill   = int(elapsed_pct * 40)
        prog_bar    = "█" * prog_fill + "░" * (40 - prog_fill)
        lines.append(f"  [{C.BLUE}{prog_bar}{C.RESET}] {elapsed_pct*100:.0f}%")
        lines.append("")

        # Hasil KNN terakhir
        lines.append(f"  {C.BOLD}Klasifikasi Terakhir:{C.RESET}")
        if state.last_activity:
            lines.append(f"    {last_icon}  {last_col}{C.BOLD}{state.last_activity:<10}{C.RESET}  "
                         f"conf {C.BOLD}{state.last_confidence*100:.0f}%{C.RESET}  "
                         f"BPM {C.BOLD}{state.last_bpm if state.last_bpm > 0 else '--'}{C.RESET}")
        else:
            lines.append(f"    {C.DIM}Menunggu hasil KNN dari server_knn.py...{C.RESET}")
        lines.append("")

        # Distribusi aktivitas sementara
        lines.append(f"  {C.BOLD}Distribusi Aktivitas (sesi ini):{C.RESET}")
        for act in ["DUDUK", "BERJALAN", "BERLARI"]:
            n, pct, col, bar = bars[act]
            icon = ACTIVITY_ICON[act]
            lines.append(f"    {icon} {col}{act:<10}{C.RESET}  "
                         f"{bar}  {pct:5.1f}%  ({n})")
        lines.append("")

        # Statistik ringkas
        lines.append(f"  {C.DIM}Avg Conf: {avg_conf:.0f}%   "
                     f"Avg BPM: {avg_bpm:.0f}   "
                     f"Dominan: {dom_col}{dom_icon} {dom_act}{C.RESET}")

        if state.true_label:
            dom_correct = dom_act == state.true_label.upper()
            mark = f"{C.GREEN}✅" if dom_correct else f"{C.YELLOW}⚠"
            lines.append(f"  Label asli: {C.BOLD}{state.true_label.upper()}{C.RESET}  "
                         f"{mark} {'SESUAI' if dom_correct else 'BERBEDA'}{C.RESET}")

        lines.append("")
        lines.append(f"  {C.DIM}Ctrl+C untuk berhenti{C.RESET}")
        lines.append(f"{C.BOLD}{'─'*62}{C.RESET}")

        print("\n".join(lines), end="", flush=True)
        time.sleep(0.5)


# ─────────────────────────────────────────────
#  RINGKASAN AKHIR SESI
# ─────────────────────────────────────────────
def print_session_summary():
    print(f"\033[2J\033[H")  # clear
    elapsed = time.time() - state.session_start

    total   = state.total_knn
    counts  = state.knn_counts
    dominant = counts.most_common(1)[0][0] if counts else "--"

    avg_conf = (sum(state.confidences) / len(state.confidences) * 100
                if state.confidences else 0)
    avg_bpm  = (sum(state.bpm_readings) / len(state.bpm_readings)
                if state.bpm_readings else 0)
    avg_accel = (sum(state.accel_readings) / len(state.accel_readings)
                 if state.accel_readings else 0)
    avg_gyro  = (sum(state.gyro_readings) / len(state.gyro_readings)
                 if state.gyro_readings else 0)

    print(f"{C.BOLD}{'═'*62}{C.RESET}")
    print(f"  {C.CYAN}{C.BOLD}RINGKASAN SESI UJI — {state.subject_id}{C.RESET}")
    print(f"  {C.DIM}(tidak ada data yang disimpan ke disk){C.RESET}")
    print(f"{C.BOLD}{'═'*62}{C.RESET}")
    print(f"  Durasi aktual  : {elapsed:.1f}s ({elapsed/60:.1f} menit)")
    print(f"  Paket sensor   : {state.total_sensor}")
    print(f"  Hasil KNN      : {total}")
    print(f"  Avg confidence : {avg_conf:.1f}%")
    print(f"  Avg BPM        : {avg_bpm:.0f} bpm")
    print(f"  Avg accel std  : {avg_accel:.4f} g")
    print(f"  Avg gyro std   : {avg_gyro:.2f} °/s")
    print(f"\n  {C.BOLD}Distribusi Klasifikasi:{C.RESET}")
    print(f"  {'─'*40}")

    dom_col = ACTIVITY_COLOR.get(dominant, C.WHITE)

    for act in ["DUDUK", "BERJALAN", "BERLARI"]:
        n    = counts.get(act, 0)
        pct  = n / total * 100 if total > 0 else 0
        col  = ACTIVITY_COLOR.get(act, C.WHITE)
        icon = ACTIVITY_ICON[act]
        mark = f" {C.BOLD}← DOMINAN{C.RESET}" if act == dominant else ""
        print(f"  {icon} {col}{act:<10}{C.RESET}  {n:5d} ({pct:5.1f}%){mark}")

    print(f"\n  {C.BOLD}Hasil Dominan : "
          f"{dom_col}{ACTIVITY_ICON.get(dominant,'')} {dominant}{C.RESET}")

    # Akurasi jika label asli diisi
    if state.true_label:
        correct = dominant == state.true_label.upper()
        pct_correct = (counts.get(state.true_label.upper(), 0) / total * 100
                       if total > 0 else 0)
        mark = f"{C.GREEN}✅ BENAR" if correct else f"{C.RED}❌ SALAH"
        print(f"  Label Asli    : {C.BOLD}{state.true_label.upper()}{C.RESET}")
        print(f"  Prediksi Dom. : {mark}{C.RESET}")
        print(f"  % sampel benar: {C.BOLD}{pct_correct:.1f}%{C.RESET} "
              f"({counts.get(state.true_label.upper(),0)}/{total})")

    print(f"\n{C.BOLD}{'═'*62}{C.RESET}\n")


# ─────────────────────────────────────────────
#  CEK SERVER_KNN AKTIF
# ─────────────────────────────────────────────
def check_knn_server(client, timeout: int = 8) -> bool:
    """
    Tunggu hingga classification/result masuk dalam `timeout` detik.
    Jika tidak ada → kemungkinan server_knn belum aktif.
    """
    print(f"\n  {C.DIM}Menunggu konfirmasi server_knn aktif "
          f"(maks {timeout} detik)...{C.RESET}", flush=True)

    deadline = time.time() + timeout
    while time.time() < deadline:
        if state.total_knn > 0:
            print(f"  {C.GREEN}✅ server_knn aktif — hasil KNN diterima!{C.RESET}\n")
            return True
        time.sleep(0.3)

    print(f"\n  {C.YELLOW}⚠  Belum ada hasil KNN dalam {timeout} detik.{C.RESET}")
    print(f"  Pastikan {C.BOLD}server_knn.py{C.RESET} sudah berjalan:")
    print(f"    python src/server_knn.py\n")
    ans = input("  Lanjut tetap? (y/n): ").strip().lower()
    return ans == "y"


# ─────────────────────────────────────────────
#  INPUT INTERAKTIF
# ─────────────────────────────────────────────
def input_subject() -> str | None:
    print(f"\n{C.BOLD}{'─'*62}{C.RESET}")
    print(f"  {C.CYAN}{C.BOLD}SESI UJI REAL-TIME — Tidak Menyimpan Data{C.RESET}")
    print(f"{'─'*62}")
    print(f"  {C.DIM}Perintah: q = keluar{C.RESET}\n")
    try:
        raw = input("  ID Subjek Uji (bebas, misal: UJI_001): ").strip()
    except (EOFError, KeyboardInterrupt):
        return None
    if raw.lower() in ("q", "quit", ""):
        return None
    return raw.replace(" ", "_")[:30]


def select_duration() -> int | None:
    print()
    print(f"  ┌──────────────────────────────────────────────────┐")
    print(f"  │  ⏱  PILIH DURASI SESI UJI                       │")
    print(f"  ├──────────────────────────────────────────────────┤")
    for i, (mnt, sec) in enumerate(DURATION_OPTIONS, start=1):
        print(f"  │  {i}. {mnt} menit ({sec} detik){'':>33}│")
    print(f"  │  q. Batal{'':>41}│")
    print(f"  └──────────────────────────────────────────────────┘\n")
    valid = {str(i): sec for i, (_, sec) in enumerate(DURATION_OPTIONS, start=1)}
    while True:
        try:
            pilih = input(f"  Pilih (1–{len(DURATION_OPTIONS)}/q): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            return None
        if pilih in ("q", "quit"):
            return None
        if pilih in valid:
            sec = valid[pilih]
            print(f"  ✅ Durasi: {sec//60} menit\n")
            return sec
        print(f"  ⚠  Masukkan 1–{len(DURATION_OPTIONS)} atau q.")


def input_true_label() -> str:
    """
    Opsional: operator bisa isi label asli aktivitas untuk
    menghitung akurasi di ringkasan akhir.
    Kosongkan jika tidak ingin membandingkan.
    """
    print(f"  {C.DIM}(Opsional) Isi label aktivitas asli untuk mengecek akurasi:")
    print(f"  1=DUDUK  2=BERJALAN  3=BERLARI  Enter=kosongkan{C.RESET}")
    try:
        pilih = input("  Label asli: ").strip()
    except (EOFError, KeyboardInterrupt):
        return ""
    mapping = {"1": "DUDUK", "2": "BERJALAN", "3": "BERLARI",
               "duduk": "DUDUK", "berjalan": "BERJALAN", "berlari": "BERLARI"}
    return mapping.get(pilih.lower(), "")


# ─────────────────────────────────────────────
#  JALANKAN SATU SESI UJI
# ─────────────────────────────────────────────
def run_test_session(client, subject_id: str, duration_sec: int, true_label: str):
    # Reset state (hanya di memori)
    state.subject_id      = subject_id
    state.duration_sec    = duration_sec
    state.true_label      = true_label
    state.session_active  = True
    state.aborted         = False
    state.session_done.clear()
    state.total_sensor    = 0
    state.total_knn       = 0
    state.knn_counts      = Counter()
    state.confidences     = []
    state.bpm_readings    = []
    state.accel_readings  = []
    state.gyro_readings   = []
    state.last_activity   = ""
    state.last_confidence = 0.0
    state.last_bpm        = 0
    state.last_knn_time   = time.time()
    state.session_start   = time.time()

    # Kirim START ke ESP32
    send_start(client, subject_id, duration_sec)
    time.sleep(0.5)

    # Cek apakah server_knn aktif (tunggu sampel pertama masuk)
    if not check_knn_server(client, timeout=8):
        state.session_active = False
        send_stop(client)
        return

    # Thread: live display
    t_display = threading.Thread(target=show_live_display, daemon=True)
    t_display.start()

    # Thread: watchdog timer
    def _watchdog():
        time.sleep(duration_sec + 3)
        if state.session_active:
            state.session_active = False
            state.session_done.set()

    threading.Thread(target=_watchdog, daemon=True).start()

    # Tunggu selesai
    state.session_done.wait()
    state.session_active = False

    # Kirim STOP ke ESP32
    send_stop(client)
    time.sleep(0.5)

    # Tampilkan ringkasan — tidak ada yang disimpan ke disk
    print_session_summary()


# ─────────────────────────────────────────────
#  SIGINT HANDLER
# ─────────────────────────────────────────────
def handle_sigint(sig, frame):
    print(f"\n\n  {C.YELLOW}Ctrl+C — menghentikan sesi...{C.RESET}")
    state.aborted        = True
    state.session_active = False
    state.session_done.set()


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    signal.signal(signal.SIGINT, handle_sigint)

    print(f"\033[2J\033[H")  # clear
    print(f"{C.BOLD}{'╔'+'═'*60+'╗'}{C.RESET}")
    print(f"{C.BOLD}║{C.CYAN}  AIoT Watch — Testing Real-Time KNN{C.RESET}{C.BOLD}{'':>24}║{C.RESET}")
    print(f"{C.BOLD}║{C.DIM}  Tidak ada data yang disimpan ke disk{C.RESET}{C.BOLD}{'':>22}║{C.RESET}")
    print(f"{C.BOLD}║{C.DIM}  Pantau juga di: web/index.html{C.RESET}{C.BOLD}{'':>28}║{C.RESET}")
    print(f"{C.BOLD}{'╚'+'═'*60+'╝'}{C.RESET}")
    print()
    print(f"  {C.BOLD}Prasyarat sebelum mulai:{C.RESET}")
    print(f"  {C.GREEN}✅{C.RESET} ESP32 menyala & terhubung WiFi")
    print(f"  {C.YELLOW}→{C.RESET}  {C.BOLD}server_knn.py harus sudah berjalan{C.RESET} di terminal lain:")
    print(f"     {C.DIM}python src/server_knn.py{C.RESET}")
    print(f"  {C.YELLOW}→{C.RESET}  Dashboard web bisa dibuka bersamaan (opsional)")
    print()

    # Koneksi MQTT
    client = mqtt.Client(
        client_id=f"test_realtime_{int(time.time())}",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    print(f"  Menghubungkan ke {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"  {C.RED}❌ Tidak bisa konek MQTT: {e}{C.RESET}")
        sys.exit(1)

    client.loop_start()
    time.sleep(2)

    # ── Loop sesi uji ──────────────────────────────────────
    while True:
        state.aborted = False

        subject_id = input_subject()
        if subject_id is None:
            break

        duration_sec = select_duration()
        if duration_sec is None:
            continue

        true_label = input_true_label()

        # Konfirmasi
        print()
        print(f"  ┌──────────────────────────────────────────────────┐")
        print(f"  │  KONFIRMASI SESI UJI                             │")
        print(f"  │  Subjek   : {subject_id:<38}│")
        print(f"  │  Durasi   : {duration_sec//60} menit ({duration_sec} detik){'':>26}│")
        label_str = true_label if true_label else "(tidak diisi)"
        print(f"  │  Label asli: {label_str:<37}│")
        print(f"  │  Simpan data: TIDAK{'':>30}│")
        print(f"  └──────────────────────────────────────────────────┘")
        print()
        try:
            ans = input("  Tekan ENTER untuk mulai, 'c' untuk batal: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break
        if ans == "c":
            continue

        run_test_session(client, subject_id, duration_sec, true_label)

        if state.aborted:
            try:
                ans = input("\n  Lanjut sesi uji baru? (y/n): ").strip().lower()
                if ans != "y":
                    break
            except (EOFError, KeyboardInterrupt):
                break
            continue

        try:
            ans = input("\n  Uji subjek lain? (y/n): ").strip().lower()
            if ans != "y":
                break
        except (EOFError, KeyboardInterrupt):
            break

    client.loop_stop()
    client.disconnect()
    print(f"\n  {C.DIM}Selesai. Tidak ada file yang disimpan.{C.RESET}\n")


if __name__ == "__main__":
    main()