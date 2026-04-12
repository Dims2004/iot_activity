"""
config.py — Konfigurasi terpusat sistem AIoT Watch
"""
import os

# ─────────────────────────────────────────────
#  DIREKTORI
# ─────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")
DATA_PROC_DIR = os.path.join(BASE_DIR, "data", "processed")
DATASET_DIR   = os.path.join(BASE_DIR, "dataset")
MODEL_DIR     = os.path.join(BASE_DIR, "models")
LOG_DIR       = os.path.join(BASE_DIR, "logs")

for _d in [DATA_RAW_DIR, DATA_PROC_DIR, DATASET_DIR, MODEL_DIR, LOG_DIR]:
    os.makedirs(_d, exist_ok=True)

# ─────────────────────────────────────────────
#  MQTT
# ─────────────────────────────────────────────
MQTT_BROKER    = "10.67.39.220"
MQTT_PORT      = 1883
MQTT_CLIENT_ID = "python_server_001"

TOPIC_SENSOR_DATA    = "sensor/esp32/data"
TOPIC_COMMAND        = "esp32/command"          # ← TAMBAHKAN BARIS INI untuk perintah ke ESP32
TOPIC_CLASSIFICATION = "classification/result"
TOPIC_STATUS         = "status/esp32"

# ─────────────────────────────────────────────
#  PENGAMBILAN DATA
# ─────────────────────────────────────────────
# Durasi sesi labeling & participant (15 menit)
SESSION_DURATION_SEC = 15 * 60   # 900 detik

# ─────────────────────────────────────────────
#  DATASET & FITUR  ← BPM ditambahkan sebagai fitur ke-3
# ─────────────────────────────────────────────
# accel_stddev : std-dev percepatan (g)
# gyro_stddev  : std-dev kecepatan sudut (°/s)
# bpm_filled   : detak jantung (BPM), nilai 0 diimputasi dengan median per kelas
FEATURES = ["accel_stddev", "gyro_stddev", "bpm_filled"]
TARGET   = "activity"

CLASSES   = ["DUDUK", "BERJALAN", "BERLARI"]
CLASS_MAP = {label: idx for idx, label in enumerate(CLASSES)}

# BPM median default per kelas (dipakai saat inference jika BPM=0)
# Akan di-override oleh nilai dari dataset setelah training
BPM_MEDIAN_DEFAULT = {
    "DUDUK":    72,
    "BERJALAN": 95,
    "BERLARI":  145,
}
BPM_GLOBAL_MEDIAN = 90   # fallback jika kelas tidak diketahui

# ─────────────────────────────────────────────
#  MODEL KNN
# ─────────────────────────────────────────────
K_NEIGHBORS = 5
KNN_METRIC  = "euclidean"
KNN_WEIGHTS = "distance"

MODEL_PATH    = os.path.join(MODEL_DIR, "knn_model.pkl")
SCALER_PATH   = os.path.join(MODEL_DIR, "scaler.pkl")
BPM_MED_PATH  = os.path.join(MODEL_DIR, "bpm_medians.pkl")   # simpan median BPM dari training
DATASET_PATH  = os.path.join(DATASET_DIR, "dataset.csv")

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
LOG_FILE  = os.path.join(LOG_DIR, "server.log")
LOG_LEVEL = "INFO"