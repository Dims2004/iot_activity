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
#  MQTT - CLOUD BROKER (EMQX Public)
# ─────────────────────────────────────────────
# EMQX Public Broker (gratis, tanpa registrasi)
# Bisa diakses dari mana saja selama ada internet
MQTT_BROKER    = "broker.emqx.io"
MQTT_PORT      = 1883
MQTT_CLIENT_ID = "python_server_001"

# Untuk WebSocket (dashboard)
MQTT_WS_PORT   = 8083  # WebSocket port untuk EMQX
MQTT_WS_PATH   = "/mqtt"

# Optional: jika menggunakan broker dengan autentikasi
# MQTT_USERNAME = ""
# MQTT_PASSWORD = ""

TOPIC_SENSOR_DATA    = "aiot/sensor/data"
TOPIC_COMMAND        = "aiot/command"
TOPIC_CLASSIFICATION = "aiot/classification/result"
TOPIC_STATUS         = "aiot/status"

# ─────────────────────────────────────────────
#  PENGAMBILAN DATA
# ─────────────────────────────────────────────
SESSION_DURATION_SEC = 15 * 60   # 900 detik

# ─────────────────────────────────────────────
#  DATASET & FITUR
# ─────────────────────────────────────────────
FEATURES = ["accel_stddev", "gyro_stddev", "bpm_filled"]
TARGET   = "activity"

CLASSES   = ["DUDUK", "BERJALAN", "BERLARI"]
CLASS_MAP = {label: idx for idx, label in enumerate(CLASSES)}

# BPM median default per kelas
BPM_MEDIAN_DEFAULT = {
    "DUDUK":    72,
    "BERJALAN": 95,
    "BERLARI":  145,
}
BPM_GLOBAL_MEDIAN = 90

# ─────────────────────────────────────────────
#  MODEL KNN
# ─────────────────────────────────────────────
K_NEIGHBORS = 5
KNN_METRIC  = "euclidean"
KNN_WEIGHTS = "distance"

MODEL_PATH    = os.path.join(MODEL_DIR, "knn_model.pkl")
SCALER_PATH   = os.path.join(MODEL_DIR, "scaler.pkl")
BPM_MED_PATH  = os.path.join(MODEL_DIR, "bpm_medians.pkl")
DATASET_PATH  = os.path.join(DATASET_DIR, "dataset.csv")

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
LOG_FILE  = os.path.join(LOG_DIR, "server.log")
LOG_LEVEL = "INFO"