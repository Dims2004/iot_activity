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
MQTT_BROKER    = "broker.emqx.io"
MQTT_PORT      = 1883
MQTT_CLIENT_ID = "python_server_001"
MQTT_KEEPALIVE = 60  # Ditambahkan

# WebSocket port untuk dashboard
MQTT_WS_PORT   = 8083
MQTT_WS_PATH   = "/mqtt"

TOPIC_SENSOR_DATA    = "sensor/esp32/data"
TOPIC_COMMAND        = "control/session"
TOPIC_CLASSIFICATION = "classification/result"
TOPIC_STATUS         = "status/esp32"

# ─────────────────────────────────────────────
#  PENGAMBILAN DATA
# ─────────────────────────────────────────────
SESSION_DURATION_SEC = 15 * 60

# ─────────────────────────────────────────────
#  DATASET & FITUR
# ─────────────────────────────────────────────
FEATURES = ["accel_stddev", "gyro_stddev", "bpm_filled"]
TARGET   = "activity"

CLASSES   = ["DUDUK", "BERJALAN", "BERLARI"]
CLASS_MAP = {label: idx for idx, label in enumerate(CLASSES)}

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