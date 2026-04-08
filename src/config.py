"""
config.py — Konfigurasi terpusat sistem AIoT Watch
Ubah nilai di sini untuk menyesuaikan dengan environment kamu.
"""

import os

# ─────────────────────────────────────────────
#  DIREKTORI
# ─────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
DATA_PROC_DIR  = os.path.join(BASE_DIR, "data", "processed")
DATASET_DIR    = os.path.join(BASE_DIR, "dataset")
MODEL_DIR      = os.path.join(BASE_DIR, "models")
LOG_DIR        = os.path.join(BASE_DIR, "logs")

# Buat direktori jika belum ada
for _d in [DATA_RAW_DIR, DATA_PROC_DIR, DATASET_DIR, MODEL_DIR, LOG_DIR]:
    os.makedirs(_d, exist_ok=True)

# ─────────────────────────────────────────────
#  MQTT
# ─────────────────────────────────────────────
MQTT_BROKER   = "192.168.18.7"
MQTT_PORT     = 1883
MQTT_CLIENT_ID = "python_server_001"

# Topics
TOPIC_SENSOR_DATA    = "sensor/esp32/data"
TOPIC_CLASSIFICATION = "classification/result"
TOPIC_STATUS         = "status/esp32"

# ─────────────────────────────────────────────
#  DATASET & FITUR
# ─────────────────────────────────────────────
FEATURES = ["accel_stddev", "gyro_stddev"]   # fitur input untuk KNN
TARGET   = "activity"                         # label kolom target

# Nama kelas harus konsisten dengan yang dikirim ESP32
CLASSES  = ["DUDUK", "BERJALAN", "BERLARI"]
CLASS_MAP = {label: idx for idx, label in enumerate(CLASSES)}

# ─────────────────────────────────────────────
#  MODEL KNN
# ─────────────────────────────────────────────
K_NEIGHBORS  = 5          # jumlah tetangga; coba 3, 5, 7
KNN_METRIC   = "euclidean"
KNN_WEIGHTS  = "distance" # 'uniform' atau 'distance'

MODEL_PATH   = os.path.join(MODEL_DIR, "knn_model.pkl")
SCALER_PATH  = os.path.join(MODEL_DIR, "scaler.pkl")
DATASET_PATH = os.path.join(DATASET_DIR, "dataset.csv")

# ─────────────────────────────────────────────
#  DATA COLLECTION
# ─────────────────────────────────────────────
# Berapa detik mengumpulkan data per sesi sebelum di-flush ke CSV
COLLECTION_FLUSH_INTERVAL = 30   # detik

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
LOG_FILE  = os.path.join(LOG_DIR, "server.log")
LOG_LEVEL = "INFO"   # DEBUG / INFO / WARNING / ERROR