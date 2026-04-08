"""
server_knn.py — Server KNN: terima data sensor → klasifikasi → publish hasil

Cara pakai:
    python src/server_knn.py

Prasyarat:
    - Model sudah dilatih: models/knn_model.pkl & models/scaler.pkl
    - Broker MQTT aktif di alamat yang ada di config.py
"""

import json
import logging
import os
import sys
import signal
import time
from datetime import datetime

import numpy as np
import joblib
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, TOPIC_CLASSIFICATION, TOPIC_STATUS,
    MODEL_PATH, SCALER_PATH, FEATURES
)
from utils import get_logger, parse_sensor_payload, load_scaler

logger = get_logger("server_knn")


# ═══════════════════════════════════════════════════════════
#  MUAT MODEL & SCALER
# ═══════════════════════════════════════════════════════════
def load_model():
    """Muat model KNN dan scaler. Keluar dengan pesan jelas jika tidak ada."""
    if not os.path.exists(MODEL_PATH):
        logger.error(
            f"Model tidak ditemukan: {MODEL_PATH}\n"
            "  → Latih model dulu dengan notebook 02_training_model.ipynb"
        )
        sys.exit(1)

    if not os.path.exists(SCALER_PATH):
        logger.error(
            f"Scaler tidak ditemukan: {SCALER_PATH}\n"
            "  → Latih model dulu dengan notebook 02_training_model.ipynb"
        )
        sys.exit(1)

    model  = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    logger.info(f"Model  dimuat dari: {MODEL_PATH}")
    logger.info(f"Scaler dimuat dari: {SCALER_PATH}")
    return model, scaler


# ═══════════════════════════════════════════════════════════
#  KLASIFIKASI
# ═══════════════════════════════════════════════════════════
def classify(model, scaler, accel_std: float, gyro_std: float) -> tuple[str, float]:
    """
    Jalankan inferensi KNN.

    Returns:
        activity   : nama kelas ("DUDUK" / "BERJALAN" / "BERLARI")
        confidence : probabilitas kelas pemenang (0–1)
    """
    X = np.array([[accel_std, gyro_std]])
    X_scaled = scaler.transform(X)

    activity = model.predict(X_scaled)[0]

    # Probabilitas (hanya tersedia jika KNN menggunakan weights='distance')
    try:
        proba      = model.predict_proba(X_scaled)[0]
        confidence = float(np.max(proba))
    except AttributeError:
        confidence = 1.0   # fallback jika model tidak support predict_proba

    return str(activity), confidence


# ═══════════════════════════════════════════════════════════
#  STATISTIK RUNTIME
# ═══════════════════════════════════════════════════════════
class Stats:
    def __init__(self):
        self.total      = 0
        self.per_class  = {"DUDUK": 0, "BERJALAN": 0, "BERLARI": 0}
        self.start_time = time.time()

    def update(self, activity: str):
        self.total += 1
        self.per_class[activity] = self.per_class.get(activity, 0) + 1

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        lines = [
            f"  Total prediksi  : {self.total}",
            f"  Uptime          : {elapsed/60:.1f} menit",
        ]
        for cls, cnt in self.per_class.items():
            pct = (cnt / self.total * 100) if self.total > 0 else 0
            lines.append(f"  {cls:<10s}: {cnt:5d} ({pct:.1f}%)")
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
#  MQTT CALLBACKS
# ═══════════════════════════════════════════════════════════
_model  = None
_scaler = None
_stats  = Stats()
_mqtt_client = None


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Terhubung ke broker MQTT {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        logger.info(f"Subscribe ke: {TOPIC_SENSOR_DATA}")
        client.subscribe(TOPIC_STATUS)
        logger.info(f"Subscribe ke: {TOPIC_STATUS}")
    else:
        logger.error(f"Gagal connect, rc={rc}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"Koneksi terputus (rc={rc}), mencoba reconnect...")


def on_message(client, userdata, msg):
    global _model, _scaler, _stats

    topic = msg.topic

    # ── Tangani topic data sensor ──────────────────────────
    if topic == TOPIC_SENSOR_DATA:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except json.JSONDecodeError:
            logger.warning("Payload bukan JSON valid, dilewati.")
            return

        row = parse_sensor_payload(payload)
        if row is None:
            logger.warning(f"Payload gagal divalidasi: {payload}")
            return

        accel_std = row["accel_stddev"]
        gyro_std  = row["gyro_stddev"]
        bpm       = row["bpm"]
        user      = row["user"]

        activity, confidence = classify(_model, _scaler, accel_std, gyro_std)
        _stats.update(activity)

        # Susun respons ke ESP32
        result = {
            "activity":   activity,
            "confidence": round(confidence, 3),
            "bpm":        bpm,
            "user":       user,
            "server_ts":  datetime.now().isoformat()
        }
        client.publish(TOPIC_CLASSIFICATION, json.dumps(result))

        logger.info(
            f"[KNN] {user} → {activity} (conf={confidence:.2f}) | "
            f"aStd={accel_std:.4f} gStd={gyro_std:.2f} BPM={bpm}"
        )

    # ── Tangani topic status (hasil akhir 5 menit) ─────────
    elif topic == TOPIC_STATUS:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            logger.info(
                f"[STATUS AKHIR] User={payload.get('user')} "
                f"Aktivitas={payload.get('final_activity')} "
                f"BPM={payload.get('final_bpm')} "
                f"Duduk={payload.get('count_duduk')} "
                f"Jalan={payload.get('count_berjalan')} "
                f"Lari={payload.get('count_berlari')}"
            )
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════
def main():
    global _model, _scaler, _stats, _mqtt_client

    logger.info("=" * 55)
    logger.info("  AIoT Watch — KNN Inference Server")
    logger.info(f"  Broker: {MQTT_BROKER}:{MQTT_PORT}")
    logger.info("=" * 55)

    _model, _scaler = load_model()
    _stats = Stats()

    client = mqtt.Client(client_id=f"{MQTT_CLIENT_ID}_knn")
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    _mqtt_client = client

    # Reconnect otomatis
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def _shutdown(sig, frame):
        logger.info("\n[STOP] Server dihentikan. Ringkasan:\n" + _stats.summary())
        client.loop_stop()
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"Tidak bisa konek ke broker: {e}")
        sys.exit(1)

    logger.info("Server aktif. Tekan Ctrl+C untuk berhenti.")
    client.loop_start()

    # Loop utama: cetak stats tiap 60 detik
    try:
        while True:
            time.sleep(60)
            logger.info("=== STATS ===\n" + _stats.summary())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()