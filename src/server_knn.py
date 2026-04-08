"""
server_knn.py — Server KNN: terima sensor → klasifikasi → publish hasil

Fitur: accel_stddev, gyro_stddev, bpm_filled (3 fitur dengan BPM)
"""
import json, os, sys, signal, time
from datetime import datetime

import numpy as np
import joblib
import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, TOPIC_CLASSIFICATION, TOPIC_STATUS,
    MODEL_PATH, SCALER_PATH, BPM_MED_PATH, FEATURES
)
from utils import get_logger, parse_sensor_payload, impute_bpm_single, load_bpm_medians

logger = get_logger("server_knn")

# ═══════════════════════════════════════════════════════════
#  LOAD MODEL, SCALER, BPM MEDIANS
# ═══════════════════════════════════════════════════════════
def load_model():
    for path, name in [(MODEL_PATH, "Model"), (SCALER_PATH, "Scaler")]:
        if not os.path.exists(path):
            logger.error(f"{name} tidak ditemukan: {path}")
            logger.error("→ Latih model dulu dengan notebook 02_training_model.ipynb")
            sys.exit(1)
    model       = joblib.load(MODEL_PATH)
    scaler      = joblib.load(SCALER_PATH)
    bpm_medians = load_bpm_medians()
    logger.info(f"Model  : {MODEL_PATH}")
    logger.info(f"Scaler : {SCALER_PATH}")
    logger.info(f"BPM median: {bpm_medians}")
    logger.info(f"Fitur  : {FEATURES}")
    return model, scaler, bpm_medians

# ═══════════════════════════════════════════════════════════
#  KLASIFIKASI — 3 FITUR: accel, gyro, bpm
# ═══════════════════════════════════════════════════════════
def classify(model, scaler, bpm_medians,
             accel_std: float, gyro_std: float, bpm: int
             ) -> tuple[str, float]:
    """
    Jalankan inferensi KNN dengan 3 fitur.
    BPM=0 diimputasi dengan median dari training data.
    """
    # Imputasi BPM=0 (sensor tidak terbaca) dengan median global
    bpm_filled = impute_bpm_single(bpm, bpm_medians=bpm_medians)

    X = np.array([[accel_std, gyro_std, bpm_filled]])
    X_scaled = scaler.transform(X)

    activity = model.predict(X_scaled)[0]

    try:
        proba      = model.predict_proba(X_scaled)[0]
        confidence = float(np.max(proba))
    except AttributeError:
        confidence = 1.0

    return str(activity), confidence, float(bpm_filled)

# ═══════════════════════════════════════════════════════════
#  STATS RUNTIME
# ═══════════════════════════════════════════════════════════
class Stats:
    def __init__(self):
        self.total     = 0
        self.per_class = {"DUDUK": 0, "BERJALAN": 0, "BERLARI": 0}
        self.start     = time.time()

    def update(self, activity: str):
        self.total += 1
        self.per_class[activity] = self.per_class.get(activity, 0) + 1

    def summary(self) -> str:
        elapsed = time.time() - self.start
        lines = [f"  Total prediksi: {self.total} | Uptime: {elapsed/60:.1f} menit"]
        for cls, cnt in self.per_class.items():
            pct = (cnt / self.total * 100) if self.total > 0 else 0
            lines.append(f"  {cls:<10}: {cnt:5d} ({pct:.1f}%)")
        return "\n".join(lines)

# ═══════════════════════════════════════════════════════════
#  MQTT
# ═══════════════════════════════════════════════════════════
_model  = None
_scaler = None
_bpm_medians = None
_stats  = Stats()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Terhubung ke broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(TOPIC_SENSOR_DATA)
        client.subscribe(TOPIC_STATUS)
    else:
        logger.error(f"Gagal connect rc={rc}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"Terputus (rc={rc}), reconnecting...")

def on_message(client, userdata, msg):
    global _model, _scaler, _bpm_medians, _stats

    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return

    if topic == TOPIC_SENSOR_DATA:
        row = parse_sensor_payload(payload)
        if row is None:
            logger.warning(f"Payload tidak valid: {payload}")
            return

        accel_std  = row["accel_stddev"]
        gyro_std   = row["gyro_stddev"]
        bpm        = row["bpm"]
        user       = row.get("participant_id") or row.get("user", "unknown")

        activity, confidence, bpm_used = classify(
            _model, _scaler, _bpm_medians,
            accel_std, gyro_std, bpm
        )
        _stats.update(activity)

        result = {
            "activity":    activity,
            "confidence":  round(confidence, 3),
            "bpm":         bpm if bpm > 0 else int(bpm_used),
            "bpm_filled":  int(bpm_used),      # nilai setelah imputasi
            "bpm_raw":     bpm,                 # nilai asli dari sensor
            "user":        user,
            "server_ts":   datetime.now().isoformat()
        }
        client.publish(TOPIC_CLASSIFICATION, json.dumps(result))

        logger.info(
            f"[KNN] {user} → {activity} (conf={confidence:.2f}) | "
            f"aStd={accel_std:.4f} gStd={gyro_std:.2f} "
            f"BPM={bpm}(raw) {int(bpm_used)}(filled)"
        )

    elif topic == TOPIC_STATUS:
        logger.info(
            f"[STATUS] user={payload.get('participant_id')} "
            f"akt={payload.get('final_activity')} "
            f"bpm={payload.get('final_bpm')}"
        )

def main():
    global _model, _scaler, _bpm_medians, _stats

    logger.info("=" * 55)
    logger.info("  AIoT Watch — KNN Inference Server (3 Fitur)")
    logger.info(f"  Fitur: {FEATURES}")
    logger.info(f"  Broker: {MQTT_BROKER}:{MQTT_PORT}")
    logger.info("=" * 55)

    _model, _scaler, _bpm_medians = load_model()
    _stats = Stats()

    client = mqtt.Client(client_id=f"{MQTT_CLIENT_ID}_knn")
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def _shutdown(sig, frame):
        logger.info("\n[STOP] Server berhenti.\n" + _stats.summary())
        client.loop_stop(); client.disconnect(); sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"Tidak bisa konek: {e}"); sys.exit(1)

    logger.info("Server aktif. Tekan Ctrl+C untuk berhenti.")
    client.loop_start()

    try:
        while True:
            time.sleep(60)
            logger.info("=== STATS ===\n" + _stats.summary())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()