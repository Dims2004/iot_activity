# =============================================================================
#  test_mqtt.py
#  Alat uji koneksi MQTT dan alur pesan antar komponen.
#
#  Mode penggunaan:
#   python test_mqtt.py --mode ping          # tes koneksi broker
#   python test_mqtt.py --mode listen        # pantau semua topic
#   python test_mqtt.py --mode simulate      # simulasi data ESP32
#   python test_mqtt.py --mode full          # tes end-to-end lengkap
# =============================================================================

import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    MQTT_CLIENT_TEST,
    TOPIC_SENSOR_DATA,
    TOPIC_CLASSIFICATION,
    TOPIC_STATUS,
    ACTIVITY_LABELS,
)
from utils import setup_logger

logger = setup_logger("test_mqtt")

# =============================================================================
#  GENERATOR DATA SIMULASI
# =============================================================================

# Profil std-dev per aktivitas (sesuai threshold di ESP32 & server)
ACTIVITY_PROFILES = {
    "DUDUK": {
        "accel_range": (0.001, 0.020),
        "gyro_range":  (0.001, 0.060),
        "bpm_range":   (55, 80),
    },
    "BERJALAN": {
        "accel_range": (0.030, 0.100),
        "gyro_range":  (0.080, 0.130),
        "bpm_range":   (80, 110),
    },
    "BERLARI": {
        "accel_range": (0.120, 0.400),
        "gyro_range":  (0.150, 0.600),
        "bpm_range":   (130, 180),
    },
}


def generate_payload(activity: str = "BERJALAN", device_id: str = "ESP32_SIM") -> dict:
    """Buat payload simulasi yang realistis sesuai profil aktivitas."""
    p = ACTIVITY_PROFILES.get(activity, ACTIVITY_PROFILES["BERJALAN"])
    return {
        "device_id":    device_id,
        "timestamp":    int(time.time() * 1000),
        "accel_stddev": round(random.uniform(*p["accel_range"]), 5),
        "gyro_stddev":  round(random.uniform(*p["gyro_range"]),  5),
        "bpm":          random.randint(*p["bpm_range"]),
        "user":         "User_SIM",
    }


# =============================================================================
#  TEST PING
# =============================================================================

class PingTest:
    """Tes apakah broker MQTT dapat dijangkau."""

    def __init__(self):
        self.connected = False
        self.client = mqtt.Client(client_id=MQTT_CLIENT_TEST + "_ping")
        self.client.on_connect = self._on_connect

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = (rc == 0)

    def run(self, timeout: int = 5) -> bool:
        logger.info("Ping broker %s:%d ...", MQTT_BROKER, MQTT_PORT)
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=5)
            self.client.loop_start()
            deadline = time.time() + timeout
            while time.time() < deadline:
                if self.connected:
                    break
                time.sleep(0.1)
            self.client.loop_stop()
            self.client.disconnect()
        except Exception as e:
            logger.error("Tidak dapat terhubung: %s", e)
            return False

        if self.connected:
            logger.info("✓ Broker dapat dijangkau.")
        else:
            logger.error("✗ Tidak ada respons dari broker dalam %ds.", timeout)
        return self.connected


# =============================================================================
#  TEST LISTEN
# =============================================================================

class ListenTest:
    """Pantau semua topic yang digunakan sistem AIoT Watch."""

    TOPICS = [TOPIC_SENSOR_DATA, TOPIC_CLASSIFICATION, TOPIC_STATUS]

    def __init__(self, duration: int = 30):
        self.duration = duration
        self.counts   = {t: 0 for t in self.TOPICS}
        self.client   = mqtt.Client(client_id=MQTT_CLIENT_TEST + "_listen")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            for topic in self.TOPICS:
                client.subscribe(topic, qos=1)
                logger.info("Listening: %s", topic)

    def _on_message(self, client, userdata, msg):
        self.counts[msg.topic] = self.counts.get(msg.topic, 0) + 1
        try:
            payload = json.loads(msg.payload.decode())
        except Exception:
            payload = msg.payload.decode()[:100]

        logger.info("[%s] %s", msg.topic, json.dumps(payload, ensure_ascii=False))

    def run(self) -> None:
        logger.info("Memantau topic selama %d detik...", self.duration)
        self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        self.client.loop_start()
        time.sleep(self.duration)
        self.client.loop_stop()
        self.client.disconnect()

        logger.info("=== Ringkasan ===")
        for t, c in self.counts.items():
            logger.info("  %-35s : %d pesan", t, c)


# =============================================================================
#  TEST SIMULATE
# =============================================================================

class SimulateTest:
    """
    Simulasikan pengiriman data sensor ESP32.

    Berguna untuk menguji server_knn.py tanpa hardware fisik.
    """

    def __init__(
        self,
        activity:  str = "BERJALAN",
        interval:  float = 2.0,
        count:     int   = 10,
        device_id: str   = "ESP32_SIM",
    ):
        self.activity  = activity
        self.interval  = interval
        self.count     = count
        self.device_id = device_id
        self.received  = []

        self.client = mqtt.Client(client_id=MQTT_CLIENT_TEST + "_sim")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe(TOPIC_CLASSIFICATION, qos=1)
            logger.info("Simulasi terhubung. Subscribe: %s", TOPIC_CLASSIFICATION)

    def _on_message(self, client, userdata, msg):
        """Tangkap balasan dari server KNN."""
        try:
            resp = json.loads(msg.payload.decode())
            self.received.append(resp)
            logger.info(
                "  ← Balasan KNN: activity=%-9s conf=%.2f bpm=%d",
                resp.get("activity",   "?"),
                resp.get("confidence", 0),
                resp.get("bpm",        0),
            )
        except Exception as e:
            logger.warning("Gagal parse balasan: %s", e)

    def run(self) -> None:
        logger.info(
            "Simulasi '%s' | %d paket | interval %.1fs | device=%s",
            self.activity, self.count, self.interval, self.device_id,
        )
        self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
        self.client.loop_start()
        time.sleep(1)  # Tunggu koneksi stabil

        for i in range(self.count):
            payload = generate_payload(self.activity, self.device_id)
            msg     = json.dumps(payload)
            info    = self.client.publish(TOPIC_SENSOR_DATA, msg, qos=1)
            logger.info(
                "  → [%2d/%d] accel=%.4f gyro=%.4f bpm=%d mid=%d",
                i + 1, self.count,
                payload["accel_stddev"],
                payload["gyro_stddev"],
                payload["bpm"],
                info.mid,
            )
            time.sleep(self.interval)

        # Tunggu balasan
        time.sleep(2)
        self.client.loop_stop()
        self.client.disconnect()

        logger.info("Dikirim: %d | Diterima balasan: %d", self.count, len(self.received))


# =============================================================================
#  TEST FULL END-TO-END
# =============================================================================

class FullTest:
    """
    Tes end-to-end lengkap:
    1. Ping broker
    2. Simulasikan semua 3 aktivitas
    3. Verifikasi balasan dari server KNN
    4. Cetak laporan hasil
    """

    def __init__(self):
        self.results = {}

    def run(self) -> bool:
        # 1. Ping
        if not PingTest().run():
            logger.error("Abort: broker tidak dapat dijangkau.")
            return False

        # 2–3. Simulasi tiap aktivitas
        for act in ACTIVITY_LABELS:
            logger.info("--- Menguji aktivitas: %s ---", act)
            sim = SimulateTest(activity=act, count=5, interval=1.5)
            sim.run()
            self.results[act] = {
                "sent":     sim.count,
                "received": len(sim.received),
                "responses": sim.received,
            }

        # 4. Laporan
        logger.info("\n=== LAPORAN FULL TEST ===")
        all_pass = True
        for act, res in self.results.items():
            rate = res["received"] / res["sent"] * 100 if res["sent"] else 0
            ok   = res["received"] > 0
            if not ok:
                all_pass = False
            logger.info(
                "  %-9s : kirim=%d terima=%d (%.0f%%) %s",
                act, res["sent"], res["received"], rate,
                "✓" if ok else "✗ GAGAL",
            )

            # Cek akurasi klasifikasi
            if res["responses"]:
                correct = sum(
                    1 for r in res["responses"]
                    if r.get("activity") == act
                )
                acc = correct / len(res["responses"]) * 100
                logger.info("    Akurasi klasifikasi: %.0f%%", acc)

        return all_pass


# =============================================================================
#  ENTRY POINT
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Alat uji MQTT untuk sistem AIoT Watch.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["ping", "listen", "simulate", "full"],
        default="ping",
        help=(
            "ping     — Tes koneksi broker\n"
            "listen   — Pantau semua topic (default 30s)\n"
            "simulate — Kirim data ESP32 simulasi\n"
            "full     — Tes end-to-end lengkap"
        ),
    )
    parser.add_argument(
        "--activity", "-a",
        choices=ACTIVITY_LABELS,
        default="BERJALAN",
        help="Aktivitas untuk mode simulate (default: BERJALAN)",
    )
    parser.add_argument(
        "--count", "-c",
        type=int,
        default=10,
        help="Jumlah paket simulasi (default: 10)",
    )
    parser.add_argument(
        "--interval", "-i",
        type=float,
        default=2.0,
        help="Interval antar paket dalam detik (default: 2.0)",
    )
    parser.add_argument(
        "--duration", "-d",
        type=int,
        default=30,
        help="Durasi mode listen dalam detik (default: 30)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    logger.info("=== AIoT Watch — MQTT Tester ===")
    logger.info("Broker: %s:%d", MQTT_BROKER, MQTT_PORT)

    if args.mode == "ping":
        ok = PingTest().run()
        sys.exit(0 if ok else 1)

    elif args.mode == "listen":
        ListenTest(duration=args.duration).run()

    elif args.mode == "simulate":
        SimulateTest(
            activity  = args.activity,
            interval  = args.interval,
            count     = args.count,
        ).run()

    elif args.mode == "full":
        ok = FullTest().run()
        sys.exit(0 if ok else 1)