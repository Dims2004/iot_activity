"""
server_test_knn.py — Testing Server untuk Dashboard AIoT Watch
Fitur:
- Menerima data sensor dari ESP32
- Simulasi klasifikasi (tidak menyimpan dataset)
- Endpoint kontrol testing dari dashboard
- Mode manual test activity
"""

import json
import os
import sys
import signal
import time
import random
from datetime import datetime
from threading import Thread, Lock
import csv
from collections import deque

import paho.mqtt.client as mqtt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID,
    TOPIC_SENSOR_DATA, TOPIC_CLASSIFICATION, TOPIC_STATUS,
    CLASSES
)
from utils import get_logger

logger = get_logger("server_test_knn")

# ============================================================
# TOPIC KHUSUS TESTING
# ============================================================
TOPIC_TEST_COMMAND = "test/command"      # Dari dashboard
TOPIC_TEST_STATUS  = "test/status"       # Ke dashboard

# ============================================================
# STATISTIK TESTING
# ============================================================
class TestStats:
    def __init__(self):
        self.total_received = 0
        self.total_published = 0
        self.last_activity = "DUDUK"
        self.manual_mode = False
        self.manual_activity = "DUDUK"
        self.start_time = time.time()
        self.lock = Lock()
        
    def inc_received(self):
        with self.lock:
            self.total_received += 1
            
    def inc_published(self):
        with self.lock:
            self.total_published += 1
            
    def set_manual_mode(self, enabled, activity=""):
        with self.lock:
            self.manual_mode = enabled
            if activity:
                self.manual_activity = activity
                
    def get_activity(self):
        with self.lock:
            if self.manual_mode:
                return self.manual_activity
            return self.last_activity
            
    def summary(self):
        elapsed = time.time() - self.start_time
        with self.lock:
            return (
                f"  Runtime: {elapsed/60:.1f} menit\n"
                f"  Total received: {self.total_received}\n"
                f"  Total published: {self.total_published}\n"
                f"  Manual mode: {'ON' if self.manual_mode else 'OFF'}\n"
                f"  Manual activity: {self.manual_activity}"
            )

# ============================================================
# SIMULASI KLASIFIKASI
# ============================================================
def simulate_classification(accel_std, gyro_std, bpm, manual_override=None):
    """
    Simulasi klasifikasi berdasarkan nilai sensor.
    Jika manual_override ditentukan, gunakan itu.
    """
    if manual_override:
        return manual_override, 0.95
        
    # Simulasi: threshold sederhana
    if gyro_std < 30 and accel_std < 0.1:
        activity = "DUDUK"
        confidence = 0.85 + random.uniform(-0.05, 0.05)
    elif gyro_std < 150 and accel_std < 0.3:
        activity = "BERJALAN"
        confidence = 0.80 + random.uniform(-0.05, 0.05)
    else:
        activity = "BERLARI"
        confidence = 0.82 + random.uniform(-0.05, 0.05)
        
    # Variasi berdasarkan BPM
    if bpm > 0:
        if bpm < 80 and activity == "BERLARI":
            activity = "BERJALAN"
            confidence = 0.75
        elif bpm > 120 and activity == "DUDUK":
            activity = "BERJALAN"
            confidence = 0.70
            
    return activity, min(0.99, confidence)

# ============================================================
# STATE
# ============================================================
test_stats = TestStats()
client = None

def get_mqtt_client():
    global client
    return client

# ============================================================
# MQTT CALLBACKS
# ============================================================
def on_connect(client_mqtt, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"Test Server terhubung ke {MQTT_BROKER}:{MQTT_PORT}")
        client_mqtt.subscribe(TOPIC_SENSOR_DATA)
        client_mqtt.subscribe(TOPIC_TEST_COMMAND)
        client_mqtt.subscribe(TOPIC_STATUS)
        
        # Kirim status bahwa test server aktif
        status_msg = {
            "server": "test_knn",
            "status": "active",
            "timestamp": datetime.now().isoformat()
        }
        client_mqtt.publish(TOPIC_TEST_STATUS, json.dumps(status_msg), retain=True)
        logger.info("Test server aktif dan siap menerima data")
    else:
        logger.error(f"Gagal connect rc={rc}")

def on_message(client_mqtt, userdata, msg):
    global test_stats
    
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        logger.error(f"JSON decode error: {e}")
        return
        
    if topic == TOPIC_SENSOR_DATA:
        # Data dari ESP32
        test_stats.inc_received()
        
        accel_std = payload.get("accel_stddev", 0.0)
        gyro_std = payload.get("gyro_stddev", 0.0)
        bpm = payload.get("bpm", 0)
        participant_id = payload.get("participant_id", "test_user")
        participant_no = payload.get("participant_no", 0)
        
        # Klasifikasi
        activity, confidence = simulate_classification(
            accel_std, gyro_std, bpm,
            test_stats.get_activity() if test_stats.manual_mode else None
        )
        
        test_stats.last_activity = activity
        
        # Kirim hasil klasifikasi
        result = {
            "activity": activity,
            "confidence": round(confidence, 3),
            "bpm": bpm if bpm > 0 else 75,
            "bpm_raw": bpm,
            "user": participant_id,
            "participant_no": participant_no,
            "accel_stddev": round(accel_std, 4),
            "gyro_stddev": round(gyro_std, 2),
            "server_ts": datetime.now().isoformat(),
            "test_mode": True
        }
        
        client_mqtt.publish(TOPIC_CLASSIFICATION, json.dumps(result))
        test_stats.inc_published()
        
        logger.info(
            f"[TEST] {participant_id} → {activity} (conf={confidence:.2f}) | "
            f"a={accel_std:.4f} g={gyro_std:.2f} bpm={bpm}"
        )
        
    elif topic == TOPIC_TEST_COMMAND:
        # Perintah dari dashboard testing
        cmd = payload.get("cmd", "")
        
        if cmd == "SET_MANUAL_MODE":
            enabled = payload.get("enabled", False)
            activity = payload.get("activity", "DUDUK")
            test_stats.set_manual_mode(enabled, activity)
            logger.info(f"Manual mode: {enabled}, activity: {activity}")
            
            # Konfirmasi
            resp = {
                "cmd": "MANUAL_MODE_CONFIRM",
                "enabled": enabled,
                "activity": activity,
                "timestamp": datetime.now().isoformat()
            }
            client_mqtt.publish(TOPIC_TEST_STATUS, json.dumps(resp))
            
        elif cmd == "GET_STATS":
            resp = {
                "cmd": "STATS_RESPONSE",
                "stats": {
                    "total_received": test_stats.total_received,
                    "total_published": test_stats.total_published,
                    "manual_mode": test_stats.manual_mode,
                    "manual_activity": test_stats.manual_activity,
                    "last_activity": test_stats.last_activity,
                    "runtime_minutes": round((time.time() - test_stats.start_time) / 60, 1)
                },
                "timestamp": datetime.now().isoformat()
            }
            client_mqtt.publish(TOPIC_TEST_STATUS, json.dumps(resp))
            
        elif cmd == "SIMULATE_SENSOR":
            # Kirim data sensor simulasi untuk testing
            sim_accel = payload.get("accel_stddev", 0.1)
            sim_gyro = payload.get("gyro_stddev", 50.0)
            sim_bpm = payload.get("bpm", 75)
            sim_participant = payload.get("participant_id", "sim_user")
            
            # Buat payload seperti dari ESP32
            sim_payload = {
                "device_id": "SIMULATOR",
                "timestamp": int(time.time() * 1000),
                "accel_stddev": sim_accel,
                "gyro_stddev": sim_gyro,
                "bpm": sim_bpm,
                "participant_id": sim_participant,
                "participant_no": payload.get("participant_no", 99)
            }
            
            # Proses seperti data dari ESP32
            on_message(client_mqtt, None, mqtt.Message(
                topic=TOPIC_SENSOR_DATA.encode(),
                payload=json.dumps(sim_payload).encode(),
                qos=0,
                retain=False
            ))
            
    elif topic == TOPIC_STATUS:
        # Log status dari ESP32
        logger.info(f"[ESP32 STATUS] {payload}")

# ============================================================
# PERINTAH INTERAKTIF
# ============================================================
def interactive_commands():
    """Loop perintah interaktif di terminal"""
    global test_stats
    
    print("\n" + "="*60)
    print("  TESTING SERVER - Perintah Interaktif")
    print("="*60)
    print("  Perintah yang tersedia:")
    print("    stats      - Tampilkan statistik")
    print("    manual on [DUDUK|BERJALAN|BERLARI] - Aktifkan manual mode")
    print("    manual off - Nonaktifkan manual mode")
    print("    sim [aStd] [gStd] [bpm] - Kirim data simulasi")
    print("    quit       - Keluar")
    print("="*60 + "\n")
    
    while True:
        try:
            cmd_input = input("test> ").strip().lower()
            
            if cmd_input == "stats":
                print("\n" + test_stats.summary())
                print()
                
            elif cmd_input.startswith("manual"):
                parts = cmd_input.split()
                if len(parts) == 2 and parts[1] == "off":
                    test_stats.set_manual_mode(False, "")
                    print("  Manual mode: OFF")
                    
                elif len(parts) >= 3 and parts[1] == "on":
                    activity = parts[2].upper()
                    if activity in ["DUDUK", "BERJALAN", "BERLARI"]:
                        test_stats.set_manual_mode(True, activity)
                        print(f"  Manual mode: ON → {activity}")
                        
                        # Kirim ke dashboard
                        if client:
                            msg = {
                                "cmd": "SET_MANUAL_MODE",
                                "enabled": True,
                                "activity": activity
                            }
                            client.publish(TOPIC_TEST_COMMAND, json.dumps(msg))
                    else:
                        print("  Aktivitas: DUDUK, BERJALAN, BERLARI")
                else:
                    print("  Usage: manual on [DUDUK|BERJALAN|BERLARI] atau manual off")
                    
            elif cmd_input.startswith("sim"):
                parts = cmd_input.split()
                if len(parts) >= 4:
                    try:
                        a_std = float(parts[1])
                        g_std = float(parts[2])
                        bpm = int(parts[3])
                        
                        sim_payload = {
                            "device_id": "CLI_SIM",
                            "timestamp": int(time.time() * 1000),
                            "accel_stddev": a_std,
                            "gyro_stddev": g_std,
                            "bpm": bpm,
                            "participant_id": "cli_user",
                            "participant_no": 99
                        }
                        
                        if client:
                            client.publish(TOPIC_SENSOR_DATA, json.dumps(sim_payload))
                            print(f"  Simulasi dikirim: a={a_std}, g={g_std}, bpm={bpm}")
                        else:
                            print("  MQTT client belum siap")
                    except ValueError:
                        print("  Format: sim [accel_std] [gyro_std] [bpm]")
                else:
                    print("  Usage: sim [accel_std] [gyro_std] [bpm]")
                    print("  Contoh: sim 0.15 45.2 82")
                    
            elif cmd_input == "quit":
                print("\n  Menghentikan test server...")
                break
                
            else:
                if cmd_input:
                    print(f"  Perintah tidak dikenal: {cmd_input}")
                    
        except EOFError:
            break
        except KeyboardInterrupt:
            print("\n")
            break

# ============================================================
# MAIN
# ============================================================
def main():
    global client
    
    logger.info("="*60)
    logger.info("  AIoT Watch — TESTING SERVER (Tanpa Simpan Dataset)")
    logger.info(f"  Broker: {MQTT_BROKER}:{MQTT_PORT}")
    logger.info("  Mode: Simulasi KNN, data tidak disimpan")
    logger.info("="*60)
    
    # MQTT Client
    client = mqtt.Client(
        client_id=f"{MQTT_CLIENT_ID}_test",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    
    def shutdown(sig, frame):
        logger.info("\n[STOP] Test server berhenti.")
        logger.info(test_stats.summary())
        
        # Kirim status offline
        if client:
            status_msg = {
                "server": "test_knn",
                "status": "inactive",
                "timestamp": datetime.now().isoformat()
            }
            client.publish(TOPIC_TEST_STATUS, json.dumps(status_msg), retain=True)
            client.loop_stop()
            client.disconnect()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"Tidak bisa konek: {e}")
        sys.exit(1)
    
    logger.info("Test server aktif. Tekan Ctrl+C untuk berhenti.")
    client.loop_start()
    
    # Jalankan perintah interaktif di thread terpisah
    Thread(target=interactive_commands, daemon=True).start()
    
    try:
        while True:
            time.sleep(60)
            logger.info("=== TEST SERVER STATS ===\n" + test_stats.summary())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()