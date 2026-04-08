"""
test_integration.py - Integration test untuk seluruh sistem
"""

import unittest
import sys
import os
import json
import time
import threading
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from config import *
import paho.mqtt.client as mqtt

class TestSystemIntegration(unittest.TestCase):
    """Integration test untuk seluruh sistem"""
    
    @classmethod
    def setUpClass(cls):
        print("\n" + "=" * 60)
        print("INTEGRATION TEST - KNN SYSTEM")
        print("=" * 60)
        
        cls.received_data = []
        cls.received_results = []
        cls.mqtt_connected = False
        
        # Setup MQTT client untuk testing
        cls.test_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        cls.test_client.on_connect = cls.on_test_connect
        cls.test_client.on_message = cls.on_test_message
        
        # Connect ke broker
        cls.test_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        cls.test_client.loop_start()
        
        time.sleep(2)
        
        if not cls.mqtt_connected:
            print("Warning: MQTT broker not available. Some tests will be skipped.")
    
    @classmethod
    def on_test_connect(cls, client, userdata, flags, rc, properties=None):
        cls.mqtt_connected = True
        print("✓ Test MQTT client connected")
        cls.test_client.subscribe(TOPIC_SUB_DATA)
        cls.test_client.subscribe(TOPIC_PUB_RESULT)
    
    @classmethod
    def on_test_message(cls, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            if msg.topic == TOPIC_SUB_DATA:
                cls.received_data.append(data)
            elif msg.topic == TOPIC_PUB_RESULT:
                cls.received_results.append(data)
        except:
            pass
    
    def test_01_mqtt_broker(self):
        """Test MQTT broker availability"""
        self.assertTrue(self.mqtt_connected, "MQTT broker not available")
        print("✓ Test 1: MQTT broker available - PASSED")
    
    def test_02_publish_sensor_data(self):
        """Test publish sensor data ke MQTT"""
        if not self.mqtt_connected:
            self.skipTest("MQTT not available")
        
        # Buat data sensor dummy
        test_data = {
            "ax": 0.12,
            "ay": 0.05,
            "az": 1.02,
            "gx": 0.23,
            "gy": 0.11,
            "gz": 0.08,
            "bpm": 75
        }
        
        # Publish data
        self.test_client.publish(TOPIC_SUB_DATA, json.dumps(test_data))
        time.sleep(1)
        
        # Cek apakah data diterima
        found = any(d.get('bpm') == 75 for d in self.received_data)
        self.assertTrue(found, "Sensor data not received")
        print("✓ Test 2: Publish sensor data - PASSED")
    
    def test_03_data_format(self):
        """Test format data yang dikirim"""
        if not self.mqtt_connected:
            self.skipTest("MQTT not available")
        
        required_fields = ['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']
        
        for data in self.received_data[-5:]:
            for field in required_fields:
                self.assertIn(field, data, f"Missing field: {field}")
        
        print("✓ Test 3: Data format validation - PASSED")
    
    def test_04_result_format(self):
        """Test format hasil klasifikasi"""
        if not self.mqtt_connected:
            self.skipTest("MQTT not available")
        
        if len(self.received_results) > 0:
            result = self.received_results[-1]
            self.assertIn('aktivitas', result)
            self.assertIn('akurasi', result)
            print("✓ Test 4: Result format validation - PASSED")
        else:
            print("⚠ Test 4: No result received yet - SKIPPED")
    
    @classmethod
    def tearDownClass(cls):
        cls.test_client.loop_stop()
        cls.test_client.disconnect()
        print("\n" + "=" * 60)
        print("INTEGRATION TEST COMPLETED")
        print("=" * 60)

if __name__ == '__main__':
    unittest.main()