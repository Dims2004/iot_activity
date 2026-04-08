import unittest
import json
import time
import threading
import paho.mqtt.client as mqtt
from src.config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_RAW, MQTT_TOPIC_RESULT

class TestMQTT(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.received_messages = []
        cls.client = mqtt.Client()
        cls.client.on_message = cls.on_message
        cls.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        cls.client.subscribe(MQTT_TOPIC_RESULT)
        cls.client.loop_start()
    
    @classmethod
    def on_message(cls, client, userdata, msg):
        cls.received_messages.append(json.loads(msg.payload.decode()))
    
    def test_send_and_receive(self):
        """Test MQTT publish and subscribe"""
        test_payload = {
            'accX': 0.15,
            'accY': 0.12,
            'accZ': 0.92,
            'bpm': 95,
            'delta': 0.25
        }
        
        self.client.publish(MQTT_TOPIC_RAW, json.dumps(test_payload))
        time.sleep(2)
        
        self.assertGreater(len(self.received_messages), 0)
        
        last_message = self.received_messages[-1]
        self.assertIn('aktivitas', last_message)
        self.assertIn('confidence', last_message)
        self.assertIn('bpm', last_message)
    
    @classmethod
    def tearDownClass(cls):
        cls.client.loop_stop()
        cls.client.disconnect()

if __name__ == '__main__':
    unittest.main()