#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_model.py - Unit test untuk model KNN
Digunakan untuk menguji performa model dengan berbagai skenario
"""

import unittest
import numpy as np
import pandas as pd
import joblib
import sys
import os
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Tambahkan path ke src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from config import *

class TestKNNModel(unittest.TestCase):
    """Test case untuk model KNN"""
    
    @classmethod
    def setUpClass(cls):
        """Setup sebelum semua test dijalankan"""
        print("\n" + "=" * 60)
        print("SETUP TEST MODEL KNN")
        print("=" * 60)
        
        # Load model
        try:
            cls.model = joblib.load(MODEL_PATH)
            cls.scaler = joblib.load(SCALER_PATH)
            cls.label_encoder = joblib.load(ENCODER_PATH)
            print("✓ Model loaded successfully")
        except Exception as e:
            print(f"✗ Failed to load model: {e}")
            raise
        
        # Load dataset untuk testing
        try:
            cls.data = pd.read_csv(DATASET_PATH)
            print(f"✓ Dataset loaded: {len(cls.data)} samples")
        except Exception as e:
            print(f"✗ Failed to load dataset: {e}")
            raise
        
        # Mapping label
        cls.label_map = {
            0: "DUDUK",
            1: "BERJALAN",
            2: "BERLARI"
        }
    
    def test_01_model_exists(self):
        """Test 1: Memastikan model dan scaler ada"""
        self.assertIsNotNone(self.model)
        self.assertIsNotNone(self.scaler)
        self.assertIsNotNone(self.label_encoder)
        print("✓ Test 1: Model exists - PASSED")
    
    def test_02_model_type(self):
        """Test 2: Memastikan tipe model benar"""
        from sklearn.neighbors import KNeighborsClassifier
        self.assertIsInstance(self.model, KNeighborsClassifier)
        print("✓ Test 2: Model type - PASSED")
    
    def test_03_feature_count(self):
        """Test 3: Memastikan jumlah fitur sesuai"""
        expected_features = 7  # ax, ay, az, gx, gy, gz, bpm
        self.assertEqual(self.model.n_features_in_, expected_features)
        print(f"✓ Test 3: Feature count ({expected_features}) - PASSED")
    
    def test_04_class_count(self):
        """Test 4: Memastikan jumlah kelas sesuai"""
        expected_classes = 3  # Duduk, Berjalan, Berlari
        self.assertEqual(len(self.model.classes_), expected_classes)
        print(f"✓ Test 4: Class count ({expected_classes}) - PASSED")
    
    def test_05_prediction_format(self):
        """Test 5: Memastikan format prediksi benar"""
        # Ambil sample pertama dari dataset
        sample = self.data.iloc[0]
        features = sample[['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']].values.reshape(1, -1)
        
        # Normalisasi
        features_scaled = self.scaler.transform(features)
        
        # Prediksi
        prediction = self.model.predict(features_scaled)
        probabilities = self.model.predict_proba(features_scaled)
        
        # Assertions
        self.assertEqual(len(prediction), 1)
        self.assertEqual(probabilities.shape, (1, len(self.model.classes_)))
        self.assertAlmostEqual(sum(probabilities[0]), 1.0, places=5)
        
        print("✓ Test 5: Prediction format - PASSED")
    
    def test_06_prediction_range(self):
        """Test 6: Memastikan prediksi dalam range yang valid"""
        # Test dengan beberapa sample
        for idx in range(min(10, len(self.data))):
            sample = self.data.iloc[idx]
            features = sample[['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']].values.reshape(1, -1)
            features_scaled = self.scaler.transform(features)
            
            prediction = self.model.predict(features_scaled)[0]
            self.assertIn(prediction, self.model.classes_)
        
        print("✓ Test 6: Prediction range - PASSED")
    
    def test_07_accuracy_threshold(self):
        """Test 7: Memastikan akurasi model di atas threshold"""
        # Prediksi seluruh dataset
        X = self.data[['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']]
        y_true = self.data['label']
        
        X_scaled = self.scaler.transform(X)
        y_pred = self.model.predict(X_scaled)
        
        # Hitung akurasi
        accuracy = accuracy_score(y_true, y_pred)
        
        # Threshold minimal 70%
        self.assertGreaterEqual(accuracy, 0.70)
        print(f"✓ Test 7: Accuracy ({accuracy:.2%}) >= 70% - PASSED")
    
    def test_08_confusion_matrix(self):
        """Test 8: Memeriksa confusion matrix"""
        X = self.data[['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']]
        y_true = self.data['label']
        
        X_scaled = self.scaler.transform(X)
        y_pred = self.model.predict(X_scaled)
        
        from sklearn.metrics import confusion_matrix
        cm = confusion_matrix(y_true, y_pred)
        
        # Cek dimensi confusion matrix
        self.assertEqual(cm.shape, (len(self.model.classes_), len(self.model.classes_)))
        
        # Cek diagonal (true positives) tidak boleh 0
        for i in range(len(self.model.classes_)):
            self.assertGreater(cm[i, i], 0)
        
        print("✓ Test 8: Confusion matrix - PASSED")
        print("\nConfusion Matrix:")
        print(cm)
    
    def test_09_precision_recall(self):
        """Test 9: Memeriksa precision dan recall"""
        X = self.data[['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']]
        y_true = self.data['label']
        
        X_scaled = self.scaler.transform(X)
        y_pred = self.model.predict(X_scaled)
        
        # Hitung precision dan recall (macro average)
        precision = precision_score(y_true, y_pred, average='macro')
        recall = recall_score(y_true, y_pred, average='macro')
        f1 = f1_score(y_true, y_pred, average='macro')
        
        print(f"\nMetrics:")
        print(f"  Precision: {precision:.2%}")
        print(f"  Recall   : {recall:.2%}")
        print(f"  F1-Score : {f1:.2%}")
        
        # Threshold minimal 60%
        self.assertGreaterEqual(precision, 0.60)
        self.assertGreaterEqual(recall, 0.60)
        
        print("✓ Test 9: Precision & Recall - PASSED")
    
    def test_10_prediction_confidence(self):
        """Test 10: Memeriksa confidence score prediksi"""
        X = self.data[['ax', 'ay', 'az', 'gx', 'gy', 'gz', 'bpm']].sample(min(20, len(self.data)))
        X_scaled = self.scaler.transform(X)
        
        probabilities = self.model.predict_proba(X_scaled)
        max_probs = np.max(probabilities, axis=1)
        
        # Confidence minimal 40% (bisa disesuaikan)
        avg_confidence = np.mean(max_probs)
        self.assertGreaterEqual(avg_confidence, 0.40)
        
        print(f"✓ Test 10: Avg confidence ({avg_confidence:.2%}) - PASSED")
    
    def test_11_extreme_values(self):
        """Test 11: Menguji dengan nilai ekstrim"""
        # Test dengan nilai maksimum
        extreme_cases = [
            [2.0, 2.0, 2.0, 50, 50, 50, 200],   # Nilai maks
            [-2.0, -2.0, -2.0, -50, -50, -50, 30],  # Nilai min
            [0, 0, 0, 0, 0, 0, 0]  # Nilai nol
        ]
        
        for case in extreme_cases:
            features = np.array([case])
            features_scaled = self.scaler.transform(features)
            
            # Harus tetap bisa prediksi tanpa error
            prediction = self.model.predict(features_scaled)
            probabilities = self.model.predict_proba(features_scaled)
            
            self.assertEqual(len(prediction), 1)
            self.assertEqual(probabilities.shape, (1, len(self.model.classes_)))
        
        print("✓ Test 11: Extreme values - PASSED")
    
    def test_12_invalid_input(self):
        """Test 12: Menguji dengan input invalid"""
        # Test dengan input yang salah dimensi
        with self.assertRaises(ValueError):
            invalid_features = np.array([[1, 2, 3]])  # Hanya 3 fitur
            self.scaler.transform(invalid_features)
        
        print("✓ Test 12: Invalid input handling - PASSED")
    
    @classmethod
    def tearDownClass(cls):
        """Cleanup setelah semua test selesai"""
        print("\n" + "=" * 60)
        print("TEST SELESAI")
        print("=" * 60)

def generate_test_report():
    """Generate test report ke file"""
    import unittest.mock
    from io import StringIO
    
    # Capture output
    captured_output = StringIO()
    sys.stdout = captured_output
    
    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestKNNModel)
    unittest.TextTestRunner(verbosity=2).run(suite)
    
    # Restore stdout
    sys.stdout = sys.__stdout__
    
    # Save to file
    report_path = os.path.join(os.path.dirname(__file__), '..', 'logs', 'test_report.txt')
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.write("TEST REPORT - KNN MODEL\n")
        f.write("=" * 60 + "\n")
        f.write(captured_output.getvalue())
    
    print(f"\nTest report saved to: {report_path}")

if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("MENJALANKAN UNIT TEST MODEL KNN")
    print("=" * 60)
    
    # Run tests
    unittest.main(argv=[''], verbosity=2, exit=False)
    
    # Generate report
    generate_test_report()