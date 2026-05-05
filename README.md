---

# 📡 Klasifikasi Aktivitas Manusia Menggunakan Algoritma K-Nearest Neighbor Berbasis Internet of Things

Proyek ini merupakan implementasi sistem **Internet of Things (IoT)** untuk melakukan **klasifikasi aktivitas manusia** menggunakan algoritma **K-Nearest Neighbor (KNN)** berdasarkan data sensor dari perangkat wearable berbasis ESP32.

---

## 📌 Deskripsi Proyek

Penelitian ini bertujuan untuk mengembangkan sistem yang mampu mengenali aktivitas manusia secara otomatis menggunakan data sensor, yaitu:

* Accelerometer dan Gyroscope (MPU6050)
* Sensor detak jantung (Pulse Sensor)

Data yang diperoleh akan dikirim ke server dan diproses menggunakan algoritma **K-Nearest Neighbor (KNN)** untuk mengklasifikasikan aktivitas berikut:

* 🪑 Duduk
* 🚶 Berjalan
* 🏃 Berlari

---

## ⚙️ Arsitektur Sistem

```
ESP32 (Sensor Node)
   ↓
MQTT Protocol
   ↓
Python Server
   ↓
Data Processing & Feature Extraction
   ↓
KNN Classification
   ↓
Hasil Prediksi Aktivitas
```

### Konsep Utama:

> **Sensor-only architecture**
> ESP32 hanya bertugas untuk:

* Mengambil data sensor
* Mengirim data ke server

Seluruh proses machine learning dilakukan di sisi server.

---

## 📂 Struktur Project

```
iot_activity/
│
├── data/               # Data mentah dari sensor
├── dataset/            # Dataset hasil preprocessing
├── logs/               # Log sistem
├── models/             # Model KNN
├── notebooks/          # Analisis & eksperimen
├── src/                # Source code utama
├── tests/              # Pengujian
├── web/                # (Optional) Web monitoring
│
├── requirements.txt
└── README.md
```

---

## 🧠 Teknologi yang Digunakan

### Hardware

* ESP32 / ESP8266
* MPU6050 (Accelerometer & Gyroscope)
* Pulse Sensor

### Software

* Python
* MQTT (Mosquitto)
* Pandas & NumPy
* Scikit-learn (KNN)
* Arduino IDE

---

## 🚀 Cara Menjalankan Sistem

### 1. Install Dependency

```bash
pip install -r requirements.txt
```

---

### 2. Jalankan MQTT Broker

```bash
mosquitto
```

---

### 3. Jalankan Data Collection

```bash
python src/data_collection.py
```

Fungsi:

* Menerima data dari ESP32 melalui MQTT
* Menyimpan data ke file CSV
* Menambahkan label aktivitas secara manual

---

### 4. Training Model KNN

```bash
python src/train_model.py
```

---

### 5. Jalankan Prediksi

```bash
python src/predict.py
```

---

## 📊 Dataset

Dataset yang digunakan memiliki fitur sebagai berikut:

| Feature      | Deskripsi                  |
| ------------ | -------------------------- |
| accel_stddev | Standar deviasi percepatan |
| gyro_stddev  | Standar deviasi rotasi     |
| bpm          | Detak jantung              |
| activity     | Label aktivitas            |

---

## 🤖 Metode Klasifikasi

Algoritma yang digunakan adalah:

### 🔹 K-Nearest Neighbor (KNN)

Karakteristik:

* Berbasis jarak (distance-based)
* Tidak memerlukan training kompleks
* Cocok untuk dataset kecil hingga menengah

### Tahapan:

1. Pengumpulan data
2. Preprocessing
3. Feature extraction
4. Training model KNN
5. Evaluasi model
6. Prediksi aktivitas

---

## 🎯 Tujuan Penelitian

* Mengimplementasikan IoT untuk pengenalan aktivitas manusia
* Menganalisis performa algoritma KNN
* Menghasilkan sistem klasifikasi aktivitas berbasis sensor
* Membangun sistem real-time berbasis MQTT

---

## 📌 Catatan Penting

* ESP32 **tidak melakukan klasifikasi**
* Label aktivitas diberikan secara manual saat pengambilan data
* Data dikirim secara real-time menggunakan MQTT

---

## 👤 Author

**Nama:** Dimas
**Program Studi:** Informatika
**Judul:**
*Klasifikasi Aktivitas Manusia Menggunakan Algoritma K-Nearest Neighbor Berbasis Internet of Things*

---

## 📄 Lisensi

Digunakan untuk keperluan akademik (Tugas Akhir)

---
