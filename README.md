Berikut README GitHub yang bisa kamu langsung pakai untuk project tugas akhir kamu (tinggal copy–paste ke `README.md`):

---

# 🐱 IoT Activity Recognition System (Tugas Akhir)

Proyek ini merupakan implementasi sistem **IoT berbasis ESP32** untuk melakukan **pengambilan data sensor aktivitas manusia** dan mengirimkannya ke server untuk proses **machine learning**. Sistem ini dirancang menggunakan arsitektur *sensor-only*, di mana proses klasifikasi dilakukan di sisi server agar lebih efisien dan fleksibel.

---

## 📌 Deskripsi Proyek

Sistem ini bertujuan untuk:

* Mengumpulkan data aktivitas menggunakan sensor:

  * Accelerometer & Gyroscope (MPU6050)
  * Heart Rate (Pulse Sensor)
* Melakukan preprocessing ringan (feature extraction)
* Mengirim data ke server menggunakan **MQTT**
* Menyimpan data dalam format CSV untuk pelatihan model
* Mengklasifikasikan aktivitas:

  * 🪑 Duduk
  * 🚶 Berjalan
  * 🏃 Berlari

---

## ⚙️ Arsitektur Sistem

```
ESP32 (Sensor Node)
   ↓
MQTT Broker
   ↓
Python (Data Collection & Processing)
   ↓
CSV Dataset
   ↓
Machine Learning Model
```

Konsep yang digunakan:

> **Sensor-only architecture**
> ESP32 hanya sebagai pengambil data, bukan untuk klasifikasi.

---

## 📂 Struktur Folder

```
iot_activity/
│
├── data/               # Data mentah dari sensor
├── dataset/            # Dataset hasil preprocessing
├── logs/               # Log sistem
├── models/             # Model machine learning
├── notebooks/          # Eksperimen & analisis
├── src/                # Source code utama
├── tests/              # Testing
├── web/                # Web interface (jika ada)
│
├── requirements.txt    # Dependencies Python
└── README.md           # Dokumentasi proyek
```

---

## 🧠 Teknologi yang Digunakan

* **Hardware**

  * ESP32 / ESP8266
  * MPU6050 (Accelerometer & Gyroscope)
  * Pulse Sensor

* **Software**

  * Python
  * MQTT (Mosquitto / Broker lain)
  * Pandas, NumPy
  * Scikit-learn / TensorFlow
  * Arduino IDE

---

## 🚀 Cara Menjalankan

### 1. Setup Python Environment

```bash
pip install -r requirements.txt
```

---

### 2. Jalankan MQTT Broker

Contoh menggunakan Mosquitto:

```bash
mosquitto
```

---

### 3. Jalankan Data Collection

```bash
python src/data_collection.py
```

Fungsi:

* Mengambil data dari MQTT
* Menyimpan ke CSV
* Menambahkan ID partisipan

---

### 4. Upload Code ke ESP32

* Buka Arduino IDE
* Upload kode ESP32
* Pastikan WiFi & MQTT sudah dikonfigurasi

---

## 📊 Dataset

Dataset terdiri dari fitur:

| Feature      | Deskripsi          |
| ------------ | ------------------ |
| accel_stddev | Variasi percepatan |
| gyro_stddev  | Variasi rotasi     |
| bpm          | Detak jantung      |
| activity     | Label aktivitas    |

---

## 🤖 Machine Learning

Model digunakan untuk klasifikasi aktivitas berdasarkan data sensor.

Tahapan:

1. Data Collection
2. Preprocessing
3. Training Model
4. Evaluation
5. Deployment (Server)

---

## 🎯 Tujuan Pengembangan

* Monitoring aktivitas manusia berbasis IoT
* Implementasi real-time data streaming
* Integrasi sensor dengan machine learning
* Efisiensi komputasi pada perangkat IoT

---

## 📌 Catatan

* ESP32 tidak melakukan klasifikasi → hanya kirim data
* Label aktivitas diberikan secara manual saat pengambilan data
* Data diambil setiap interval tertentu (misalnya 15 menit)

---

## 👤 Author

**Nama:** Dimas
**Project:** Tugas Akhir Informatika
**Topik:** IoT + Activity Recognition

---

## 📄 Lisensi

Project ini dibuat untuk keperluan akademik (Tugas Akhir).

---

