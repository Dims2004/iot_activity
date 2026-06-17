# Klasifikasi Aktivitas Manusia Menggunakan Algoritma K-Nearest Neighbor Berbasis Internet of Things

## Deskripsi Proyek

Penelitian ini bertujuan untuk mengembangkan sistem klasifikasi aktivitas manusia berbasis Internet of Things (IoT) menggunakan perangkat wearable yang terdiri dari ESP32, sensor MPU6050, dan Pulse Sensor.

Sistem dirancang untuk mengenali tiga jenis aktivitas manusia, yaitu:

* DUDUK
* BERJALAN
* BERLARI

Data sensor yang diperoleh dari perangkat wearable dikirim melalui protokol MQTT ke server untuk dilakukan proses pengolahan data, preprocessing, pelatihan model, dan klasifikasi aktivitas menggunakan algoritma K-Nearest Neighbor (KNN).

Penelitian ini merupakan implementasi Tugas Akhir Program Studi Informatika Universitas Telkom Surabaya dengan judul:

**Klasifikasi Aktivitas Manusia Menggunakan Algoritma K-Nearest Neighbor Berbasis Internet of Things**

---

# Latar Belakang

Pemantauan aktivitas manusia merupakan salah satu penerapan Internet of Things (IoT) yang banyak digunakan pada bidang kesehatan, olahraga, dan pemantauan aktivitas harian. Dengan memanfaatkan sensor gerak dan sensor fisiologis, sistem dapat mengenali pola aktivitas pengguna secara otomatis.

Pada penelitian ini digunakan kombinasi sensor MPU6050 yang terdiri dari accelerometer dan gyroscope untuk mendeteksi gerakan tubuh, serta Pulse Sensor untuk mengukur detak jantung pengguna secara real-time. Data sensor tersebut kemudian diproses menggunakan algoritma K-Nearest Neighbor (KNN) untuk melakukan klasifikasi aktivitas manusia.

---

# Tujuan Penelitian

Penelitian ini memiliki tujuan sebagai berikut:

1. Merancang perangkat wearable berbasis ESP32 untuk akuisisi data aktivitas manusia.
2. Mengumpulkan dataset aktivitas manusia menggunakan sensor MPU6050 dan Pulse Sensor.
3. Melakukan preprocessing dataset menggunakan teknik imputasi, deteksi outlier, dan normalisasi.
4. Menerapkan algoritma K-Nearest Neighbor (KNN) untuk klasifikasi aktivitas manusia.
5. Mengevaluasi performa model menggunakan metrik Accuracy, Precision, Recall, dan F1-Score.
6. Mengimplementasikan sistem klasifikasi aktivitas manusia secara real-time menggunakan MQTT.

---

# Arsitektur Sistem

```text
MPU6050 + Pulse Sensor
          │
          ▼
        ESP32
          │
          ▼
 MQTT Publisher (ESP32)
          │
          ▼
      MQTT Broker
          │
          ▼
  Data Collection Server
          │
          ▼
        Dataset
          │
          ▼
 Exploratory Data Analysis
          │
          ▼
     Preprocessing
          │
          ▼
     Training Model
          │
          ▼
      Model KNN
          │
          ▼
      Server KNN
          │
          ▼
 Real-Time Classification
          │
          ▼
 Dashboard Monitoring
```

Pada penelitian ini ESP32 hanya berfungsi sebagai perangkat akuisisi data dan pengirim data sensor melalui MQTT.

Seluruh proses machine learning dilakukan pada server menggunakan Python.

---

# Perangkat Keras

Perangkat keras yang digunakan dalam penelitian ini adalah:

| Komponen               | Fungsi                             |
| ---------------------- | ---------------------------------- |
| ESP32                  | Mikrokontroler utama               |
| MPU6050                | Sensor accelerometer dan gyroscope |
| Pulse Sensor           | Sensor detak jantung               |
| OLED SSD1306 0.96 Inch | Menampilkan BPM dan aktivitas      |
| Kabel USB              | Sumber daya perangkat              |

---

# Perangkat Lunak

Perangkat lunak yang digunakan:

* Arduino IDE
* Python
* MQTT (EMQX Broker)
* Pandas
* NumPy
* Scikit-Learn
* Matplotlib
* Seaborn
* Joblib
* Paho MQTT

---

# Struktur Proyek

```text
iot_activity_knn/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── dataset/
│   ├── dataset.csv
│   └── sessions_summary.csv
│
├── models/
│   ├── knn_model.pkl
│   ├── scaler.pkl
│   └── bpm_medians.pkl
│
├── logs/
│
├── notebooks/
│   ├── 01_eksplorasi_data.ipynb
│   └── 02_training_model.ipynb
│
├── src/
│   ├── collect_participants.py
│   ├── server_knn.py
│   ├── config.py
│   └── utils.py
│
├── requirements.txt
└── README.md
```

---

# Dataset

Dataset diperoleh secara mandiri melalui proses pengambilan data menggunakan perangkat wearable yang dikembangkan pada penelitian ini.

Karakteristik dataset:

* Jumlah Partisipan : 15 Orang
* Aktivitas : Duduk, Berjalan, Berlari
* Durasi Aktivitas : 10 Menit per Aktivitas
* Total Data Awal : 13.410 Sampel
* Total Data Setelah Preprocessing : 13.214 Sampel

Fitur yang digunakan:

| Fitur        | Deskripsi                     |
| ------------ | ----------------------------- |
| accel_stddev | Standar deviasi accelerometer |
| gyro_stddev  | Standar deviasi gyroscope     |
| bpm_filled   | Nilai BPM hasil imputasi      |
| activity     | Label aktivitas               |

---

# Tahapan Penelitian

## 1. Pengambilan Data

Data dikumpulkan menggunakan:

* ESP32
* MPU6050
* Pulse Sensor
* MQTT

Label aktivitas diberikan secara manual oleh operator saat proses pengambilan data.

Aktivitas yang direkam:

* DUDUK
* BERJALAN
* BERLARI

Output tahap ini berupa dataset mentah yang disimpan dalam format CSV.

---

## 2. Exploratory Data Analysis (EDA)

Tahap eksplorasi data dilakukan untuk memahami karakteristik dataset.

Analisis yang dilakukan:

* Statistik deskriptif
* Distribusi kelas aktivitas
* Analisis missing value
* Analisis BPM berdasarkan aktivitas
* Visualisasi distribusi fitur

---

## 3. Preprocessing Dataset

Tahapan preprocessing meliputi:

### Imputasi BPM

Nilai BPM yang bernilai 0 diganti menggunakan median BPM berdasarkan aktivitas.

### Penghapusan Outlier

Outlier dideteksi menggunakan metode Z-Score dengan threshold:

```text
± 3.5
```

### Normalisasi Data

Normalisasi fitur dilakukan menggunakan:

```text
MinMaxScaler
```

Fitur yang dinormalisasi:

* accel_stddev
* gyro_stddev
* bpm_filled

---

## 4. Pelatihan Model KNN

Pelatihan model dilakukan menggunakan:

* Algoritma K-Nearest Neighbor
* Euclidean Distance
* Distance Weighting
* Stratified Train-Test Split
* 5-Fold Stratified Cross Validation

Konfigurasi model terbaik:

| Parameter | Nilai              |
| --------- | ------------------ |
| K Optimal | 18                 |
| Metric    | Euclidean Distance |
| Weight    | Distance           |

Output pelatihan:

```text
knn_model.pkl
scaler.pkl
bpm_medians.pkl
```

---

## 5. Evaluasi Model

Evaluasi model dilakukan menggunakan:

* Accuracy
* Precision
* Recall
* F1-Score
* Confusion Matrix

Hasil model terbaik:

| Parameter         | Nilai  |
| ----------------- | ------ |
| Training Accuracy | 100%   |
| Test Accuracy     | 83.68% |
| K Optimal         | 18     |

---

## 6. Implementasi Server KNN Real-Time

Setelah model selesai dilatih, sistem memasuki tahap klasifikasi real-time.

Server KNN bertugas:

1. Subscribe data sensor dari MQTT.
2. Menerima fitur accel_stddev, gyro_stddev, dan bpm.
3. Melakukan imputasi BPM jika diperlukan.
4. Melakukan normalisasi menggunakan scaler hasil training.
5. Melakukan prediksi aktivitas menggunakan model KNN.
6. Publish hasil klasifikasi ke MQTT.

Output klasifikasi:

* DUDUK
* BERJALAN
* BERLARI

---

# Cara Menjalankan Sistem

## 1. Install Dependency

```bash
pip install -r requirements.txt
```

---

## 2. Pengambilan Data

Jalankan program pengumpulan dataset:

```bash
python src/collect_participants.py
```

Fungsi:

* Menerima data sensor dari ESP32
* Memberikan label aktivitas secara manual
* Menyimpan dataset aktivitas manusia

---

## 3. Exploratory Data Analysis

Buka notebook:

```text
01_eksplorasi_data.ipynb
```

Kemudian jalankan seluruh sel untuk melakukan analisis dataset.

---

## 4. Pelatihan Model KNN

Buka notebook:

```text
02_training_model.ipynb
```

Notebook akan:

* Melakukan preprocessing
* Menentukan nilai K optimal
* Melatih model KNN
* Menyimpan model hasil pelatihan

Output:

```text
models/knn_model.pkl
models/scaler.pkl
models/bpm_medians.pkl
```

---

## 5. Menjalankan Server KNN

Jalankan:

```bash
python src/server_knn.py
```

Server akan:

* Subscribe ke topic sensor/esp32/data
* Melakukan klasifikasi aktivitas
* Publish hasil ke topic classification/result

---

# MQTT Topics

| Topic                 | Fungsi                        |
| --------------------- | ----------------------------- |
| sensor/esp32/data     | Data sensor dari ESP32        |
| control/session       | Kontrol sesi pengambilan data |
| classification/result | Hasil klasifikasi aktivitas   |
| status/esp32          | Status perangkat              |

---

# Hasil Penelitian

Penelitian menghasilkan sistem klasifikasi aktivitas manusia berbasis IoT yang mampu mengenali aktivitas:

* Duduk
* Berjalan
* Berlari

menggunakan kombinasi fitur:

* accel_stddev
* gyro_stddev
* bpm_filled

dengan akurasi pengujian sebesar:

```text
83.68%
```

---

# Batasan Penelitian

1. Aktivitas yang digunakan hanya Duduk, Berjalan, dan Berlari.
2. Sensor yang digunakan hanya MPU6050 dan Pulse Sensor.
3. Algoritma yang digunakan hanya K-Nearest Neighbor (KNN).
4. Sistem menggunakan komunikasi MQTT melalui jaringan Wi-Fi.
5. Penelitian tidak digunakan untuk diagnosis medis.

---

# Penulis

Nama : Dimas Febrianto

NIM : 1203220069

Program Studi : S1 Informatika

Universitas Telkom Surabaya

Tahun : 2026

---

# Lisensi

Proyek ini dibuat untuk keperluan akademik dan penelitian Tugas Akhir.
