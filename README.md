# Klasifikasi Aktivitas Manusia Menggunakan Algoritma K-Nearest Neighbor Berbasis Internet of Things

## Deskripsi Proyek

Proyek ini merupakan implementasi sistem Internet of Things (IoT) untuk klasifikasi aktivitas manusia menggunakan algoritma K-Nearest Neighbor (KNN). Sistem dirancang untuk mengumpulkan data aktivitas manusia melalui perangkat wearable berbasis ESP32 yang terintegrasi dengan sensor MPU6050 dan Pulse Sensor.

Aktivitas yang diklasifikasikan dalam penelitian ini meliputi:

* Duduk
* Berjalan
* Berlari

Data sensor yang diperoleh dikirim melalui protokol MQTT ke server untuk proses penyimpanan dataset, preprocessing, pelatihan model, dan klasifikasi aktivitas menggunakan algoritma KNN.

Penelitian ini merupakan implementasi dari Tugas Akhir Program Studi Informatika Universitas Telkom Surabaya dengan judul:

**"Klasifikasi Aktivitas Manusia Menggunakan Algoritma K-Nearest Neighbor Berbasis Internet of Things"**

---

## Tujuan Penelitian

Penelitian ini bertujuan untuk:

1. Merancang perangkat wearable berbasis ESP32 untuk akuisisi data aktivitas manusia.
2. Mengumpulkan dataset aktivitas manusia menggunakan sensor MPU6050 dan Pulse Sensor.
3. Melakukan preprocessing dataset menggunakan teknik imputasi, deteksi outlier, dan normalisasi.
4. Menerapkan algoritma K-Nearest Neighbor (KNN) untuk klasifikasi aktivitas manusia.
5. Mengevaluasi performa model menggunakan metrik Accuracy, Precision, Recall, dan F1-Score.

---

## Arsitektur Sistem

Sistem menggunakan konsep pemrosesan terpusat pada server (server-side machine learning).

```
MPU6050 + Pulse Sensor
          │
          ▼
        ESP32
          │
          ▼
   MQTT Publisher
          │
          ▼
      MQTT Broker
          │
          ▼
    Python Server
          │
          ├── Penyimpanan Dataset
          ├── Preprocessing Data
          ├── Pelatihan Model KNN
          └── Klasifikasi Aktivitas
          │
          ▼
 Dashboard Monitoring
```

Pada penelitian ini ESP32 hanya berfungsi sebagai perangkat akuisisi data sensor dan pengirim data melalui MQTT. Seluruh proses machine learning dilakukan pada server menggunakan Python.

---

## Perangkat Keras

Komponen yang digunakan dalam penelitian ini:

| Komponen           | Fungsi                                  |
| ------------------ | --------------------------------------- |
| ESP32              | Mikrokontroler utama                    |
| MPU6050            | Sensor accelerometer dan gyroscope      |
| Pulse Sensor       | Sensor detak jantung                    |
| OLED SSD1306 0.96" | Menampilkan informasi aktivitas dan BPM |
| Kabel USB          | Sumber daya perangkat                   |

---

## Perangkat Lunak

Perangkat lunak yang digunakan:

* Arduino IDE
* Python
* MQTT Broker (EMQX Public Broker)
* Pandas
* NumPy
* Scikit-Learn
* Matplotlib
* Seaborn
* Joblib

---

## Struktur Proyek

```
iot_activity_knn/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── dataset/
│
├── models/
│
├── notebooks/
│
├── src/
│   ├── data_collection.py
│   ├── preprocessing.py
│   ├── train_knn.py
│   ├── evaluate_model.py
│   └── realtime_server.py
│
├── web_dashboard/
│
├── requirements.txt
└── README.md
```

---

## Dataset

Dataset diperoleh melalui proses pengambilan data langsung menggunakan perangkat wearable yang dikembangkan.

Karakteristik dataset:

* Jumlah Partisipan : 15 orang
* Aktivitas : Duduk, Berjalan, Berlari
* Durasi Aktivitas : 10 menit per aktivitas
* Total Data Awal : 13.410 sampel
* Total Data Setelah Preprocessing : 13.214 sampel

Fitur yang digunakan:

| Fitur        | Deskripsi                     |
| ------------ | ----------------------------- |
| accel_stddev | Standar deviasi accelerometer |
| gyro_stddev  | Standar deviasi gyroscope     |
| bpm_filled   | Nilai BPM hasil imputasi      |
| activity     | Label aktivitas               |

---

## Tahapan Penelitian

### 1. Pengambilan Data

Data dikumpulkan menggunakan:

* Sensor MPU6050
* Pulse Sensor
* ESP32
* MQTT

Label aktivitas diberikan secara manual saat proses pengambilan data.

### 2. Exploratory Data Analysis (EDA)

Tahap analisis data meliputi:

* Statistik deskriptif
* Distribusi kelas
* Analisis missing value
* Analisis BPM berdasarkan aktivitas

### 3. Preprocessing Data

Tahapan preprocessing:

* Imputasi nilai BPM bernilai 0 menggunakan median berdasarkan aktivitas
* Deteksi dan penghapusan outlier menggunakan metode Z-Score (±3.5)
* Normalisasi fitur menggunakan MinMaxScaler

### 4. Pelatihan Model

Pelatihan model dilakukan menggunakan:

* Algoritma K-Nearest Neighbor (KNN)
* Stratified Train-Test Split (80:20)
* 5-Fold Stratified Cross Validation
* Euclidean Distance
* Distance Weighting

### 5. Evaluasi Model

Evaluasi model menggunakan:

* Accuracy
* Precision
* Recall
* F1-Score
* Confusion Matrix

---

## Hasil Penelitian

Parameter model terbaik:

| Parameter         | Nilai              |
| ----------------- | ------------------ |
| Algoritma         | K-Nearest Neighbor |
| Nilai K Optimal   | 18                 |
| Metrik Jarak      | Euclidean Distance |
| Weights           | Distance           |
| Data Latih        | 10.519             |
| Data Uji          | 2.630              |
| Training Accuracy | 100%               |
| Test Accuracy     | 83,68%             |

Hasil menunjukkan bahwa kombinasi fitur accelerometer, gyroscope, dan BPM mampu menghasilkan performa klasifikasi yang baik untuk membedakan aktivitas duduk, berjalan, dan berlari.

---

## Cara Menjalankan Sistem

### Instalasi Dependensi

```bash
pip install -r requirements.txt
```

### Menjalankan Pengambilan Data

```bash
python src/data_collection.py
```

### Menjalankan Preprocessing

```bash
python src/preprocessing.py
```

### Melatih Model KNN

```bash
python src/train_knn.py
```

### Evaluasi Model

```bash
python src/evaluate_model.py
```

### Menjalankan Server Real-Time

```bash
python src/realtime_server.py
```

---

## Batasan Penelitian

1. Aktivitas yang diklasifikasikan hanya terdiri dari duduk, berjalan, dan berlari.
2. Sensor yang digunakan hanya MPU6050 dan Pulse Sensor.
3. Algoritma yang digunakan hanya K-Nearest Neighbor (KNN).
4. Sistem menggunakan komunikasi MQTT melalui jaringan Wi-Fi.
5. Evaluasi dilakukan menggunakan metrik klasifikasi standar dan bukan untuk tujuan diagnosis medis.

---

## Penulis

Nama : Dimas Febrianto

NIM : 1203220069

Program Studi : S1 Informatika

Universitas Telkom Surabaya

Tahun : 2026

---

## Lisensi

Proyek ini dikembangkan untuk keperluan akademik dan penelitian Tugas Akhir.
