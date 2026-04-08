"""
utils.py — Fungsi bantu: preprocessing, normalisasi, logging
"""

import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
import joblib

from config import (
    LOG_FILE, LOG_LEVEL, SCALER_PATH,
    FEATURES, TARGET, CLASSES, CLASS_MAP
)


# ═══════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════
def get_logger(name: str = "aiot") -> logging.Logger:
    """
    Kembalikan logger yang menulis ke file dan console sekaligus.
    Panggil sekali di tiap modul: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger                           # hindari duplikasi handler

    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler ke file
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler ke console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ═══════════════════════════════════════════════════════════
#  VALIDASI & PARSING PAYLOAD MQTT
# ═══════════════════════════════════════════════════════════
def parse_sensor_payload(payload: dict) -> dict | None:
    """
    Validasi dan bersihkan payload JSON dari ESP32.
    Kembalikan dict yang sudah bersih, atau None jika tidak valid.

    Payload yang diharapkan:
    {
        "device_id":    "ESP32_001",
        "timestamp":    <millis>,
        "accel_stddev": <float>,
        "gyro_stddev":  <float>,
        "bpm":          <int>,
        "user":         "User_001",
        "local_act":    "DUDUK" | "BERJALAN" | "BERLARI"   (opsional)
    }
    """
    required = ["accel_stddev", "gyro_stddev", "bpm"]
    for key in required:
        if key not in payload:
            return None

    try:
        accel = float(payload["accel_stddev"])
        gyro  = float(payload["gyro_stddev"])
        bpm   = int(payload["bpm"])
    except (ValueError, TypeError):
        return None

    # Sanity check nilai sensor
    if not (0.0 <= accel <= 10.0):   return None   # g
    if not (0.0 <= gyro  <= 600.0):  return None   # °/s
    if bpm != 0 and not (30 <= bpm <= 220): return None

    return {
        "device_id":    payload.get("device_id", "unknown"),
        "timestamp":    payload.get("timestamp", 0),
        "accel_stddev": round(accel, 6),
        "gyro_stddev":  round(gyro, 4),
        "bpm":          bpm,
        "user":         payload.get("user", "unknown"),
        "local_act":    payload.get("local_act", ""),
        "received_at":  datetime.now().isoformat()
    }


# ═══════════════════════════════════════════════════════════
#  PREPROCESSING DATASET
# ═══════════════════════════════════════════════════════════
def load_and_clean_dataset(csv_path: str) -> pd.DataFrame:
    """
    Muat dataset CSV, bersihkan, dan kembalikan DataFrame siap pakai.

    Kolom minimum yang diharapkan: accel_stddev, gyro_stddev, activity
    """
    df = pd.read_csv(csv_path)

    # Rename kolom jika perlu (toleransi variasi nama)
    rename_map = {
        "accel_std": "accel_stddev",
        "gyro_std":  "gyro_stddev",
        "label":     "activity",
        "class":     "activity",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Hapus kolom yang tidak diperlukan
    useful_cols = FEATURES + [TARGET] + ["bpm", "user", "received_at"]
    df = df[[c for c in useful_cols if c in df.columns]]

    # Hapus baris dengan NaN di fitur atau label
    df = df.dropna(subset=FEATURES + [TARGET])

    # Validasi label
    df = df[df[TARGET].isin(CLASSES)]

    # Hapus outlier ekstrem (z-score > 4)
    for feat in FEATURES:
        mean, std = df[feat].mean(), df[feat].std()
        if std > 0:
            df = df[((df[feat] - mean) / std).abs() <= 4]

    df = df.reset_index(drop=True)
    return df


def normalize_features(
    df: pd.DataFrame,
    fit: bool = True,
    scaler: MinMaxScaler | None = None
) -> tuple[pd.DataFrame, MinMaxScaler]:
    """
    Normalisasi fitur dengan MinMaxScaler.

    Args:
        df    : DataFrame dengan kolom FEATURES
        fit   : True → fit scaler baru; False → gunakan scaler yang diberikan
        scaler: scaler existing jika fit=False

    Returns:
        df_norm  : DataFrame ternormalisasi
        scaler   : scaler yang dipakai
    """
    if fit:
        scaler = MinMaxScaler()
        df[FEATURES] = scaler.fit_transform(df[FEATURES])
        joblib.dump(scaler, SCALER_PATH)
    else:
        if scaler is None:
            raise ValueError("Scaler harus diberikan jika fit=False")
        df[FEATURES] = scaler.transform(df[FEATURES])

    return df, scaler


def encode_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Tambahkan kolom label numerik berdasarkan CLASS_MAP."""
    df["label"] = df[TARGET].map(CLASS_MAP)
    return df


# ═══════════════════════════════════════════════════════════
#  FITUR TAMBAHAN (opsional, bisa diaktifkan)
# ═══════════════════════════════════════════════════════════
def add_bpm_feature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tambahkan BPM sebagai fitur ke-3 jika tersedia.
    BPM=0 (tidak terbaca) diisi dengan median BPM valid.
    """
    if "bpm" not in df.columns:
        return df

    valid_bpm  = df.loc[df["bpm"] > 0, "bpm"]
    median_bpm = int(valid_bpm.median()) if len(valid_bpm) > 0 else 75

    df["bpm_filled"] = df["bpm"].replace(0, median_bpm)
    return df


# ═══════════════════════════════════════════════════════════
#  UTILITAS UMUM
# ═══════════════════════════════════════════════════════════
def load_scaler() -> MinMaxScaler:
    """Muat scaler yang sudah disimpan saat training."""
    if not os.path.exists(SCALER_PATH):
        raise FileNotFoundError(
            f"Scaler tidak ditemukan di {SCALER_PATH}. "
            "Jalankan notebook 02_training_model.ipynb terlebih dahulu."
        )
    return joblib.load(SCALER_PATH)


def timestamp_filename(prefix: str = "data", ext: str = "csv") -> str:
    """Hasilkan nama file dengan timestamp, misal: data_20250612_153045.csv"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"


def class_distribution(df: pd.DataFrame) -> pd.Series:
    """Tampilkan distribusi kelas dalam persen."""
    counts = df[TARGET].value_counts()
    pct    = (counts / len(df) * 100).round(2)
    return pd.DataFrame({"count": counts, "pct": pct})