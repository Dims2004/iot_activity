"""
utils.py — Fungsi bantu: preprocessing, normalisasi, BPM imputation, logging
FIXED: Added debug logging for payload parsing
"""
import os, logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
import joblib

from config import (
    LOG_FILE, LOG_LEVEL, SCALER_PATH, BPM_MED_PATH,
    FEATURES, TARGET, CLASSES, CLASS_MAP,
    BPM_MEDIAN_DEFAULT, BPM_GLOBAL_MEDIAN
)

# Logging
def get_logger(name: str = "aiot") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger

# Validasi payload
def parse_sensor_payload(payload: dict) -> dict | None:
    """Validasi dan bersihkan payload JSON dari ESP32."""
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

    if not (0.0 <= accel <= 10.0): return None
    if not (0.0 <= gyro  <= 600.0): return None
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

# Preprocessing dataset
def load_and_clean_dataset(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    rename_map = {
        "accel_std": "accel_stddev",
        "gyro_std":  "gyro_stddev",
        "label":     "activity",
        "class":     "activity",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    base_feats = ["accel_stddev", "gyro_stddev", "bpm", TARGET]
    extra = ["bpm_filled", "participant_id", "participant_no", "received_at"]
    keep = [c for c in base_feats + extra if c in df.columns]
    df = df[keep].copy()
    df = df.dropna(subset=["accel_stddev", "gyro_stddev", TARGET])
    df = df[df[TARGET].isin(CLASSES)]
    if "bpm" not in df.columns:
        df["bpm"] = 0
    df = df.reset_index(drop=True)
    return df

# BPM Imputation
def impute_bpm(df: pd.DataFrame,
               bpm_medians: dict | None = None,
               fit: bool = True) -> tuple[pd.DataFrame, dict]:
    df = df.copy()
    if fit:
        bpm_medians = {}
        for cls in CLASSES:
            valid = df[(df[TARGET] == cls) & (df["bpm"] > 0)]["bpm"]
            bpm_medians[cls] = int(valid.median()) if len(valid) > 0 else BPM_MEDIAN_DEFAULT[cls]
        all_valid = df[df["bpm"] > 0]["bpm"]
        global_med = int(all_valid.median()) if len(all_valid) > 0 else BPM_GLOBAL_MEDIAN
        bpm_medians["_global"] = global_med
        print("BPM Median per kelas (untuk imputasi):")
        for k, v in bpm_medians.items():
            print(f"  {k}: {v} bpm")

    def fill_bpm(row):
        if row["bpm"] > 0:
            return row["bpm"]
        cls = row.get(TARGET, "")
        return bpm_medians.get(cls, bpm_medians.get("_global", BPM_GLOBAL_MEDIAN))

    df["bpm_filled"] = df.apply(fill_bpm, axis=1).astype(float)
    return df, bpm_medians

def impute_bpm_single(bpm: int, activity_hint: str = "",
                      bpm_medians: dict | None = None) -> float:
    if bpm > 0:
        return float(bpm)
    if bpm_medians is None:
        return float(BPM_MEDIAN_DEFAULT.get(activity_hint, BPM_GLOBAL_MEDIAN))
    return float(bpm_medians.get(activity_hint,
                                  bpm_medians.get("_global", BPM_GLOBAL_MEDIAN)))

# Normalisasi
def normalize_features(df: pd.DataFrame,
                        fit: bool = True,
                        scaler: MinMaxScaler | None = None
                        ) -> tuple[pd.DataFrame, MinMaxScaler]:
    for f in FEATURES:
        if f not in df.columns:
            raise ValueError(
                f"Kolom '{f}' tidak ditemukan. "
                f"Jalankan impute_bpm() dulu untuk membuat 'bpm_filled'."
            )
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
    df["label"] = df[TARGET].map(CLASS_MAP)
    return df

# Hapus outlier
def remove_outliers(df: pd.DataFrame, z_thresh: float = 3.5) -> pd.DataFrame:
    raw_feats = ["accel_stddev", "gyro_stddev", "bpm_filled"]
    for feat in raw_feats:
        if feat not in df.columns:
            continue
        mean, std = df[feat].mean(), df[feat].std()
        if std > 0:
            df = df[((df[feat] - mean) / std).abs() <= z_thresh]
    return df.reset_index(drop=True)

# Load utilities
def load_scaler() -> MinMaxScaler:
    if not os.path.exists(SCALER_PATH):
        raise FileNotFoundError(
            f"Scaler tidak ditemukan di {SCALER_PATH}. "
            "Jalankan notebook 02_training_model.ipynb terlebih dahulu."
        )
    return joblib.load(SCALER_PATH)

def load_bpm_medians() -> dict:
    if not os.path.exists(BPM_MED_PATH):
        print(f"[WARN] bpm_medians tidak ditemukan, pakai default.")
        return {**BPM_MEDIAN_DEFAULT, "_global": BPM_GLOBAL_MEDIAN}
    return joblib.load(BPM_MED_PATH)

def class_distribution(df: pd.DataFrame) -> pd.DataFrame:
    counts = df[TARGET].value_counts()
    pct    = (counts / len(df) * 100).round(2)
    return pd.DataFrame({"count": counts, "pct": pct})

def timestamp_filename(prefix: str = "data", ext: str = "csv") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"