"""
Domain knowledge for the Timor-Leste low-voltage grid - ANOMALY DETECTION.

Single source of truth (used by the data generator, the live producer, the
Spark jobs, the stream processor and the website) for:
  * the 14 municipalities and the customer types with their daily load shapes
  * the 6 anomaly types the system must detect (+ Normal)
  * the meter "baseline" (normal consumption of a meter at a given hour)
  * the 16 model features and how they are engineered
  * human-readable explanations of an anomaly
"""
from __future__ import annotations

import numpy as np

# ---------------------------------------------------------------------------
# Grid constants (Timor-Leste: 220 V / 50 Hz low-voltage distribution)
# ---------------------------------------------------------------------------
NOMINAL_VOLTAGE = 220.0
NOMINAL_FREQUENCY = 50.0
FEEDER_DROP_SCALE = 0.45     # share of the municipality voltage-drop factor seen at the meter

# name, capital, latitude, longitude, relative population weight, grid, voltage-drop factor
MUNICIPALITIES = [
    ("Aileu",     "Aileu",          -8.7281, 125.5664,  54, "main",    0.085),
    ("Ainaro",    "Ainaro",         -8.9924, 125.5082,  73, "main",    0.095),
    ("Atauro",    "Atauro",         -8.2667, 125.6014,  10, "atauro",  0.100),
    ("Baucau",    "Baucau",         -8.4757, 126.4563, 134, "main",    0.070),
    ("Bobonaro",  "Maliana",        -8.9917, 125.2197, 107, "main",    0.090),
    ("Cova Lima", "Suai",           -9.3129, 125.2565,  74, "main",    0.085),
    ("Dili",      "Dili",           -8.5586, 125.5736, 325, "main",    0.042),
    ("Ermera",    "Gleno",          -8.7239, 125.4361, 138, "main",    0.090),
    ("Lautém",    "Lospalos",       -8.5217, 126.9983,  70, "main",    0.095),
    ("Liquiçá",   "Liquiçá",        -8.5875, 125.3419,  84, "main",    0.070),
    ("Manatuto",  "Manatuto",       -8.5114, 126.0131,  51, "main",    0.085),
    ("Manufahi",  "Same",           -9.0042, 125.6486,  61, "main",    0.090),
    ("Oecusse",   "Pante Macassar", -9.2000, 124.3833,  81, "oecusse", 0.080),
    ("Viqueque",  "Viqueque",       -8.8575, 126.3647,  80, "main",    0.095),
]
MUNICIPALITY_NAMES = [m[0] for m in MUNICIPALITIES]
MUNICIPALITY_COORDS = {m[0]: (m[2], m[3]) for m in MUNICIPALITIES}
MUNICIPALITY_GRID = {m[0]: m[5] for m in MUNICIPALITIES}
MUNICIPALITY_CODES = {
    "Aileu": "AIL", "Ainaro": "AIN", "Atauro": "ATA", "Baucau": "BAU",
    "Bobonaro": "BOB", "Cova Lima": "COV", "Dili": "DIL", "Ermera": "ERM",
    "Lautém": "LAU", "Liquiçá": "LIQ", "Manatuto": "MNT", "Manufahi": "MNF",
    "Oecusse": "OEC", "Viqueque": "VIQ",
}
GRIDS = ["main", "oecusse", "atauro"]

# ---------------------------------------------------------------------------
# Customer types
# ---------------------------------------------------------------------------
CUSTOMER_TYPES = ["residential", "commercial", "government", "industrial"]
CUSTOMER_TYPE_SHARE = [0.78, 0.15, 0.05, 0.02]
CUSTOMER_TYPE_TET = {"residential": "Residensiál", "commercial": "Komersiál",
                     "government": "Governu", "industrial": "Indústria"}
CUSTOMER_NAME_PREFIX = {"residential": "Uma-kain", "commercial": "Loja",
                        "government": "Edifísiu Governu", "industrial": "Fábrika"}
CONTRACT_KVA = {
    "residential": ([1.3, 2.2, 3.5, 5.5], [0.30, 0.38, 0.22, 0.10]),
    "commercial":  ([5.5, 7.7, 11.0, 16.5], [0.30, 0.30, 0.25, 0.15]),
    "government":  ([7.7, 11.0, 16.5, 23.0], [0.25, 0.30, 0.30, 0.15]),
    "industrial":  ([23.0, 33.0, 41.5, 55.0], [0.30, 0.30, 0.25, 0.15]),
}
PEAK_UTILISATION = {"residential": 0.63, "commercial": 0.65, "government": 0.57, "industrial": 0.72}
POWER_FACTOR_BASE = {"residential": (0.945, 0.020), "commercial": (0.895, 0.035),
                     "government": (0.915, 0.030), "industrial": (0.845, 0.050)}
TEMPERATURE_SENSITIVITY = {"residential": 0.015, "commercial": 0.030, "government": 0.030, "industrial": 0.010}
WEEKEND_FACTOR = {"residential": 1.08, "commercial": 0.78, "government": 0.28, "industrial": 0.55}
LOAD_SHAPES = {   # fraction of the daily peak, hour 0..23 local time
    "residential": [0.30, 0.27, 0.25, 0.25, 0.27, 0.35, 0.50, 0.55, 0.45, 0.38, 0.35, 0.36,
                    0.40, 0.38, 0.36, 0.38, 0.45, 0.62, 0.85, 0.95, 1.00, 0.90, 0.70, 0.45],
    "commercial":  [0.20, 0.18, 0.18, 0.18, 0.20, 0.25, 0.35, 0.55, 0.75, 0.88, 0.95, 1.00,
                    0.95, 0.95, 0.98, 0.95, 0.90, 0.85, 0.80, 0.70, 0.55, 0.40, 0.30, 0.25],
    "government":  [0.15, 0.15, 0.15, 0.15, 0.15, 0.18, 0.25, 0.50, 0.85, 0.95, 1.00, 1.00,
                    0.80, 0.95, 1.00, 0.95, 0.70, 0.40, 0.25, 0.20, 0.18, 0.17, 0.16, 0.15],
    "industrial":  [0.55, 0.55, 0.55, 0.55, 0.55, 0.60, 0.70, 0.85, 0.95, 1.00, 1.00, 0.98,
                    0.90, 0.98, 1.00, 0.98, 0.92, 0.80, 0.70, 0.65, 0.60, 0.58, 0.56, 0.55],
}
SHAPE_TABLE = np.array([LOAD_SHAPES[t] for t in CUSTOMER_TYPES])          # (4, 24)
TYPE_INDEX = {t: i for i, t in enumerate(CUSTOMER_TYPES)}


def phase_divisor(phase):
    """Current per phase: single phase I = S/V ; three phase I = S/(3V)."""
    return np.where(np.asarray(phase) == "three", 3.0, 1.0)


# ---------------------------------------------------------------------------
# Anomaly types (the target of the models)
# ---------------------------------------------------------------------------
NORMAL = "Normal"
ANOMALY_TYPES = ["Normal", "Voltage Sag", "Over-Voltage", "Frequency Deviation",
                 "Overload", "Energy Theft", "Meter Fault"]
ANOMALIES = ANOMALY_TYPES[1:]
ANOMALY_INDEX = {a: i for i, a in enumerate(ANOMALY_TYPES)}
ANOMALY_TET = {
    "Normal": "Normal",
    "Voltage Sag": "Voltajen Tun",
    "Over-Voltage": "Voltajen Sa'e Liu",
    "Frequency Deviation": "Frekuénsia La Estável",
    "Overload": "Karga Liu",
    "Energy Theft": "Na'ok Eletrisidade",
    "Meter Fault": "Kontador Aat",
}
# what happens physically + who must act
ANOMALY_INFO = {
    "Voltage Sag": ("Feeder voltage drops 14-35 V for minutes to hours (fault, overloaded transformer, "
                    "long rural line). Affects every meter on the feeder.", "Grid operator / substation"),
    "Over-Voltage": ("Voltage rises above +10 % at night when the feeder is lightly loaded "
                     "(tap changer, capacitor bank). Damages appliances.", "Grid operator"),
    "Frequency Deviation": ("Grid frequency leaves 49.5-50.5 Hz after a generator trip or load "
                            "rejection at the power station. Affects a whole grid.", "Power station"),
    "Overload": ("Customer draws more than the contracted kVA (load ratio > 100 %). Risk of fuse "
                 "or transformer damage.", "Customer service"),
    "Energy Theft": ("Meter bypass / tampering: the meter registers only 5-55 % of the real "
                     "consumption for days or weeks (non-technical loss).", "Revenue protection team"),
    "Meter Fault": ("Current-transformer channel fault: the measured current no longer matches "
                    "power, voltage and power factor.", "Meter maintenance"),
}
ANOMALY_SEVERITY = {"Voltage Sag": "High", "Over-Voltage": "Medium", "Frequency Deviation": "High",
                    "Overload": "High", "Energy Theft": "High", "Meter Fault": "Medium"}
# when two events overlap, the label is the first one in this list
LABEL_PRIORITY = ["Meter Fault", "Energy Theft", "Frequency Deviation", "Voltage Sag", "Over-Voltage", "Overload"]
ANOMALY_COLORS = {
    "Normal": "#94a3b8", "Voltage Sag": "#2563eb", "Over-Voltage": "#7c3aed",
    "Frequency Deviation": "#0891b2", "Overload": "#ea580c", "Energy Theft": "#dc2626",
    "Meter Fault": "#ca8a04",
}


def assign_label(masks: dict[str, np.ndarray], shape) -> np.ndarray:
    """Combine boolean event masks into one label index per reading (priority order)."""
    label = np.zeros(shape, dtype=np.int8)
    for name in reversed(LABEL_PRIORITY):           # lowest priority first, overwritten by higher ones
        m = masks.get(name)
        if m is not None:
            label = np.where(m, ANOMALY_INDEX[name], label)
    return label


# ---------------------------------------------------------------------------
# Meter baseline = normal power of THIS meter at this hour (utility load profile)
# ---------------------------------------------------------------------------
def baseline_kw(customer_type, contracted_kva, peak_util, pf_base, hour, is_weekend, temperature):
    """Expected active power (kW) of a meter: contract x typical utilisation x daily shape
    x weekend factor x temperature effect x power factor. Vectorised (numpy / pandas)."""
    ct = np.asarray(customer_type)
    ti = np.vectorize(TYPE_INDEX.get)(ct) if ct.ndim else TYPE_INDEX[str(ct)]
    h = np.asarray(hour, dtype=int)
    shape = SHAPE_TABLE[ti, h]
    wk = np.where(np.asarray(is_weekend, dtype=int) == 1,
                  np.vectorize(WEEKEND_FACTOR.get)(ct) if ct.ndim else WEEKEND_FACTOR[str(ct)], 1.0)
    beta = np.vectorize(TEMPERATURE_SENSITIVITY.get)(ct) if ct.ndim else TEMPERATURE_SENSITIVITY[str(ct)]
    temp_f = np.clip(1 + beta * (np.asarray(temperature, dtype=float) - 27.0), 0.85, 1.45)
    return (np.asarray(contracted_kva, dtype=float) * np.asarray(peak_util, dtype=float) * shape * wk * temp_f
            * np.asarray(pf_base, dtype=float))


# ---------------------------------------------------------------------------
# Model features
# ---------------------------------------------------------------------------
NUMERIC_FEATURES = [
    "voltage_v", "voltage_dev_pct", "current_a", "power_kw", "power_factor",
    "frequency_hz", "freq_dev_hz", "temperature_c", "contracted_kva",
    "load_ratio", "baseline_ratio", "consistency_ratio", "hour", "is_weekend",
]
CATEGORICAL_FEATURES = ["customer_type", "municipality"]
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET = "anomaly_type"

FEATURE_LABELS = {
    "voltage_v": "Voltage (V)", "voltage_dev_pct": "Voltage deviation from 220 V (%)",
    "current_a": "Current (A)", "power_kw": "Active power (kW)", "power_factor": "Power factor",
    "frequency_hz": "Frequency (Hz)", "freq_dev_hz": "Frequency deviation from 50 Hz",
    "temperature_c": "Temperature (°C)", "contracted_kva": "Contracted power (kVA)",
    "load_ratio": "Load ratio (S / contracted kVA)", "baseline_ratio": "Power / meter baseline",
    "consistency_ratio": "P / (V·I·PF) consistency", "hour": "Hour of day", "is_weekend": "Weekend",
    "customer_type": "Customer type", "municipality": "Municipality",
}
FEATURE_TET = {
    "voltage_v": "Voltajen (V)", "voltage_dev_pct": "Devisaun voltajen (%)", "current_a": "Korrente (A)",
    "power_kw": "Poténsia (kW)", "power_factor": "Fator poténsia", "frequency_hz": "Frekuénsia (Hz)",
    "freq_dev_hz": "Devisaun frekuénsia (Hz)", "temperature_c": "Temperatura (°C)",
    "contracted_kva": "Kontratu (kVA)", "load_ratio": "Rasiu karga", "baseline_ratio": "Rasiu ho baseline",
    "consistency_ratio": "Konsisténsia P/(V·I·PF)", "hour": "Oras", "is_weekend": "Fin-de-semana",
    "customer_type": "Tipu kliente", "municipality": "Munisípiu",
}

# Valid physical ranges used by the cleaning step (veracity)
VALID_RANGES = {
    "voltage_v": (150.0, 270.0),
    "current_a": (0.0, 400.0),
    "power_kw": (0.0, 120.0),
    "energy_kwh": (0.0, 120.0),
    "power_factor": (0.30, 1.00),
    "frequency_hz": (45.0, 55.0),
    "temperature_c": (5.0, 45.0),
    "contracted_kva": (0.5, 100.0),
}
RATIO_CAP = 20.0


def add_features_pandas(df, meters=None):
    """Feature engineering for a pandas DataFrame of readings (stream fallback, website, tests).
    `meters` (meter_id, peak_util, pf_base) is joined when those columns are missing.
    The Spark version is common.spark.add_features_spark - same formulas."""
    import pandas as pd

    out = df.copy()
    if meters is not None and "peak_util" not in out.columns:
        out = out.merge(meters[["meter_id", "peak_util", "pf_base"]], on="meter_id", how="left")
    ts = pd.to_datetime(out["timestamp"])
    out["hour"] = ts.dt.hour
    out["is_weekend"] = (ts.dt.dayofweek >= 5).astype(int)
    pf = out["power_factor"].clip(lower=0.3)
    out["load_ratio"] = ((out["power_kw"] / pf) / out["contracted_kva"]).round(4)
    out["voltage_dev_pct"] = ((out["voltage_v"] - NOMINAL_VOLTAGE) / NOMINAL_VOLTAGE * 100).round(3)
    out["freq_dev_hz"] = (out["frequency_hz"] - NOMINAL_FREQUENCY).round(3)
    for col in ("peak_util", "pf_base"):              # unknown meter -> typical values of its customer type
        if col not in out.columns:
            out[col] = np.nan
    peak = out["peak_util"].astype(float).fillna(out["customer_type"].map(PEAK_UTILISATION))
    pfb = out["pf_base"].astype(float).fillna(out["customer_type"].map(lambda t: POWER_FACTOR_BASE[t][0]))
    base = baseline_kw(out["customer_type"].to_numpy(), out["contracted_kva"].to_numpy(), peak.to_numpy(),
                       pfb.to_numpy(), out["hour"].to_numpy(), out["is_weekend"].to_numpy(),
                       out["temperature_c"].to_numpy())
    out["baseline_kw"] = np.round(base, 4)
    out["baseline_ratio"] = (out["power_kw"] / np.maximum(base, 0.01)).clip(0, RATIO_CAP).round(4)
    div = phase_divisor(out["phase"].to_numpy()) if "phase" in out else 1.0
    vip = out["voltage_v"] * out["current_a"] * pf * div / 1000.0
    cons = np.where(vip > 1e-6, out["power_kw"] / np.maximum(vip, 1e-6), np.where(out["power_kw"] > 0.01, RATIO_CAP, 1.0))
    out["consistency_ratio"] = np.clip(cons, 0, RATIO_CAP).round(4)
    return out


def explain_anomaly(r) -> list[str]:
    """Human-readable evidence for a reading (works with a dict, Series or namedtuple)."""
    g = (lambda k, d=None: r.get(k, d)) if hasattr(r, "get") else (lambda k, d=None: getattr(r, k, d))
    out = []
    v, f = g("voltage_v"), g("frequency_hz")
    if v is not None:
        dev = (v - NOMINAL_VOLTAGE) / NOMINAL_VOLTAGE * 100
        if dev <= -10:
            out.append(f"Voltage {v:.1f} V ({dev:+.1f}% husi 220 V)")
        elif dev >= 10:
            out.append(f"Voltage {v:.1f} V ({dev:+.1f}% husi 220 V)")
    if f is not None and abs(f - NOMINAL_FREQUENCY) > 0.5:
        out.append(f"Frequency {f:.2f} Hz (normal 49.5-50.5)")
    lr = g("load_ratio")
    if lr is not None and lr > 1.0:
        out.append(f"Load {lr * 100:.0f}% husi contracted kVA")
    br = g("baseline_ratio")
    if br is not None and br < 0.45:
        out.append(f"Consumption {br * 100:.0f}% de'it husi meter baseline (suspeita Na'ok Eletrisidade)")
    cr = g("consistency_ratio")
    if cr is not None and (cr < 0.7 or cr > 1.4):
        out.append(f"Current la tuir P/(V·PF): ratio {cr:.2f} (sensor aat)")
    return out
