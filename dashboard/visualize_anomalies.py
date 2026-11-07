"""
visualize_anomalies.py - Vizualizasaun deteksaun anomalia (PNG ba relatóriu)

  anomaly_pca_2d.png        espasu fitur (PCA 2D) kór tuir tipu anomalia (ground truth)
  anomaly_3d.png            devisaun voltajen x rasiu baseline x konsisténsia P/(V.I.PF)
  confusion_matrix.png      Random Forest ativu iha test sample (meters separadu)
  feature_importance.png    importánsia Gini kada fitur

Hala'o (husi folder projetu):
    python dashboard/visualize_anomalies.py --dataset 20GB
"""
import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.decomposition import PCA  # noqa: E402
from sklearn.metrics import confusion_matrix  # noqa: E402
from sklearn.preprocessing import StandardScaler  # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402
from ml.predict import load_active, predict_frame  # noqa: E402
from ml.train import read_parquet_folder  # noqa: E402

OUT = HERE / "anomaly_output"
PALETTE = dict(zip(D.ANOMALIES, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]))
PALETTE[D.NORMAL] = "#a3a39e"
plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.alpha": 0.25,
                     "font.size": 10})


def sample_for_plot(test: pd.DataFrame, n_normal: int = 6000, n_per_type: int = 700) -> pd.DataFrame:
    parts = [test[test.anomaly_type == D.NORMAL].sample(min(n_normal, (test.anomaly_type == D.NORMAL).sum()), random_state=1)]
    for a in D.ANOMALIES:
        g = test[test.anomaly_type == a]
        parts.append(g.sample(min(n_per_type, len(g)), random_state=1))
    return pd.concat(parts)


def plot_pca(df: pd.DataFrame, path: Path) -> None:
    X = StandardScaler().fit_transform(df[["voltage_dev_pct", "freq_dev_hz", "power_factor", "load_ratio", "baseline_ratio", "consistency_ratio", "temperature_c"]].clip(upper=5))
    pca = PCA(n_components=2, random_state=42)
    P = pca.fit_transform(X)
    fig, ax = plt.subplots(figsize=(9, 6.5), dpi=130)
    for a in D.ANOMALY_TYPES:
        m = (df.anomaly_type == a).to_numpy()
        ax.scatter(P[m, 0], P[m, 1], s=8 if a == D.NORMAL else 14, alpha=0.35 if a == D.NORMAL else 0.8,
                   color=PALETTE[a], label=f"{D.ANOMALY_TET[a]} ({m.sum():,})", edgecolor="none")
    ax.set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0] * 100:.1f}% varians)")
    ax.set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1] * 100:.1f}% varians)")
    ax.set_title("Feature space (PCA 2D) - kór tuir tipu anomalia (ground truth)")
    ax.legend(frameon=False, markerscale=2, fontsize=8)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_3d(df: pd.DataFrame, path: Path) -> None:
    fig = plt.figure(figsize=(9, 7), dpi=130)
    ax = fig.add_subplot(111, projection="3d")
    for a in D.ANOMALY_TYPES:
        d = df[df.anomaly_type == a]
        ax.scatter(d.voltage_dev_pct, d.baseline_ratio.clip(upper=4), d.consistency_ratio.clip(upper=4),
                   s=5 if a == D.NORMAL else 10, alpha=0.25 if a == D.NORMAL else 0.75, color=PALETTE[a], label=D.ANOMALY_TET[a])
    ax.set_xlabel("Voltage deviation (%)")
    ax.set_ylabel("Power / baseline (baseline ratio)")
    ax.set_zlabel("P / (V·I·PF) (consistency)")
    ax.set_title("Tipu anomalia iha feature prinsipál 3")
    ax.view_init(elev=20, azim=-60)
    ax.legend(frameon=False, loc="upper left", fontsize=8, markerscale=2)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_confusion(test: pd.DataFrame, path: Path) -> None:
    _row, obj = load_active("supervised")
    pred = predict_frame(test, obj)["predicted_type"]
    cm = confusion_matrix(test.anomaly_type, pred, labels=D.ANOMALY_TYPES)
    pct = cm / cm.sum(axis=1, keepdims=True).clip(min=1)
    fig, ax = plt.subplots(figsize=(8.5, 7), dpi=130)
    ax.imshow(pct, cmap="Blues", vmin=0, vmax=1)
    ax.grid(False)
    ax.set_xticks(range(len(D.ANOMALY_TYPES)), [D.ANOMALY_TET[a] for a in D.ANOMALY_TYPES], rotation=35, ha="right")
    ax.set_yticks(range(len(D.ANOMALY_TYPES)), [D.ANOMALY_TET[a] for a in D.ANOMALY_TYPES])
    for i in range(len(cm)):
        for j in range(len(cm)):
            ax.text(j, i, f"{cm[i, j]:,}", ha="center", va="center", fontsize=8,
                    color="white" if pct[i, j] > 0.5 else "#333")
    ax.set_xlabel("Prediction Random Forest")
    ax.set_ylabel("Atuál (ground truth)")
    ax.set_title(f"Confusion Matrix - test set {len(test):,} readings (meters separadu)")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_importance(path: Path) -> None:
    row, _ = load_active("supervised")
    fi = json.loads(row["feature_importance"])
    s = pd.Series(fi).sort_values()
    fig, ax = plt.subplots(figsize=(8, 6), dpi=130)
    ax.barh([D.FEATURE_LABELS.get(k, k) for k in s.index], s.values, color=PALETTE["Voltage Sag"], height=0.7)
    ax.set_xlabel("Feature Importance (Gini, %)")
    ax.set_title(f"Random Forest #{row['id']} - Feature Importance")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", default="20GB")
    args = ap.parse_args()
    OUT.mkdir(exist_ok=True)
    print("[1] Lee test sample (meters separadu) ...")
    test = read_parquet_folder(config.CURATED_DIR / f"test_sample_{args.dataset}")
    df = sample_for_plot(test)
    print(f"    {len(test):,} readings | plot {len(df):,}")
    print("[2] Halo gráfiku ...")
    plot_pca(df, OUT / "anomaly_pca_2d.png")
    plot_3d(df, OUT / "anomaly_3d.png")
    plot_confusion(test, OUT / "confusion_matrix.png")
    plot_importance(OUT / "feature_importance.png")
    print(f"[OK] PNG rai iha {OUT}")


if __name__ == "__main__":
    main()
