"""
Distributed anomaly detection with Spark MLlib on the FULL curated dataset
(millions of readings) - the Big Data version of the Random Forest.

Train meters  -> anomaly rows (up to a cap per type) + a sample of Normal rows (majority undersampling)
Test meters   -> ALL their readings, natural distribution (held out by meter)
Pipeline      : StringIndexer + OneHotEncoder -> VectorAssembler -> RandomForestClassifier
Metrics       : accuracy, macro F1 over the 7 types, anomaly-vs-normal precision / recall / F1,
                confusion matrix, feature importance -> tbl_models (engine = spark-mllib)

    python -m pipeline.spark_ml_train --dataset 20GB
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from common import db  # noqa: E402
from common import domain as D  # noqa: E402
from common.spark import get_spark, read_parquet  # noqa: E402

from pyspark import StorageLevel  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import RandomForestClassifier  # noqa: E402
from pyspark.ml.feature import IndexToString, OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from sqlalchemy import insert  # noqa: E402


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="20GB")
    ap.add_argument("--normal-rows", type=int, default=1_700_000, help="Normal rows kept for training")
    ap.add_argument("--cap-per-type", type=int, default=100_000, help="max training rows per anomaly type")
    ap.add_argument("--normal-weight", type=float, default=1.0,
                    help="optional weight of Normal rows (1 = off; on 20GB weight 2 gave a lower F1)")
    ap.add_argument("--trees", type=int, default=80)
    ap.add_argument("--depth", type=int, default=18)
    ap.add_argument("--bins", type=int, default=128, help="maxBins: thresholds tested per feature")
    args = ap.parse_args()

    run_id = db.start_run("spark_mllib_train", "spark-mllib", args.dataset)
    spark = get_spark(f"PowerPredict-MLlib-{args.dataset}")
    try:
        df = read_parquet(spark, config.CURATED_DIR / f"readings_{args.dataset}")
        df = df.select(*D.FEATURES, "anomaly_type", "is_test_meter")
        train_all = df.filter(F.col("is_test_meter") == 0)
        counts = {r["anomaly_type"]: int(r["count"]) for r in train_all.groupBy("anomaly_type").count().collect()}
        fractions = {a: min(1.0, (args.normal_rows if a == D.NORMAL else args.cap_per_type) / max(n, 1))
                     for a, n in counts.items()}
        train = (train_all.sampleBy("anomaly_type", fractions, seed=42)
                 .withColumn("w", F.when(F.col("anomaly_type") == D.NORMAL, F.lit(args.normal_weight)).otherwise(F.lit(1.0)))
                 .persist(StorageLevel.MEMORY_AND_DISK))
        test = df.filter(F.col("is_test_meter") == 1).persist(StorageLevel.MEMORY_AND_DISK)
        n_train, n_test = train.count(), test.count()
        log(f"MLlib: train {n_train:,} rows | test {n_test:,} rows (held-out meters) | "
            f"{spark.sparkContext.defaultParallelism} cores")

        label_idx = StringIndexer(inputCol="anomaly_type", outputCol="label").fit(train)
        cat_idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in D.CATEGORICAL_FEATURES]
        ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in D.CATEGORICAL_FEATURES],
                            outputCols=[f"{c}_ohe" for c in D.CATEGORICAL_FEATURES])
        assembler = VectorAssembler(inputCols=D.NUMERIC_FEATURES + [f"{c}_ohe" for c in D.CATEGORICAL_FEATURES],
                                    outputCol="features")
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="w",
                                    numTrees=args.trees, maxDepth=args.depth,
                                    impurity="gini", featureSubsetStrategy="sqrt", subsamplingRate=0.8, maxBins=args.bins, seed=42)
        to_str = IndexToString(inputCol="prediction", outputCol="predicted_type", labels=label_idx.labels)
        t = time.time()
        model = Pipeline(stages=[label_idx, *cat_idx, ohe, assembler, rf, to_str]).fit(train)
        secs = time.time() - t
        log(f"trained {args.trees} trees (depth {args.depth}) in {secs:.1f}s")

        pred = model.transform(test).select("anomaly_type", "predicted_type").persist()
        cm = {(r["anomaly_type"], r["predicted_type"]): int(r["count"])
              for r in pred.groupBy("anomaly_type", "predicted_type").count().collect()}
        pred.unpersist()
        labels = D.ANOMALY_TYPES
        matrix = [[cm.get((a, b), 0) for b in labels] for a in labels]
        total = sum(map(sum, matrix))
        correct = sum(matrix[i][i] for i in range(len(labels)))
        per = {}
        for i, a in enumerate(labels):
            tp = matrix[i][i]
            col = sum(matrix[r][i] for r in range(len(labels)))
            row = sum(matrix[i])
            p = tp / col if col else 0.0
            r_ = tp / row if row else 0.0
            per[a] = {"precision": p, "recall": r_, "f1": 2 * p * r_ / (p + r_) if p + r_ else 0.0, "support": row}
        n0 = D.ANOMALY_INDEX[D.NORMAL]
        tn = matrix[n0][n0]
        fp = sum(matrix[n0]) - tn
        fn = sum(matrix[i][n0] for i in range(len(labels)) if i != n0)
        tp = total - tn - fp - fn
        ap_ = tp / (tp + fp) if tp + fp else 0.0
        ar_ = tp / (tp + fn) if tp + fn else 0.0
        af1 = 2 * ap_ * ar_ / (ap_ + ar_) if ap_ + ar_ else 0.0

        vec = model.stages[-2].featureImportances.toArray()
        meta = model.transform(test.limit(1)).schema["features"].metadata["ml_attr"]["attrs"]
        imp: dict[str, float] = {}
        for a in sorted([a for k in meta for a in meta[k]], key=lambda a: a["idx"]):
            base = next((c for c in D.CATEGORICAL_FEATURES if a["name"].startswith(c + "_ohe")), a["name"])
            imp[base] = imp.get(base, 0.0) + float(vec[a["idx"]])
        tot = sum(imp.values()) or 1
        imp = dict(sorted(((k, round(v / tot * 100, 3)) for k, v in imp.items()), key=lambda kv: -kv[1]))

        with db.begin() as conn:
            conn.execute(insert(db.models).values(
                task="supervised", algorithm="random_forest", engine="spark-mllib", dataset=args.dataset,
                params=db.to_json({"numTrees": args.trees, "maxDepth": args.depth, "maxBins": args.bins,
                                   "normal_rows": args.normal_rows, "cap_per_type": args.cap_per_type, "normal_weight": args.normal_weight, "featureSubsetStrategy": "sqrt"}),
                n_train=n_train, n_test=n_test, accuracy=correct / total,
                precision_macro=sum(v["precision"] for v in per.values()) / len(per),
                recall_macro=sum(v["recall"] for v in per.values()) / len(per),
                f1_macro=sum(v["f1"] for v in per.values()) / len(per),
                anomaly_precision=ap_, anomaly_recall=ar_, anomaly_f1=af1, false_positive_rate=fp / max(fp + tn, 1),
                train_seconds=round(secs, 2), feature_importance=db.to_json(imp),
                confusion_matrix=db.to_json({"labels": labels, "matrix": matrix,
                                             "binary": {"tn": tn, "fp": fp, "fn": fn, "tp": tp}}),
                class_report=db.to_json({"per_class": per, "cores": spark.sparkContext.defaultParallelism,
                                         "natural_anomaly_rate": 1 - sum(matrix[n0]) / total}),
                is_active=False,
                notes=f"Spark MLlib distributed training on {n_train:,} rows; tested on ALL {n_test:,} readings of held-out meters"))
        log(f"accuracy {correct / total * 100:.2f}% | anomaly P {ap_:.4f} R {ar_:.4f} F1 {af1:.4f}")
        db.finish_run(run_id, "success", input_rows=n_train + n_test, output_rows=n_test,
                      details={"accuracy": correct / total, "anomaly_f1": af1, "train_seconds": secs,
                               "n_train": n_train, "n_test": n_test},
                      message=f"MLlib Random Forest on {n_train:,} train rows")
    except Exception as exc:
        db.finish_run(run_id, "failed", message=str(exc)[:2000])
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
