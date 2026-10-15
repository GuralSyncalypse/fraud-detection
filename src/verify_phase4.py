"""Verify Phase 4 reports and HDFS prediction artifacts in a new Spark session."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


REPORT_PATH = Path("/workspace/output/results/phase4_summary.json")
EXPECTED_MODELS = {
    "lr_none",
    "lr_class_weight",
    "rf_none",
    "rf_class_weight",
    "rf_undersampling",
    "isolation_forest",
}


def main() -> None:
    report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    expected_rows = int(report["evaluation_scope"]["rows"])
    reported_models = {row["slug"] for row in report["model_comparison"]}
    if reported_models != EXPECTED_MODELS:
        raise RuntimeError(
            f"Unexpected model set: expected={EXPECTED_MODELS}, actual={reported_models}"
        )
    selection = report.get("selection", {})
    classifier_models = EXPECTED_MODELS - {"isolation_forest"}
    if selection.get("recommended_model") not in classifier_models:
        raise RuntimeError("Missing or invalid validation-based model selection")
    thresholds = selection.get("thresholds_by_model", {})
    if set(thresholds) != classifier_models or any(
        not 0.0 <= float(value) <= 1.0 for value in thresholds.values()
    ):
        raise RuntimeError("Invalid validation-based thresholds")
    reported_thresholds = {
        row["slug"]: float(row["decision_threshold"])
        for row in report["model_comparison"]
        if row["slug"] in classifier_models
    }
    if reported_thresholds != {key: float(value) for key, value in thresholds.items()}:
        raise RuntimeError("Model thresholds do not match selection metadata")

    for metrics in report["model_comparison"]:
        confusion_total = sum(int(metrics[name]) for name in ("tp", "tn", "fp", "fn"))
        if confusion_total != expected_rows:
            raise RuntimeError(f"Confusion matrix mismatch for {metrics['slug']}")
        for name in ("precision", "recall", "f1", "pr_auc", "accuracy"):
            if not 0.0 <= float(metrics[name]) <= 1.0:
                raise RuntimeError(f"Invalid {name} for {metrics['slug']}")

    spark = SparkSession.builder.appName("phase-4-artifact-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        checked: dict[str, dict[str, int]] = {}
        for slug, path in report["prediction_paths"].items():
            frame = spark.read.parquet(path)
            score_col = (
                "anomaly_score" if slug == "isolation_forest" else "fraud_probability"
            )
            required = {
                "transaction_id",
                "transaction_time",
                "amount",
                score_col,
                "prediction",
                "status",
                "actual_class",
                "model",
            }
            missing = required.difference(frame.columns)
            if missing:
                raise RuntimeError(f"{slug} missing columns: {sorted(missing)}")
            row = frame.agg(
                F.count("*").alias("rows"),
                F.sum(
                    F.when(
                        F.col("transaction_id").isNull()
                        | F.col("transaction_time").isNull()
                        | F.col(score_col).isNull(),
                        1,
                    ).otherwise(0)
                ).alias("invalid_rows"),
                F.sum(F.when(F.col("prediction") == 1, 1).otherwise(0)).alias(
                    "flagged_rows"
                ),
            ).first()
            if int(row["rows"]) != expected_rows or int(row["invalid_rows"] or 0):
                raise RuntimeError(f"Invalid saved predictions for {slug}: {row.asDict()}")
            checked[slug] = {
                "rows": int(row["rows"]),
                "flagged_rows": int(row["flagged_rows"] or 0),
            }

        print(
            json.dumps(
                {
                    "status": "PASS",
                    "new_spark_application": True,
                    "test_rows": expected_rows,
                    "models": checked,
                    "report": str(REPORT_PATH),
                },
                indent=2,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
