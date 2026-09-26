"""Verify final charts, suspicious transactions and new-data predictions."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, functions as F


FIGURE_DIR = Path("/workspace/output/figures")
RESULT_DIR = Path("/workspace/output/results")
EXPECTED_FIGURES = {
    "01_class_distribution.png",
    "02_amount_distribution.png",
    "03_fraud_amount_distribution.png",
    "04_confusion_matrix.png",
    "05_metric_comparison.png",
    "06_precision_recall_curve.png",
    "07_rf_feature_importance.png",
    "08_probability_distribution.png",
    "09_threshold_analysis.png",
    "10_top_suspicious_transactions.png",
}


def write_json_atomic(payload: dict[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary, destination)


def copy_local_file_to_hdfs(
    spark: SparkSession, local_path: str, hdfs_path: str
) -> None:
    jvm = spark._jvm
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    destination = jvm.org.apache.hadoop.fs.Path(hdfs_path)
    filesystem = destination.getFileSystem(configuration)
    filesystem.mkdirs(destination.getParent())
    filesystem.copyFromLocalFile(
        False,
        True,
        jvm.org.apache.hadoop.fs.Path(f"file://{local_path}"),
        destination,
    )


def main() -> None:
    missing_figures = sorted(
        name
        for name in EXPECTED_FIGURES
        if not (FIGURE_DIR / name).is_file()
        or (FIGURE_DIR / name).stat().st_size < 5_000
    )
    if missing_figures:
        raise RuntimeError(f"Missing or invalid figures: {missing_figures}")

    visualization_report = json.loads(
        (RESULT_DIR / "visualization_summary.json").read_text(encoding="utf-8")
    )
    prediction_report = json.loads(
        (RESULT_DIR / "new_prediction_summary.json").read_text(encoding="utf-8")
    )
    spark = SparkSession.builder.appName("phase-5-artifact-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        new_predictions = spark.read.parquet(
            "hdfs:///financial/predictions/new_transactions"
        )
        required_columns = {
            "transaction_id",
            "transaction_time",
            "amount",
            "fraud_probability",
            "prediction",
            "status",
            "model",
            "threshold",
        }
        missing_columns = required_columns.difference(new_predictions.columns)
        if missing_columns:
            raise RuntimeError(
                f"New predictions missing columns: {sorted(missing_columns)}"
            )
        new_row = new_predictions.agg(
            F.count("*").alias("rows"),
            F.sum(F.when(F.col("prediction") == 1, 1).otherwise(0)).alias(
                "flagged"
            ),
            F.sum(
                F.when(
                    F.col("transaction_id").isNull()
                    | F.col("transaction_time").isNull()
                    | F.col("fraud_probability").isNull(),
                    1,
                ).otherwise(0)
            ).alias("invalid"),
        ).first()
        if int(new_row["rows"]) != int(prediction_report["input_rows"]):
            raise RuntimeError("New prediction row count mismatch")
        if int(new_row["invalid"] or 0):
            raise RuntimeError("New prediction output contains null required fields")

        suspicious = spark.read.parquet(
            "hdfs:///financial/predictions/suspicious_transactions"
        )
        suspicious_row = suspicious.agg(
            F.count("*").alias("rows"),
            F.sum(
                F.when(F.col("status") != "NEEDS_REVIEW", 1).otherwise(0)
            ).alias("invalid_status"),
        ).first()
        if int(suspicious_row["rows"]) != 20 or int(
            suspicious_row["invalid_status"] or 0
        ):
            raise RuntimeError("Suspicious transaction artifact is invalid")

        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "PASS",
            "new_spark_application": True,
            "figures": sorted(EXPECTED_FIGURES),
            "new_prediction": {
                "rows": int(new_row["rows"]),
                "flagged_rows": int(new_row["flagged"] or 0),
                "model_retrained": prediction_report["model_retrained"],
            },
            "top_suspicious_rows": int(suspicious_row["rows"]),
            "spark_aggregation_before_pandas": visualization_report[
                "spark_aggregation_before_pandas"
            ],
            "full_dataset_to_pandas": visualization_report[
                "full_dataset_to_pandas"
            ],
        }
        destination = RESULT_DIR / "phase5_summary.json"
        write_json_atomic(summary, destination)
        copy_local_file_to_hdfs(
            spark,
            str(destination),
            "hdfs:///financial/predictions/metadata/phase5_summary.json",
        )
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
