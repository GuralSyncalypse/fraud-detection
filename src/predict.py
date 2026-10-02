"""Load a saved Spark model and score new unlabeled transactions from HDFS."""

from __future__ import annotations

import argparse
import csv
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pyspark.ml.classification import (
    LogisticRegressionModel,
    RandomForestClassificationModel,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.utils import AnalysisException

from feature_engineering import prepare_inference_features


DEFAULT_INPUT = "hdfs:///financial/raw/new_transactions.csv"
DEFAULT_OUTPUT = "hdfs:///financial/predictions/new_transactions"
DEFAULT_MODEL_BASE = "hdfs:///financial/models"
DEFAULT_MODEL = "rf_undersampling"
DEFAULT_THRESHOLD = 0.5
DEFAULT_REPORT = "/workspace/output/results/new_prediction_summary.json"
DEFAULT_CSV = "/workspace/output/results/new_transactions_predictions.csv"

MODEL_LOADERS = {
    "lr_none": ("logistic_regression/none", LogisticRegressionModel),
    "lr_class_weight": (
        "logistic_regression/class_weight",
        LogisticRegressionModel,
    ),
    "rf_none": ("random_forest/none", RandomForestClassificationModel),
    "rf_class_weight": (
        "random_forest/class_weight",
        RandomForestClassificationModel,
    ),
    "rf_undersampling": (
        "random_forest/undersampling",
        RandomForestClassificationModel,
    ),
}

NEW_TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.LongType(), True),
        T.StructField("transaction_time", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("use_chip", T.StringType(), True),
        T.StructField("merchant_state", T.StringType(), True),
        T.StructField("mcc", T.IntegerType(), True),
        T.StructField("errors", T.StringType(), True),
    ]
)


def read_and_validate_new_transactions(
    spark: SparkSession, input_path: str
) -> DataFrame:
    raw = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(NEW_TRANSACTION_SCHEMA)
        .csv(input_path)
    )
    parsed = raw.withColumn(
        "transaction_time",
        F.to_timestamp(F.trim(F.col("transaction_time")), "yyyy-MM-dd HH:mm:ss"),
    )
    invalid = parsed.where(
        F.col("transaction_id").isNull()
        | F.col("transaction_time").isNull()
        | F.col("amount").isNull()
        | F.isnan(F.col("amount"))
        | (F.col("amount") < 0)
        | F.col("mcc").isNull()
    ).count()
    if invalid:
        raise ValueError(f"New transaction input contains {invalid} invalid rows")
    duplicate_ids = (
        parsed.groupBy("transaction_id")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if duplicate_ids:
        raise ValueError(f"New transaction input contains {duplicate_ids} duplicate IDs")
    return parsed


def read_phase4_selection(spark: SparkSession, model_base: str) -> dict[str, Any]:
    """Load validation-based defaults; keep old artifacts usable."""
    try:
        rows = spark.read.text(f"{model_base}/metadata/phase4_summary.json").collect()
    except AnalysisException:
        return {}
    payload = json.loads("\n".join(row["value"] for row in rows))
    return payload.get("selection", {})


def write_local_outputs(
    rows: Iterable[Any], report: dict[str, Any], csv_path: str, report_path: str
) -> None:
    csv_destination = Path(csv_path)
    csv_destination.parent.mkdir(parents=True, exist_ok=True)
    csv_temp = csv_destination.with_suffix(csv_destination.suffix + ".tmp")
    fieldnames = [
        "transaction_id",
        "transaction_time",
        "amount",
        "fraud_probability",
        "prediction",
        "status",
        "model",
        "threshold",
    ]
    with csv_temp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            values = row.asDict()
            values["transaction_time"] = values["transaction_time"].isoformat(
                sep=" "
            )
            writer.writerow(values)
    os.replace(csv_temp, csv_destination)

    report_destination = Path(report_path)
    report_destination.parent.mkdir(parents=True, exist_ok=True)
    report_temp = report_destination.with_suffix(report_destination.suffix + ".tmp")
    report_temp.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(report_temp, report_destination)


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


def run_prediction(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    model_base: str,
    model_slug: str | None,
    threshold: float | None,
    report_path: str,
    csv_path: str,
) -> dict[str, Any]:
    selection = read_phase4_selection(spark, model_base)
    model_slug = model_slug or selection.get("recommended_model", DEFAULT_MODEL)
    if threshold is None:
        threshold = float(
            selection.get("thresholds_by_model", {}).get(model_slug, DEFAULT_THRESHOLD)
        )
    if model_slug not in MODEL_LOADERS:
        raise ValueError(f"Unknown model {model_slug}; choose from {sorted(MODEL_LOADERS)}")
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("threshold must be in [0, 1]")

    resolved_input_path = input_path
    if not input_path.startswith(("hdfs://", "viewfs://")):
        resolved_input_path = f"hdfs:///financial/raw/dashboard_upload_{uuid.uuid4().hex}.csv"
        copy_local_file_to_hdfs(
            spark,
            input_path.removeprefix("file://"),
            resolved_input_path,
        )
    raw = read_and_validate_new_transactions(spark, resolved_input_path)
    input_count = raw.count()
    preprocessor = PipelineModel.load(f"{model_base}/preprocessor")
    relative_path, loader = MODEL_LOADERS[model_slug]
    model = loader.load(f"{model_base}/{relative_path}")

    inference_features = prepare_inference_features(raw)
    output = (
        model.transform(preprocessor.transform(inference_features))
        .withColumn("fraud_probability", vector_to_array("probability")[1])
        .withColumn(
            "prediction",
            (F.col("fraud_probability") >= F.lit(float(threshold))).cast("int"),
        )
        .withColumn(
            "status",
            F.when(F.col("prediction") == 1, F.lit("NEEDS_REVIEW")).otherwise(
                F.lit("NORMAL")
            ),
        )
        .withColumn("model", F.lit(model_slug))
        .withColumn("threshold", F.lit(float(threshold)))
        .select(
            "transaction_id",
            "transaction_time",
            "amount",
            "fraud_probability",
            "prediction",
            "status",
            "model",
            "threshold",
        )
    )
    output.write.mode("overwrite").parquet(output_path)
    reloaded = spark.read.parquet(output_path)
    summary = reloaded.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("prediction") == 1, 1).otherwise(0)).alias("flagged"),
    ).first()
    if int(summary["rows"]) != input_count:
        raise RuntimeError("New prediction row count changed after HDFS save/reload")

    flagged_count = int(summary["flagged"] or 0)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "input_rows": input_count,
        "model": model_slug,
        "model_path": f"{model_base}/{relative_path}",
        "preprocessor_path": f"{model_base}/preprocessor",
        "threshold": threshold,
        "flagged_rows": flagged_count,
        "normal_rows": input_count - flagged_count,
        "output_path": output_path,
        "class_column_required": False,
        "model_retrained": False,
    }
    # ponytail: stream one Spark partition at a time; add a paged export when CSVs exceed local disk limits.
    write_local_outputs(
        reloaded.orderBy(F.desc("fraud_probability")).toLocalIterator(),
        report,
        csv_path,
        report_path,
    )
    copy_local_file_to_hdfs(
        spark,
        report_path,
        "hdfs:///financial/predictions/metadata/new_prediction_summary.json",
    )
    print(json.dumps(report, indent=2))
    print(f"[phase5] PASS new predictions saved to {output_path}")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--model-base", default=DEFAULT_MODEL_BASE)
    parser.add_argument("--model", default=None, choices=sorted(MODEL_LOADERS))
    parser.add_argument("--threshold", type=float, default=None)
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--csv", default=DEFAULT_CSV)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("financial-fraud-new-prediction").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_prediction(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            model_base=args.model_base,
            model_slug=args.model,
            threshold=args.threshold,
            report_path=args.report,
            csv_path=args.csv,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
