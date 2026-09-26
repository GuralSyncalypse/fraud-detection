"""Phase 4: evaluate saved models on the untouched test set and save predictions."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

import joblib
import pandas as pd
from pyspark import SparkFiles, StorageLevel
from pyspark.ml.classification import (
    LogisticRegressionModel,
    RandomForestClassificationModel,
)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from anomaly_detection import ISOLATION_FEATURES
from evaluation import (
    DEFAULT_THRESHOLDS,
    area_under_pr,
    binary_metrics,
    prediction_output,
    threshold_analysis,
    write_csv_rows,
    write_json_atomic,
)
from feature_engineering import extract_feature_names


DEFAULT_TEST = "hdfs:///financial/processed/ml_splits/test"
DEFAULT_LABELED = "hdfs:///financial/processed/labeled_transactions"
DEFAULT_MODEL_BASE = "hdfs:///financial/models"
DEFAULT_PREDICTION_BASE = "hdfs:///financial/predictions"
DEFAULT_REPORT = "/workspace/output/results/phase4_summary.json"
DEFAULT_RESULT_DIR = "/workspace/output/results"

SPARK_MODEL_SPECS = (
    (
        "lr_none",
        "Logistic Regression",
        "None",
        "logistic_regression/none",
        LogisticRegressionModel,
    ),
    (
        "lr_class_weight",
        "Logistic Regression",
        "Class Weight",
        "logistic_regression/class_weight",
        LogisticRegressionModel,
    ),
    (
        "rf_none",
        "Random Forest",
        "None",
        "random_forest/none",
        RandomForestClassificationModel,
    ),
    (
        "rf_class_weight",
        "Random Forest",
        "Class Weight",
        "random_forest/class_weight",
        RandomForestClassificationModel,
    ),
    (
        "rf_undersampling",
        "Random Forest",
        "Undersampling",
        "random_forest/undersampling",
        RandomForestClassificationModel,
    ),
)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("financial-fraud-phase-4").getOrCreate()


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


def _score_isolation_batch(
    model: Any, rows: list[Any]
) -> Iterator[tuple[int, Any, float, int, float, int]]:
    frame = pd.DataFrame(
        [[row[name] for name in ISOLATION_FEATURES] for row in rows],
        columns=ISOLATION_FEATURES,
    )
    decisions = model.decision_function(frame)
    predictions = model.predict(frame)
    for row, decision, prediction in zip(rows, decisions, predictions):
        yield (
            int(row["transaction_id"]),
            row["transaction_time"],
            float(row["amount"]),
            int(row["class"]),
            float(-decision),  # Higher score means more suspicious.
            int(prediction == -1),
        )


def _score_isolation_partition(
    rows: Iterator[Any], model_filename: str, batch_size: int
) -> Iterator[tuple[int, Any, float, int, float, int]]:
    model = joblib.load(SparkFiles.get(model_filename))
    batch: list[Any] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield from _score_isolation_batch(model, batch)
            batch.clear()
    if batch:
        yield from _score_isolation_batch(model, batch)


def score_isolation_forest(
    spark: SparkSession,
    test_features: DataFrame,
    model_path: str,
    batch_size: int,
) -> DataFrame:
    """Distribute the sklearn artifact and score each Spark partition in batches."""
    if batch_size <= 0:
        raise ValueError("isolation batch size must be positive")
    spark.sparkContext.addFile(model_path)
    model_filename = Path(urlparse(model_path).path).name
    selected = test_features.select(
        "transaction_id", "transaction_time", "class", *ISOLATION_FEATURES
    )
    schema = T.StructType(
        [
            T.StructField("transaction_id", T.LongType(), False),
            T.StructField("transaction_time", T.TimestampType(), True),
            T.StructField("amount", T.DoubleType(), False),
            T.StructField("class", T.IntegerType(), False),
            T.StructField("anomaly_score", T.DoubleType(), False),
            T.StructField("prediction", T.IntegerType(), False),
        ]
    )
    scored_rdd = selected.rdd.mapPartitions(
        lambda rows: _score_isolation_partition(rows, model_filename, batch_size)
    )
    return spark.createDataFrame(scored_rdd, schema=schema)


def evaluate_scored_frame(
    predictions: DataFrame,
    score_col: str,
    include_thresholds: bool,
    thresholds: tuple[float, ...],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    metrics = binary_metrics(predictions)
    metrics["pr_auc"] = area_under_pr(predictions, score_col=score_col)
    threshold_rows = (
        threshold_analysis(predictions, score_col=score_col, thresholds=thresholds)
        if include_thresholds
        else []
    )
    return metrics, threshold_rows


def run_phase4(
    spark: SparkSession,
    test_path: str,
    labeled_path: str,
    model_base: str,
    prediction_base: str,
    report_path: str,
    result_dir: str,
    thresholds: tuple[float, ...],
    isolation_batch_size: int,
) -> dict[str, Any]:
    print(f"[phase4] Reading untouched test split from {test_path}")
    test = spark.read.parquet(test_path)
    transaction_times = spark.read.parquet(labeled_path).select(
        "transaction_id", "transaction_time"
    )
    enriched_test = test.join(transaction_times, "transaction_id", "left")
    preprocessor = PipelineModel.load(f"{model_base}/preprocessor")
    test_features = preprocessor.transform(enriched_test).persist(StorageLevel.DISK_ONLY)
    test_count = test_features.count()
    missing_time_count = test_features.where(F.col("transaction_time").isNull()).count()
    if missing_time_count:
        raise RuntimeError(
            f"{missing_time_count} test rows are missing transaction_time after join"
        )
    print(f"[phase4] Test rows={test_count:,}; preprocessing model loaded from HDFS")

    model_results: list[dict[str, Any]] = []
    threshold_results: list[dict[str, Any]] = []
    feature_importance_rows: list[dict[str, Any]] = []
    prediction_paths: dict[str, str] = {}
    feature_names = extract_feature_names(test_features)

    for slug, model_name, imbalance_method, relative_path, loader in SPARK_MODEL_SPECS:
        print(f"[phase4] Evaluating {slug} on the full test set")
        model = loader.load(f"{model_base}/{relative_path}")
        predictions = (
            model.transform(test_features)
            .withColumn("fraud_probability", vector_to_array("probability")[1])
            .withColumn(
                "prediction",
                (F.col("fraud_probability") >= F.lit(0.5)).cast("int"),
            )
            .select(
                "transaction_id",
                "transaction_time",
                "amount",
                "class",
                "fraud_probability",
                "prediction",
            )
            .persist(StorageLevel.DISK_ONLY)
        )
        if predictions.count() != test_count:
            raise RuntimeError(f"Prediction row count mismatch for {slug}")

        metrics, threshold_rows = evaluate_scored_frame(
            predictions,
            score_col="fraud_probability",
            include_thresholds=True,
            thresholds=thresholds,
        )
        result = {
            "slug": slug,
            "model": model_name,
            "imbalance_method": imbalance_method,
            "decision_threshold": 0.5,
            **metrics,
        }
        model_results.append(result)
        threshold_results.extend(
            {"slug": slug, "model": model_name, **row} for row in threshold_rows
        )

        prediction_path = f"{prediction_base}/{slug}"
        prediction_output(predictions, slug, "fraud_probability").write.mode(
            "overwrite"
        ).partitionBy("prediction").parquet(prediction_path)
        prediction_paths[slug] = prediction_path

        if isinstance(model, RandomForestClassificationModel):
            for index, importance in enumerate(model.featureImportances.toArray()):
                feature_importance_rows.append(
                    {
                        "slug": slug,
                        "feature": feature_names[index],
                        "importance": float(importance),
                    }
                )
        predictions.unpersist()

    isolation_slug = "isolation_forest"
    print("[phase4] Evaluating Isolation Forest in distributed batches")
    isolation_predictions = score_isolation_forest(
        spark,
        test_features,
        model_path=f"{model_base}/isolation_forest/model.joblib",
        batch_size=isolation_batch_size,
    ).persist(StorageLevel.DISK_ONLY)
    if isolation_predictions.count() != test_count:
        raise RuntimeError("Prediction row count mismatch for Isolation Forest")
    isolation_metrics, _ = evaluate_scored_frame(
        isolation_predictions,
        score_col="anomaly_score",
        include_thresholds=False,
        thresholds=thresholds,
    )
    model_results.append(
        {
            "slug": isolation_slug,
            "model": "Isolation Forest",
            "imbalance_method": "Unsupervised",
            "decision_threshold": "sklearn contamination=auto boundary",
            **isolation_metrics,
        }
    )
    isolation_prediction_path = f"{prediction_base}/{isolation_slug}"
    prediction_output(
        isolation_predictions, isolation_slug, "anomaly_score"
    ).write.mode("overwrite").partitionBy("prediction").parquet(
        isolation_prediction_path
    )
    prediction_paths[isolation_slug] = isolation_prediction_path
    isolation_predictions.unpersist()

    result_directory = Path(result_dir)
    comparison_path = str(result_directory / "model_comparison.csv")
    threshold_path = str(result_directory / "threshold_analysis.csv")
    imbalance_path = str(result_directory / "imbalance_experiments.csv")
    confusion_path = str(result_directory / "confusion_matrices.csv")
    importance_path = str(result_directory / "rf_feature_importance.csv")

    metric_fields = [
        "slug",
        "model",
        "imbalance_method",
        "evaluation_rows",
        "precision",
        "recall",
        "f1",
        "pr_auc",
        "accuracy",
    ]
    write_csv_rows(model_results, comparison_path, metric_fields)
    write_csv_rows(
        [row for row in model_results if row["slug"] != isolation_slug],
        imbalance_path,
        metric_fields,
    )
    write_csv_rows(
        model_results,
        confusion_path,
        ["slug", "model", "tp", "tn", "fp", "fn"],
    )
    write_csv_rows(
        threshold_results,
        threshold_path,
        [
            "slug",
            "model",
            "threshold",
            "precision",
            "recall",
            "f1",
            "tp",
            "tn",
            "fp",
            "fn",
        ],
    )
    write_csv_rows(
        sorted(
            feature_importance_rows,
            key=lambda row: (row["slug"], -row["importance"]),
        ),
        importance_path,
        ["slug", "feature", "importance"],
    )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "evaluation_scope": {
            "path": test_path,
            "rows": test_count,
            "test_was_sampled": False,
            "same_test_rows_for_all_models": True,
            "default_classifier_threshold": 0.5,
        },
        "model_comparison": model_results,
        "threshold_analysis": threshold_results,
        "prediction_paths": prediction_paths,
        "local_result_files": {
            "model_comparison": comparison_path,
            "imbalance_experiments": imbalance_path,
            "confusion_matrices": confusion_path,
            "threshold_analysis": threshold_path,
            "rf_feature_importance": importance_path,
        },
        "notes": [
            "Precision, recall and F1 are for class=1 (Fraud).",
            "Accuracy is reported for context and is not used alone for selection.",
            "Threshold analysis is descriptive; business costs are needed before choosing a production threshold.",
            "Isolation Forest never uses class as an input feature; class is used only after scoring for evaluation.",
        ],
    }
    write_json_atomic(report, report_path)

    metadata_files = [
        report_path,
        comparison_path,
        imbalance_path,
        confusion_path,
        threshold_path,
        importance_path,
    ]
    for local_path in metadata_files:
        copy_local_file_to_hdfs(
            spark,
            local_path,
            f"{prediction_base}/metadata/{Path(local_path).name}",
        )

    test_features.unpersist()
    print(json.dumps(report, indent=2))
    print(f"[phase4] PASS report={report_path}")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test", default=DEFAULT_TEST)
    parser.add_argument("--labeled", default=DEFAULT_LABELED)
    parser.add_argument("--model-base", default=DEFAULT_MODEL_BASE)
    parser.add_argument("--prediction-base", default=DEFAULT_PREDICTION_BASE)
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--result-dir", default=DEFAULT_RESULT_DIR)
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        default=list(DEFAULT_THRESHOLDS),
    )
    parser.add_argument("--isolation-batch-size", type=int, default=4096)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_phase4(
            spark=spark,
            test_path=args.test,
            labeled_path=args.labeled,
            model_base=args.model_base,
            prediction_base=args.prediction_base,
            report_path=args.report,
            result_dir=args.result_dir,
            thresholds=tuple(args.thresholds),
            isolation_batch_size=args.isolation_batch_size,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
