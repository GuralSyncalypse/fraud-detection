"""Phase 5 visualizations built only from Spark aggregates and small CSV results."""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F


DEFAULT_LABELED = "hdfs:///financial/processed/labeled_transactions"
DEFAULT_PREDICTION_BASE = "hdfs:///financial/predictions"
DEFAULT_MODEL = "rf_undersampling"
DEFAULT_RESULT_DIR = "/workspace/output/results"
DEFAULT_FIGURE_DIR = "/workspace/output/figures"
CLASSIFIER_SLUGS = (
    "lr_none",
    "lr_class_weight",
    "rf_none",
    "rf_class_weight",
    "rf_undersampling",
)
RF_CLASSIFIER_SLUGS = {
    "rf_none",
    "rf_class_weight",
    "rf_undersampling",
}

MODEL_LABELS = {
    "lr_none": "Logistic Regression · không cân bằng",
    "lr_class_weight": "Logistic Regression · trọng số lớp",
    "rf_none": "Random Forest · không cân bằng",
    "rf_class_weight": "Random Forest · trọng số lớp",
    "rf_undersampling": "Random Forest · giảm mẫu lớp bình thường",
}


def _style_axis(axis: Any) -> None:
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.grid(axis="y", color="#DCE6F2", linewidth=0.8)
    axis.set_axisbelow(True)
    axis.tick_params(colors="#34495E", labelsize=10)
    axis.title.set_color("#102A43")
    axis.xaxis.label.set_color("#486581")
    axis.yaxis.label.set_color("#486581")


def _save_figure(figure: Any, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    figure.tight_layout()
    figure.savefig(destination, dpi=180, bbox_inches="tight")
    plt.close(figure)


def _write_csv(
    rows: Iterable[dict[str, Any]], destination: Path, fieldnames: Sequence[str]
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, destination)


def collect_histogram(
    data: DataFrame,
    column_name: str,
    bins: int,
    lower: float,
    upper: float,
) -> list[dict[str, Any]]:
    """Aggregate a numeric histogram in Spark; collect only `bins` rows."""
    if bins <= 0 or upper <= lower:
        raise ValueError("Histogram requires positive bins and upper > lower")
    width = (upper - lower) / bins
    bucket = F.floor((F.col(column_name) - F.lit(lower)) / F.lit(width)).cast(
        "int"
    )
    rows = (
        data.where(F.col(column_name).between(lower, upper))
        .withColumn("bucket", F.least(F.lit(bins - 1), bucket))
        .groupBy("bucket")
        .count()
        .collect()
    )
    counts = {int(row["bucket"]): int(row["count"]) for row in rows}
    return [
        {
            "bin": index,
            "lower": lower + index * width,
            "upper": lower + (index + 1) * width,
            "center": lower + (index + 0.5) * width,
            "count": counts.get(index, 0),
        }
        for index in range(bins)
    ]


def collect_pr_curve(
    spark: SparkSession, prediction_base: str, bins: int = 200
) -> list[dict[str, Any]]:
    """Build an approximate PR curve from distributed score buckets."""
    result: list[dict[str, Any]] = []
    for slug in CLASSIFIER_SLUGS:
        predictions = spark.read.parquet(f"{prediction_base}/{slug}").select(
            "fraud_probability", "actual_class"
        )
        bucket = F.least(
            F.lit(bins - 1),
            F.floor(F.col("fraud_probability") * F.lit(bins)).cast("int"),
        )
        rows = (
            predictions.withColumn("score_bucket", bucket)
            .groupBy("score_bucket", "actual_class")
            .count()
            .collect()
        )
        positives = sum(
            int(row["count"]) for row in rows if int(row["actual_class"]) == 1
        )
        by_bucket: dict[int, list[int]] = {}
        for row in rows:
            values = by_bucket.setdefault(int(row["score_bucket"]), [0, 0])
            values[int(row["actual_class"])] += int(row["count"])

        tp = 0
        fp = 0
        result.append(
            {
                "slug": slug,
                "threshold": 1.0,
                "precision": 1.0,
                "recall": 0.0,
            }
        )
        for bucket_index in sorted(by_bucket, reverse=True):
            normal_count, fraud_count = by_bucket[bucket_index]
            tp += fraud_count
            fp += normal_count
            result.append(
                {
                    "slug": slug,
                    "threshold": bucket_index / bins,
                    "precision": tp / (tp + fp) if tp + fp else 1.0,
                    "recall": tp / positives if positives else 0.0,
                }
            )
    return result


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


def generate_visualizations(
    spark: SparkSession,
    labeled_path: str = DEFAULT_LABELED,
    prediction_base: str = DEFAULT_PREDICTION_BASE,
    selected_model: str = DEFAULT_MODEL,
    result_dir: str = DEFAULT_RESULT_DIR,
    figure_dir: str = DEFAULT_FIGURE_DIR,
) -> dict[str, Any]:
    if selected_model not in CLASSIFIER_SLUGS:
        raise ValueError(f"Visualization model must be one of {CLASSIFIER_SLUGS}")
    results = Path(result_dir)
    figures = Path(figure_dir)
    results.mkdir(parents=True, exist_ok=True)
    figures.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "figure.facecolor": "#F8FBFF",
            "axes.facecolor": "#FFFFFF",
            "axes.edgecolor": "#B8C7D9",
            "font.size": 10,
            "axes.titlesize": 16,
            "axes.titleweight": "bold",
            "axes.labelsize": 12,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "legend.fontsize": 10,
        }
    )

    labeled = spark.read.parquet(labeled_path).select(
        "transaction_id", "transaction_time", "amount", "class"
    )
    selected_predictions = spark.read.parquet(
        f"{prediction_base}/{selected_model}"
    )

    class_rows = [
        {"class": int(row["class"]), "count": int(row["count"])}
        for row in labeled.groupBy("class").count().orderBy("class").collect()
    ]
    _write_csv(class_rows, results / "class_distribution.csv", ["class", "count"])
    figure, axis = plt.subplots(figsize=(7, 4.5))
    labels = ["Bình thường" if row["class"] == 0 else "Gian lận" for row in class_rows]
    values = [row["count"] for row in class_rows]
    bars = axis.bar(labels, values, color=["#2F80ED", "#EB5757"], width=0.58)
    axis.set_yscale("log")
    axis.set_title("Phân bố giao dịch: bình thường và gian lận", pad=14, fontweight="bold")
    axis.set_ylabel("Số giao dịch · thang logarit")
    axis.bar_label(bars, labels=[f"{value:,}" for value in values], padding=3)
    _style_axis(axis)
    _save_figure(figure, figures / "01_class_distribution.png")

    amount_lower, amount_upper = labeled.approxQuantile(
        "amount", [0.01, 0.99], 0.001
    )
    amount_histogram = collect_histogram(
        labeled, "amount", 50, float(amount_lower), float(amount_upper)
    )
    _write_csv(
        amount_histogram,
        results / "amount_histogram.csv",
        ["bin", "lower", "upper", "center", "count"],
    )
    figure, axis = plt.subplots(figsize=(8, 4.5))
    width = amount_histogram[0]["upper"] - amount_histogram[0]["lower"]
    axis.bar(
        [row["center"] for row in amount_histogram],
        [row["count"] for row in amount_histogram],
        width=width * 0.92,
        color="#4C78A8",
    )
    axis.set_title("Phân bố số tiền giao dịch", pad=14, fontweight="bold")
    axis.set_xlabel("Số tiền")
    axis.set_ylabel("Số giao dịch")
    _style_axis(axis)
    _save_figure(figure, figures / "02_amount_distribution.png")

    fraud = labeled.where(F.col("class") == 1)
    fraud_lower, fraud_upper = fraud.approxQuantile("amount", [0.0, 0.99], 0.001)
    fraud_histogram = collect_histogram(
        fraud, "amount", 40, float(fraud_lower), float(fraud_upper)
    )
    _write_csv(
        fraud_histogram,
        results / "fraud_amount_histogram.csv",
        ["bin", "lower", "upper", "center", "count"],
    )
    figure, axis = plt.subplots(figsize=(8, 4.5))
    fraud_width = fraud_histogram[0]["upper"] - fraud_histogram[0]["lower"]
    axis.bar(
        [row["center"] for row in fraud_histogram],
        [row["count"] for row in fraud_histogram],
        width=fraud_width * 0.92,
        color="#EB5757",
    )
    axis.set_title("Phân bố số tiền giao dịch gian lận", pad=14, fontweight="bold")
    axis.set_xlabel("Số tiền")
    axis.set_ylabel("Số giao dịch gian lận")
    _style_axis(axis)
    _save_figure(figure, figures / "03_fraud_amount_distribution.png")

    comparison = pd.read_csv(results / "model_comparison.csv")
    selected_metrics = comparison.loc[comparison["slug"] == selected_model].iloc[0]
    confusion = np.array(
        [
            [int(selected_metrics["tn"]), int(selected_metrics["fp"])],
            [int(selected_metrics["fn"]), int(selected_metrics["tp"])],
        ]
    ) if {"tn", "fp", "fn", "tp"}.issubset(comparison.columns) else None
    if confusion is None:
        confusion_rows = pd.read_csv(results / "confusion_matrices.csv")
        row = confusion_rows.loc[confusion_rows["slug"] == selected_model].iloc[0]
        confusion = np.array([[int(row.tn), int(row.fp)], [int(row.fn), int(row.tp)]])
    figure, axis = plt.subplots(figsize=(6, 5))
    image = axis.imshow(np.log1p(confusion), cmap="Blues")
    figure.colorbar(image, ax=axis, label="log(1 + count)")
    axis.set_xticks([0, 1], ["Dự đoán bình thường", "Dự đoán cần kiểm tra"])
    axis.set_yticks([0, 1], ["Thực tế bình thường", "Thực tế gian lận"])
    axis.set_title("Ma trận nhầm lẫn", pad=14, fontweight="bold")
    for row_index in range(2):
        for column_index in range(2):
            axis.text(
                column_index,
                row_index,
                f"{confusion[row_index, column_index]:,}",
                ha="center",
                va="center",
                color="black",
                fontweight="bold",
            )
    _save_figure(figure, figures / "04_confusion_matrix.png")

    figure, axis = plt.subplots(figsize=(10, 5))
    positions = np.arange(len(comparison))
    bar_width = 0.25
    for offset, metric, color in (
        (-bar_width, "precision", "#4C78A8"),
        (0.0, "recall", "#E45756"),
        (bar_width, "f1", "#72B7B2"),
    ):
        axis.bar(
            positions + offset,
            comparison[metric],
            bar_width,
            label=metric.title(),
            color=color,
        )
    axis.set_xticks(positions, [MODEL_LABELS.get(slug, slug) for slug in comparison["slug"]], rotation=20, ha="right")
    axis.set_ylim(0, 1.05)
    axis.set_ylabel("Điểm đánh giá cho lớp gian lận")
    axis.set_title("So sánh hiệu năng các mô hình", pad=14, fontweight="bold")
    axis.legend(["Độ chính xác", "Độ bao phủ", "F1"], frameon=False)
    _style_axis(axis)
    _save_figure(figure, figures / "05_metric_comparison.png")

    pr_rows = collect_pr_curve(spark, prediction_base)
    _write_csv(
        pr_rows,
        results / "precision_recall_curve.csv",
        ["slug", "threshold", "precision", "recall"],
    )
    figure, axis = plt.subplots(figsize=(8, 5))
    for slug in CLASSIFIER_SLUGS:
        rows = [row for row in pr_rows if row["slug"] == slug]
        axis.plot(
            [row["recall"] for row in rows],
            [row["precision"] for row in rows],
            label=MODEL_LABELS.get(slug, slug),
            linewidth=1.8,
        )
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1.02)
    axis.set_xlabel("Độ bao phủ (Recall)")
    axis.set_ylabel("Độ chính xác (Precision)")
    axis.set_title("Đường cong Precision – Recall", pad=14, fontweight="bold")
    axis.legend(fontsize=8, frameon=False)
    _style_axis(axis)
    _save_figure(figure, figures / "06_precision_recall_curve.png")

    importance_model = (
        selected_model if selected_model in RF_CLASSIFIER_SLUGS else DEFAULT_MODEL
    )
    importance = pd.read_csv(results / "rf_feature_importance.csv")
    importance = (
        importance.loc[importance["slug"] == importance_model]
        .nlargest(15, "importance")
        .sort_values("importance")
    )
    figure, axis = plt.subplots(figsize=(9, 6))
    axis.barh(importance["feature"], importance["importance"], color="#54A24B")
    axis.set_title("Các yếu tố ảnh hưởng đến mô hình", pad=14, fontweight="bold")
    axis.set_xlabel("Độ quan trọng · không phải quan hệ nhân quả")
    _style_axis(axis)
    _save_figure(figure, figures / "07_rf_feature_importance.png")

    probability_histogram = collect_histogram(
        selected_predictions, "fraud_probability", 50, 0.0, 1.0
    )
    _write_csv(
        probability_histogram,
        results / "probability_histogram.csv",
        ["bin", "lower", "upper", "center", "count"],
    )
    figure, axis = plt.subplots(figsize=(8, 4.5))
    axis.bar(
        [row["center"] for row in probability_histogram],
        [row["count"] for row in probability_histogram],
        width=0.018,
        color="#F58518",
    )
    axis.set_yscale("log")
    axis.set_xlabel("Xác suất gian lận")
    axis.set_ylabel("Số giao dịch · thang logarit")
    axis.set_title("Phân bố điểm rủi ro", pad=14, fontweight="bold")
    _style_axis(axis)
    _save_figure(figure, figures / "08_probability_distribution.png")

    threshold_data = pd.read_csv(results / "threshold_analysis.csv")
    threshold_data = threshold_data.loc[threshold_data["slug"] == selected_model]
    figure, axis = plt.subplots(figsize=(8, 4.5))
    for metric, color in (
        ("precision", "#4C78A8"),
        ("recall", "#E45756"),
        ("f1", "#72B7B2"),
    ):
        axis.plot(
            threshold_data["threshold"],
            threshold_data[metric],
            marker="o",
            label={"precision": "Độ chính xác", "recall": "Độ bao phủ", "f1": "F1"}[metric],
            color=color,
        )
    axis.set_ylim(0, 1.02)
    axis.set_xlabel("Ngưỡng cảnh báo")
    axis.set_ylabel("Điểm đánh giá")
    axis.set_title("Ảnh hưởng của ngưỡng cảnh báo", pad=14, fontweight="bold")
    axis.legend(frameon=False)
    _style_axis(axis)
    _save_figure(figure, figures / "09_threshold_analysis.png")

    top_suspicious = (
        selected_predictions.where(F.col("prediction") == 1)
        .orderBy(F.desc("fraud_probability"), F.asc("transaction_id"))
        .limit(20)
    )
    top_suspicious.write.mode("overwrite").parquet(
        f"{prediction_base}/suspicious_transactions"
    )
    top_rows = [row.asDict() for row in top_suspicious.collect()]
    top_csv_rows = [
        {
            "transaction_id": row["transaction_id"],
            "transaction_time": row["transaction_time"].isoformat(sep=" "),
            "amount": row["amount"],
            "fraud_probability": row["fraud_probability"],
            "status": row["status"],
            "actual_class": row["actual_class"],
            "model": row["model"],
        }
        for row in top_rows
    ]
    _write_csv(
        top_csv_rows,
        results / "top_suspicious_transactions.csv",
        [
            "transaction_id",
            "transaction_time",
            "amount",
            "fraud_probability",
            "status",
            "actual_class",
            "model",
        ],
    )
    chart_rows = list(reversed(top_csv_rows[:15]))
    figure, axis = plt.subplots(figsize=(9, 6))
    axis.barh(
        [str(row["transaction_id"]) for row in chart_rows],
        [float(row["fraud_probability"]) for row in chart_rows],
        color="#EB5757",
    )
    axis.set_xlim(0, 1)
    axis.set_xlabel("Xác suất gian lận")
    axis.set_ylabel("Mã giao dịch")
    axis.set_title("Top giao dịch cần kiểm tra", pad=14, fontweight="bold")
    _style_axis(axis)
    _save_figure(figure, figures / "10_top_suspicious_transactions.png")

    figure_files = sorted(path.name for path in figures.glob("*.png"))
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "selected_demo_model": selected_model,
        "feature_importance_model": importance_model,
        "selection_note": "Selected for demonstration, not declared a business-optimal model.",
        "spark_aggregation_before_pandas": True,
        "full_dataset_to_pandas": False,
        "figure_count": len(figure_files),
        "figures": figure_files,
        "top_suspicious_rows": len(top_csv_rows),
        "top_suspicious_hdfs_path": f"{prediction_base}/suspicious_transactions",
        "pr_curve": "Approximate distributed curve using 200 score bins.",
    }
    report_path = results / "visualization_summary.json"
    temporary = report_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(temporary, report_path)
    copy_local_file_to_hdfs(
        spark,
        str(report_path),
        f"{prediction_base}/metadata/visualization_summary.json",
    )
    print(json.dumps(report, indent=2))
    print(f"[phase5] PASS generated {len(figure_files)} figures")
    return report
