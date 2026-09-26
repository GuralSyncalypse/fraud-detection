"""Distributed evaluation helpers for binary fraud classifiers."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Iterable, Sequence

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame, functions as F


DEFAULT_THRESHOLDS = (0.2, 0.3, 0.4, 0.5, 0.6, 0.7)


def _divide(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else 0.0


def metrics_from_counts(tp: int, tn: int, fp: int, fn: int) -> dict[str, Any]:
    """Calculate fraud-class metrics from an explicit confusion matrix."""
    precision = _divide(tp, tp + fp)
    recall = _divide(tp, tp + fn)
    f1 = _divide(2 * precision * recall, precision + recall)
    total = tp + tn + fp + fn
    return {
        "evaluation_rows": total,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "specificity": _divide(tn, tn + fp),
        "accuracy": _divide(tp + tn, total),
    }


def binary_metrics(
    predictions: DataFrame,
    label_col: str = "class",
    prediction_col: str = "prediction",
) -> dict[str, Any]:
    """Aggregate TP/TN/FP/FN in Spark without collecting prediction rows."""
    label = F.col(label_col).cast("int")
    prediction = F.col(prediction_col).cast("int")
    row = predictions.agg(
        F.sum(F.when((label == 1) & (prediction == 1), 1).otherwise(0)).alias(
            "tp"
        ),
        F.sum(F.when((label == 0) & (prediction == 0), 1).otherwise(0)).alias(
            "tn"
        ),
        F.sum(F.when((label == 0) & (prediction == 1), 1).otherwise(0)).alias(
            "fp"
        ),
        F.sum(F.when((label == 1) & (prediction == 0), 1).otherwise(0)).alias(
            "fn"
        ),
    ).first()
    return metrics_from_counts(
        tp=int(row["tp"] or 0),
        tn=int(row["tn"] or 0),
        fp=int(row["fp"] or 0),
        fn=int(row["fn"] or 0),
    )


def area_under_pr(
    predictions: DataFrame,
    score_col: str,
    label_col: str = "class",
) -> float:
    """Compute PR-AUC with Spark's distributed binary evaluator."""
    evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=score_col,
        metricName="areaUnderPR",
    )
    return float(evaluator.evaluate(predictions))


def threshold_analysis(
    predictions: DataFrame,
    score_col: str,
    thresholds: Sequence[float] = DEFAULT_THRESHOLDS,
    label_col: str = "class",
) -> list[dict[str, Any]]:
    """Evaluate several score thresholds with one distributed aggregation."""
    if not thresholds:
        return []
    for threshold in thresholds:
        if not 0.0 <= threshold <= 1.0:
            raise ValueError(f"threshold must be in [0, 1], got {threshold}")

    expressions = []
    label = F.col(label_col).cast("int")
    for index, threshold in enumerate(thresholds):
        predicted_positive = F.col(score_col) >= F.lit(float(threshold))
        expressions.extend(
            [
                F.sum(
                    F.when((label == 1) & predicted_positive, 1).otherwise(0)
                ).alias(f"t{index}_tp"),
                F.sum(
                    F.when((label == 0) & ~predicted_positive, 1).otherwise(0)
                ).alias(f"t{index}_tn"),
                F.sum(
                    F.when((label == 0) & predicted_positive, 1).otherwise(0)
                ).alias(f"t{index}_fp"),
                F.sum(
                    F.when((label == 1) & ~predicted_positive, 1).otherwise(0)
                ).alias(f"t{index}_fn"),
            ]
        )

    row = predictions.agg(*expressions).first()
    result = []
    for index, threshold in enumerate(thresholds):
        metrics = metrics_from_counts(
            tp=int(row[f"t{index}_tp"] or 0),
            tn=int(row[f"t{index}_tn"] or 0),
            fp=int(row[f"t{index}_fp"] or 0),
            fn=int(row[f"t{index}_fn"] or 0),
        )
        result.append({"threshold": float(threshold), **metrics})
    return result


def prediction_output(
    predictions: DataFrame,
    model_name: str,
    score_col: str,
) -> DataFrame:
    """Create the stable, presentation-friendly prediction schema."""
    return predictions.select(
        "transaction_id",
        "transaction_time",
        "amount",
        F.col(score_col).cast("double").alias(score_col),
        F.col("prediction").cast("int").alias("prediction"),
        F.when(F.col("prediction") == 1, F.lit("NEEDS_REVIEW"))
        .otherwise(F.lit("NORMAL"))
        .alias("status"),
        F.col("class").cast("int").alias("actual_class"),
        F.lit(model_name).alias("model"),
    )


def write_json_atomic(payload: dict[str, Any], destination: str) -> None:
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def write_csv_rows(
    rows: Iterable[dict[str, Any]], destination: str, fieldnames: Sequence[str]
) -> None:
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)
