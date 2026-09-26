"""Local Isolation Forest training on a bounded Spark-engineered sample."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
from pyspark.sql import DataFrame, functions as F
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


ISOLATION_FEATURES = [
    "amount",
    "transaction_hour",
    "transaction_day_of_week",
    "transaction_month",
    "transaction_year_numeric",
    "is_weekend",
    "is_online",
    "has_error",
]


def train_isolation_forest(
    spark_train: DataFrame,
    local_model_path: str,
    max_rows: int = 200_000,
    seed: int = 42,
) -> tuple[Pipeline, dict[str, Any]]:
    """Fit locally after Spark derives features and bounds the transfer size."""
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")

    total_count = spark_train.count()
    hash_buckets = 1_000_000
    threshold = max(1, min(hash_buckets, int(hash_buckets * max_rows / total_count)))
    bucket = F.pmod(
        F.xxhash64(F.col("transaction_id"), F.lit(seed)), F.lit(hash_buckets)
    )
    bounded = spark_train.where(bucket < threshold).select(*ISOLATION_FEATURES).limit(
        max_rows
    )
    local_features = bounded.toPandas()
    if local_features.empty:
        raise RuntimeError("Isolation Forest sample is empty")

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "isolation_forest",
                IsolationForest(
                    n_estimators=200,
                    contamination="auto",
                    random_state=seed,
                    n_jobs=-1,
                ),
            ),
        ]
    )
    # Class is deliberately absent from local_features and therefore from fit().
    model.fit(local_features)

    destination = Path(local_model_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, destination)
    loaded = joblib.load(destination)
    if len(loaded.predict(local_features.head(10))) != min(10, len(local_features)):
        raise RuntimeError("Isolation Forest reload validation failed")

    return model, {
        "execution": "local scikit-learn after distributed Spark feature derivation",
        "training_rows": int(len(local_features)),
        "features": ISOLATION_FEATURES,
        "class_used_as_feature": False,
        "n_estimators": 200,
        "contamination": "auto",
        "local_model_path": str(destination),
    }

