"""Leakage-safe Spark feature engineering and deterministic data splitting."""

from __future__ import annotations

from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame, functions as F


RANDOM_SEED = 42
HASH_BUCKETS = 1_000_000

NUMERIC_FEATURES = [
    "amount",
    "transaction_hour",
    "transaction_day_of_week",
    "transaction_month",
    "transaction_year_numeric",
    "is_weekend",
    "is_online",
    "has_error",
]

CATEGORICAL_FEATURES = ["use_chip", "mcc_category", "merchant_state_category"]


def _derived_feature_columns() -> list[Any]:
    """Return the shared, stateless columns used by training and inference."""
    return [
        F.col("amount").cast("double").alias("amount"),
        F.coalesce(F.col("use_chip"), F.lit("Unknown")).alias("use_chip"),
        F.coalesce(F.col("mcc").cast("string"), F.lit("Unknown")).alias(
            "mcc_category"
        ),
        F.coalesce(F.col("merchant_state"), F.lit("ONLINE_OR_UNKNOWN")).alias(
            "merchant_state_category"
        ),
        F.hour("transaction_time").cast("double").alias("transaction_hour"),
        F.dayofweek("transaction_time")
        .cast("double")
        .alias("transaction_day_of_week"),
        F.month("transaction_time").cast("double").alias("transaction_month"),
        F.year("transaction_time").cast("double").alias("transaction_year_numeric"),
        F.when(F.dayofweek("transaction_time").isin(1, 7), 1.0)
        .otherwise(0.0)
        .alias("is_weekend"),
        F.when(F.col("merchant_state").isNull(), 1.0)
        .otherwise(0.0)
        .alias("is_online"),
        F.when(F.col("errors").isNotNull(), 1.0)
        .otherwise(0.0)
        .alias("has_error"),
    ]


def prepare_inference_features(transactions: DataFrame) -> DataFrame:
    """Derive model inputs for unlabeled transactions."""
    return transactions.select(
        "transaction_id", "transaction_time", *_derived_feature_columns()
    )


def prepare_base_features(transactions: DataFrame) -> DataFrame:
    """Derive labeled model inputs without using the label as a feature."""
    return transactions.select(
        "transaction_id", "class", *_derived_feature_columns()
    )


def deterministic_train_test_split(
    features: DataFrame, train_fraction: float = 0.8, seed: int = RANDOM_SEED
) -> tuple[DataFrame, DataFrame]:
    """Stable 80/20 split based on transaction ID; no sampling touches the test set."""
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must be between 0 and 1")

    boundary = int(HASH_BUCKETS * train_fraction)
    bucket = F.pmod(
        F.xxhash64(F.col("transaction_id"), F.lit(seed)), F.lit(HASH_BUCKETS)
    )
    with_bucket = features.withColumn("_split_bucket", bucket)
    train = with_bucket.where(F.col("_split_bucket") < boundary).drop(
        "_split_bucket"
    )
    test = with_bucket.where(F.col("_split_bucket") >= boundary).drop(
        "_split_bucket"
    )
    return train, test


def deterministic_cap(
    data: DataFrame,
    total_count: int,
    target_rows: int,
    seed: int = RANDOM_SEED + 1,
) -> DataFrame:
    """Apply a class-neutral compute cap while preserving the natural distribution."""
    if target_rows <= 0 or total_count <= target_rows:
        return data

    threshold = max(1, min(HASH_BUCKETS, int(HASH_BUCKETS * target_rows / total_count)))
    bucket = F.pmod(
        F.xxhash64(F.col("transaction_id"), F.lit(seed)), F.lit(HASH_BUCKETS)
    )
    return data.where(bucket < threshold)


def deterministic_undersample(
    train: DataFrame,
    normal_count: int,
    fraud_count: int,
    normal_to_fraud_ratio: float = 10.0,
    seed: int = RANDOM_SEED + 2,
) -> DataFrame:
    """Keep all fraud and reduce only normal rows in the training set."""
    if fraud_count <= 0:
        raise ValueError("Training data contains no fraud rows")

    desired_normal = min(normal_count, int(fraud_count * normal_to_fraud_ratio))
    threshold = max(
        1, min(HASH_BUCKETS, int(HASH_BUCKETS * desired_normal / normal_count))
    )
    bucket = F.pmod(
        F.xxhash64(F.col("transaction_id"), F.lit(seed)), F.lit(HASH_BUCKETS)
    )
    normal_sample = train.where((F.col("class") == 0) & (bucket < threshold))
    fraud = train.where(F.col("class") == 1)
    return fraud.unionByName(normal_sample)


def build_preprocessing_pipeline() -> Pipeline:
    """Create estimators that must be fitted only on the training split."""
    indexers = [
        StringIndexer(
            inputCol=name,
            outputCol=f"{name}_index",
            handleInvalid="keep",
            stringOrderType="frequencyDesc",
        )
        for name in CATEGORICAL_FEATURES
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{name}_index" for name in CATEGORICAL_FEATURES],
        outputCols=[f"{name}_ohe" for name in CATEGORICAL_FEATURES],
        handleInvalid="keep",
        dropLast=True,
    )
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES
        + [f"{name}_ohe" for name in CATEGORICAL_FEATURES],
        outputCol="features_raw",
        handleInvalid="keep",
    )
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=False,
        withStd=True,
    )
    return Pipeline(stages=[*indexers, encoder, assembler, scaler])


def fit_preprocessor(train: DataFrame) -> PipelineModel:
    return build_preprocessing_pipeline().fit(train)


def class_distribution(data: DataFrame) -> dict[str, Any]:
    rows = data.groupBy("class").count().orderBy("class").collect()
    total = sum(int(row["count"]) for row in rows)
    return {
        "total": total,
        "classes": {
            str(row["class"]): {
                "count": int(row["count"]),
                "percentage": round(100.0 * int(row["count"]) / total, 6),
            }
            for row in rows
        },
    }


def extract_feature_names(transformed: DataFrame) -> list[str]:
    """Read VectorAssembler metadata; indices match scaled vector positions."""
    metadata = transformed.schema["features_raw"].metadata
    attributes = metadata.get("ml_attr", {}).get("attrs", {})
    indexed_names: list[tuple[int, str]] = []
    for attribute_type in attributes.values():
        for attribute in attribute_type:
            indexed_names.append((int(attribute["idx"]), attribute["name"]))

    if indexed_names:
        return [name for _, name in sorted(indexed_names)]

    first = transformed.select("features").first()
    return [f"feature_{index}" for index in range(first["features"].size)]
