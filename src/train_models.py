"""Phase 3: leakage-safe split, imbalance experiments and model training."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pyspark import StorageLevel
from pyspark.ml.classification import (
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassificationModel,
    RandomForestClassifier,
)
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import DataFrame, SparkSession, functions as F

from anomaly_detection import train_isolation_forest
from feature_engineering import (
    RANDOM_SEED,
    class_distribution,
    deterministic_cap,
    deterministic_train_test_split,
    deterministic_undersample,
    extract_feature_names,
    fit_preprocessor,
    prepare_base_features,
)


DEFAULT_INPUT = "hdfs:///financial/processed/labeled_transactions"
DEFAULT_SPLIT_BASE = "hdfs:///financial/processed/ml_splits"
DEFAULT_MODEL_BASE = "hdfs:///financial/models"
DEFAULT_REPORT = "/workspace/output/results/phase3_summary.json"
DEFAULT_ISOLATION_MODEL = "/workspace/output/models/isolation_forest.joblib"


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("financial-fraud-phase-3").getOrCreate()


def timed_fit(name: str, estimator: Any, data: DataFrame) -> tuple[Any, float]:
    print(f"[phase3] Training {name}")
    started = time.perf_counter()
    model = estimator.fit(data)
    seconds = round(time.perf_counter() - started, 3)
    print(f"[phase3] Finished {name} in {seconds:.3f}s")
    return model, seconds


def save_and_reload_spark_model(
    model: Any,
    path: str,
    loader: Callable[[str], Any],
    validation_features: DataFrame,
) -> None:
    model.write().overwrite().save(path)
    loaded = loader(path)
    if loaded.transform(validation_features.limit(10)).count() != 10:
        raise RuntimeError(f"Reload validation failed for {path}")


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


def write_report(report: dict[str, Any], local_path: str) -> None:
    destination = Path(local_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
    os.replace(temporary, destination)


def run_phase3(
    spark: SparkSession,
    input_path: str,
    split_base: str,
    model_base: str,
    report_path: str,
    isolation_model_path: str,
    max_supervised_train_rows: int,
    isolation_max_rows: int,
    rf_num_trees: int,
    rf_max_depth: int,
    undersample_ratio: float,
) -> dict[str, Any]:
    print(f"[phase3] Reading labeled Parquet from {input_path}")
    source = spark.read.parquet(input_path)
    base_features = prepare_base_features(source)
    train, test = deterministic_train_test_split(base_features, seed=RANDOM_SEED)

    train_path = f"{split_base}/train"
    test_path = f"{split_base}/test"
    print("[phase3] Saving deterministic train/test splits")
    train.write.mode("overwrite").partitionBy("class").parquet(train_path)
    test.write.mode("overwrite").partitionBy("class").parquet(test_path)

    # Reloading makes later phases independent of upstream partition order.
    train = spark.read.parquet(train_path)
    test = spark.read.parquet(test_path)
    train_distribution = class_distribution(train)
    test_distribution = class_distribution(test)
    total_count = train_distribution["total"] + test_distribution["total"]
    actual_train_fraction = train_distribution["total"] / total_count

    print("[phase3] Creating class-neutral, natural-distribution training cap")
    modeling_train = deterministic_cap(
        train,
        total_count=train_distribution["total"],
        target_rows=max_supervised_train_rows,
        seed=RANDOM_SEED + 1,
    ).persist(StorageLevel.DISK_ONLY)
    modeling_distribution = class_distribution(modeling_train)

    print("[phase3] Fitting preprocessing on TRAIN only")
    preprocessor = fit_preprocessor(modeling_train)
    preprocessor_path = f"{model_base}/preprocessor"
    preprocessor.write().overwrite().save(preprocessor_path)
    reloaded_preprocessor = PipelineModel.load(preprocessor_path)

    modeling_features = reloaded_preprocessor.transform(modeling_train).persist(
        StorageLevel.DISK_ONLY
    )
    modeling_features.count()
    validation_features = reloaded_preprocessor.transform(test.limit(100))
    feature_names = extract_feature_names(modeling_features)

    normal_model_count = modeling_distribution["classes"]["0"]["count"]
    fraud_model_count = modeling_distribution["classes"]["1"]["count"]
    total_model_count = modeling_distribution["total"]
    normal_weight = total_model_count / (2.0 * normal_model_count)
    fraud_weight = total_model_count / (2.0 * fraud_model_count)
    weighted_features = modeling_features.withColumn(
        "class_weight",
        F.when(F.col("class") == 1, F.lit(fraud_weight)).otherwise(
            F.lit(normal_weight)
        ),
    )

    training_times: dict[str, float] = {}
    model_paths: dict[str, str] = {"preprocessor": preprocessor_path}

    lr_none, training_times["lr_none"] = timed_fit(
        "Logistic Regression / no imbalance handling",
        LogisticRegression(
            featuresCol="features",
            labelCol="class",
            maxIter=20,
            regParam=0.01,
            elasticNetParam=0.0,
            standardization=False,
        ),
        modeling_features,
    )
    model_paths["lr_none"] = f"{model_base}/logistic_regression/none"
    save_and_reload_spark_model(
        lr_none,
        model_paths["lr_none"],
        LogisticRegressionModel.load,
        validation_features,
    )

    lr_weighted, training_times["lr_class_weight"] = timed_fit(
        "Logistic Regression / class weight",
        LogisticRegression(
            featuresCol="features",
            labelCol="class",
            weightCol="class_weight",
            maxIter=20,
            regParam=0.01,
            elasticNetParam=0.0,
            standardization=False,
        ),
        weighted_features,
    )
    model_paths["lr_class_weight"] = f"{model_base}/logistic_regression/class_weight"
    save_and_reload_spark_model(
        lr_weighted,
        model_paths["lr_class_weight"],
        LogisticRegressionModel.load,
        validation_features,
    )

    rf_parameters = {
        "featuresCol": "features",
        "labelCol": "class",
        "numTrees": rf_num_trees,
        "maxDepth": rf_max_depth,
        "featureSubsetStrategy": "sqrt",
        "subsamplingRate": 0.8,
        "seed": RANDOM_SEED,
    }
    rf_none, training_times["rf_none"] = timed_fit(
        "Random Forest / no imbalance handling",
        RandomForestClassifier(**rf_parameters),
        modeling_features,
    )
    model_paths["rf_none"] = f"{model_base}/random_forest/none"
    save_and_reload_spark_model(
        rf_none,
        model_paths["rf_none"],
        RandomForestClassificationModel.load,
        validation_features,
    )

    rf_weighted, training_times["rf_class_weight"] = timed_fit(
        "Random Forest / class weight",
        RandomForestClassifier(weightCol="class_weight", **rf_parameters),
        weighted_features,
    )
    model_paths["rf_class_weight"] = f"{model_base}/random_forest/class_weight"
    save_and_reload_spark_model(
        rf_weighted,
        model_paths["rf_class_weight"],
        RandomForestClassificationModel.load,
        validation_features,
    )

    full_train_normal = train_distribution["classes"]["0"]["count"]
    full_train_fraud = train_distribution["classes"]["1"]["count"]
    undersampled_train = deterministic_undersample(
        train,
        normal_count=full_train_normal,
        fraud_count=full_train_fraud,
        normal_to_fraud_ratio=undersample_ratio,
        seed=RANDOM_SEED + 2,
    ).persist(StorageLevel.DISK_ONLY)
    undersampled_distribution = class_distribution(undersampled_train)
    undersampled_features = reloaded_preprocessor.transform(
        undersampled_train
    ).persist(StorageLevel.DISK_ONLY)
    undersampled_features.count()

    rf_undersampled, training_times["rf_undersampling"] = timed_fit(
        "Random Forest / random undersampling",
        RandomForestClassifier(**rf_parameters),
        undersampled_features,
    )
    model_paths["rf_undersampling"] = f"{model_base}/random_forest/undersampling"
    save_and_reload_spark_model(
        rf_undersampled,
        model_paths["rf_undersampling"],
        RandomForestClassificationModel.load,
        validation_features,
    )

    print("[phase3] Training local Isolation Forest on bounded Spark features")
    isolation_started = time.perf_counter()
    _, isolation_summary = train_isolation_forest(
        train,
        local_model_path=isolation_model_path,
        max_rows=isolation_max_rows,
        seed=RANDOM_SEED,
    )
    training_times["isolation_forest"] = round(
        time.perf_counter() - isolation_started, 3
    )
    isolation_hdfs_path = f"{model_base}/isolation_forest/model.joblib"
    copy_local_file_to_hdfs(spark, isolation_model_path, isolation_hdfs_path)
    isolation_summary["hdfs_model_path"] = isolation_hdfs_path
    model_paths["isolation_forest"] = isolation_hdfs_path

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "seed": RANDOM_SEED,
        "spark_version": spark.version,
        "input": input_path,
        "split": {
            "method": "deterministic xxhash64(transaction_id, seed)",
            "requested_train_fraction": 0.8,
            "actual_train_fraction": round(actual_train_fraction, 8),
            "train": train_distribution,
            "test": test_distribution,
            "test_was_sampled": False,
        },
        "supervised_training": {
            "compute_cap_target_rows": max_supervised_train_rows,
            "cap_is_class_neutral": True,
            "natural_distribution_train": modeling_distribution,
            "class_weights": {
                "0": normal_weight,
                "1": fraud_weight,
            },
            "undersampling": {
                "normal_to_fraud_target_ratio": undersample_ratio,
                "distribution": undersampled_distribution,
                "test_set_modified": False,
            },
            "rf_parameters": {
                "numTrees": rf_num_trees,
                "maxDepth": rf_max_depth,
                "featureSubsetStrategy": "sqrt",
                "subsamplingRate": 0.8,
            },
        },
        "features": {
            "count": len(feature_names),
            "names": feature_names,
            "class_in_feature_vector": False,
            "identifier_columns_in_feature_vector": False,
            "preprocessor_fit_scope": "training data only",
        },
        "isolation_forest": isolation_summary,
        "training_seconds": training_times,
        "model_paths": model_paths,
        "split_paths": {"train": train_path, "test": test_path},
    }
    write_report(report, report_path)
    report_hdfs_path = f"{model_base}/metadata/phase3_summary.json"
    copy_local_file_to_hdfs(spark, report_path, report_hdfs_path)
    print(json.dumps(report, indent=2))
    print(f"[phase3] PASS report={report_path}")

    undersampled_features.unpersist()
    undersampled_train.unpersist()
    modeling_features.unpersist()
    modeling_train.unpersist()
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--split-base", default=DEFAULT_SPLIT_BASE)
    parser.add_argument("--model-base", default=DEFAULT_MODEL_BASE)
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--isolation-model", default=DEFAULT_ISOLATION_MODEL)
    parser.add_argument("--max-supervised-train-rows", type=int, default=500_000)
    parser.add_argument("--isolation-max-rows", type=int, default=200_000)
    parser.add_argument("--rf-num-trees", type=int, default=25)
    parser.add_argument("--rf-max-depth", type=int, default=8)
    parser.add_argument("--undersample-ratio", type=float, default=10.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_phase3(
            spark=spark,
            input_path=args.input,
            split_base=args.split_base,
            model_base=args.model_base,
            report_path=args.report,
            isolation_model_path=args.isolation_model,
            max_supervised_train_rows=args.max_supervised_train_rows,
            isolation_max_rows=args.isolation_max_rows,
            rf_num_trees=args.rf_num_trees,
            rf_max_depth=args.rf_max_depth,
            undersample_ratio=args.undersample_ratio,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

