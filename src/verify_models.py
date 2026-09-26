"""Load every Phase 3 model in a new Spark application and predict a small batch."""

from __future__ import annotations

import json

import joblib
from pyspark.ml.classification import (
    LogisticRegressionModel,
    RandomForestClassificationModel,
)
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession

from anomaly_detection import ISOLATION_FEATURES


TEST_PATH = "hdfs:///financial/processed/ml_splits/test"
MODEL_BASE = "hdfs:///financial/models"
LOCAL_ISOLATION_MODEL = "/workspace/output/models/isolation_forest.joblib"


def main() -> None:
    spark = SparkSession.builder.appName("phase-3-model-reload-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        test = spark.read.parquet(TEST_PATH)
        sample = test.limit(100)
        preprocessor = PipelineModel.load(f"{MODEL_BASE}/preprocessor")
        transformed = preprocessor.transform(sample)

        spark_models = {
            "lr_none": LogisticRegressionModel.load(
                f"{MODEL_BASE}/logistic_regression/none"
            ),
            "lr_class_weight": LogisticRegressionModel.load(
                f"{MODEL_BASE}/logistic_regression/class_weight"
            ),
            "rf_none": RandomForestClassificationModel.load(
                f"{MODEL_BASE}/random_forest/none"
            ),
            "rf_class_weight": RandomForestClassificationModel.load(
                f"{MODEL_BASE}/random_forest/class_weight"
            ),
            "rf_undersampling": RandomForestClassificationModel.load(
                f"{MODEL_BASE}/random_forest/undersampling"
            ),
        }
        spark_prediction_counts = {
            name: model.transform(transformed).select("prediction").count()
            for name, model in spark_models.items()
        }
        if any(count != 100 for count in spark_prediction_counts.values()):
            raise RuntimeError(
                f"Spark model reload validation failed: {spark_prediction_counts}"
            )

        isolation_model = joblib.load(LOCAL_ISOLATION_MODEL)
        local_features = sample.select(*ISOLATION_FEATURES).toPandas()
        isolation_count = len(isolation_model.predict(local_features))
        if isolation_count != 100:
            raise RuntimeError("Isolation Forest reload validation failed")

        result = {
            "status": "PASS",
            "spark_model_prediction_counts": spark_prediction_counts,
            "isolation_forest_prediction_count": isolation_count,
            "new_spark_application": True,
        }
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

