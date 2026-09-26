"""CLI entry point for distributed Phase 5 visualization aggregates."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from visualization import (
    CLASSIFIER_SLUGS,
    DEFAULT_FIGURE_DIR,
    DEFAULT_LABELED,
    DEFAULT_MODEL,
    DEFAULT_PREDICTION_BASE,
    DEFAULT_RESULT_DIR,
    generate_visualizations,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labeled", default=DEFAULT_LABELED)
    parser.add_argument("--prediction-base", default=DEFAULT_PREDICTION_BASE)
    parser.add_argument("--model", default=DEFAULT_MODEL, choices=CLASSIFIER_SLUGS)
    parser.add_argument("--result-dir", default=DEFAULT_RESULT_DIR)
    parser.add_argument("--figure-dir", default=DEFAULT_FIGURE_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("financial-fraud-phase-5-charts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        generate_visualizations(
            spark=spark,
            labeled_path=args.labeled,
            prediction_base=args.prediction_base,
            selected_model=args.model,
            result_dir=args.result_dir,
            figure_dir=args.figure_dir,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
