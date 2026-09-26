"""Phase 2 Spark EDA, cleaning, label join and Parquet conversion."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


DEFAULT_TRANSACTIONS_PATH = os.getenv(
    "TRANSACTIONS_HDFS_PATH", "hdfs:///financial/raw/transactions_data.csv"
)
DEFAULT_LABELS_PATH = os.getenv(
    "LABELS_HDFS_PATH", "hdfs:///financial/raw/fraud_labels.csv"
)
DEFAULT_OUTPUT_PATH = os.getenv(
    "PROCESSED_HDFS_PATH", "hdfs:///financial/processed/transactions"
)
DEFAULT_LABELED_OUTPUT_PATH = os.getenv(
    "LABELED_PROCESSED_HDFS_PATH",
    "hdfs:///financial/processed/labeled_transactions",
)
DEFAULT_REPORT_PATH = os.getenv(
    "PHASE2_REPORT_PATH", "/workspace/output/results/phase2_summary.json"
)


TRANSACTION_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("date", StringType(), True),
        StructField("client_id", LongType(), True),
        StructField("card_id", LongType(), True),
        StructField("amount", StringType(), True),
        StructField("use_chip", StringType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("merchant_city", StringType(), True),
        StructField("merchant_state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("mcc", IntegerType(), True),
        StructField("errors", StringType(), True),
    ]
)

LABEL_SCHEMA = StructType(
    [
        StructField("transaction_id", LongType(), True),
        StructField("label", StringType(), True),
    ]
)


def create_spark_session(app_name: str = "financial-fraud-phase-2") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_raw_transactions(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .schema(TRANSACTION_SCHEMA)
        .csv(path)
    )


def read_normalized_labels(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .schema(LABEL_SCHEMA)
        .csv(path)
    )


def _blank_or_null(column_name: str) -> Any:
    column = F.col(column_name)
    return column.isNull() | (F.trim(column.cast("string")) == "")


def collect_raw_profile(raw: DataFrame) -> dict[str, Any]:
    expressions = [F.count(F.lit(1)).alias("row_count")]
    expressions.extend(
        F.sum(F.when(_blank_or_null(name), 1).otherwise(0)).alias(name)
        for name in raw.columns
    )
    row = raw.agg(*expressions).first().asDict()
    row_count = int(row.pop("row_count"))

    duplicate_transaction_ids = (
        raw.where(F.col("id").isNotNull())
        .groupBy("id")
        .count()
        .where(F.col("count") > 1)
        .select(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicates"))
        .first()["duplicates"]
    )

    return {
        "row_count": row_count,
        "column_count": len(raw.columns),
        "columns": raw.columns,
        "null_or_blank_counts": {key: int(value or 0) for key, value in row.items()},
        "duplicate_transaction_ids": int(duplicate_transaction_ids or 0),
    }


def clean_transactions(raw: DataFrame) -> DataFrame:
    parsed_amount = F.regexp_replace(F.trim(F.col("amount")), r"[$,]", "").cast(
        "double"
    )
    parsed_time = F.to_timestamp(F.trim(F.col("date")), "yyyy-MM-dd HH:mm:ss")

    return (
        raw.select(
            F.col("id").alias("transaction_id"),
            parsed_time.alias("transaction_time"),
            F.col("client_id"),
            F.col("card_id"),
            parsed_amount.alias("amount"),
            F.when(_blank_or_null("use_chip"), F.lit("Unknown"))
            .otherwise(F.trim(F.col("use_chip")))
            .alias("use_chip"),
            F.col("merchant_id"),
            F.when(_blank_or_null("merchant_city"), F.lit(None))
            .otherwise(F.trim(F.col("merchant_city")))
            .alias("merchant_city"),
            F.when(_blank_or_null("merchant_state"), F.lit(None))
            .otherwise(F.trim(F.col("merchant_state")))
            .alias("merchant_state"),
            F.when(_blank_or_null("zip"), F.lit(None))
            .otherwise(F.regexp_replace(F.trim(F.col("zip")), r"\.0$", ""))
            .alias("zip"),
            F.col("mcc"),
            F.when(_blank_or_null("errors"), F.lit(None))
            .otherwise(F.trim(F.col("errors")))
            .alias("errors"),
        )
        .where(
            F.col("transaction_id").isNotNull()
            & F.col("transaction_time").isNotNull()
            & F.col("amount").isNotNull()
        )
        .dropDuplicates(["transaction_id"])
        .withColumn("transaction_year", F.year("transaction_time"))
    )


def clean_labels(raw_labels: DataFrame) -> DataFrame:
    return (
        raw_labels.select(
            "transaction_id",
            F.when(F.upper(F.trim(F.col("label"))) == "YES", F.lit(1))
            .when(F.upper(F.trim(F.col("label"))) == "NO", F.lit(0))
            .otherwise(F.lit(None).cast("int"))
            .alias("class"),
        )
        .where(F.col("transaction_id").isNotNull() & F.col("class").isNotNull())
        .dropDuplicates(["transaction_id"])
    )


def collect_clean_profile(
    cleaned_transactions: DataFrame,
    cleaned_labels: DataFrame,
    labeled: DataFrame,
) -> dict[str, Any]:
    transaction_count = cleaned_transactions.count()
    label_count = cleaned_labels.count()
    labeled_count = labeled.count()

    class_rows = labeled.groupBy("class").count().orderBy("class").collect()
    class_distribution = {
        str(row["class"]): {
            "count": int(row["count"]),
            "percentage": round(100.0 * row["count"] / labeled_count, 6),
        }
        for row in class_rows
    }

    amount_row = labeled.agg(
        F.min("amount").alias("min"),
        F.max("amount").alias("max"),
        F.avg("amount").alias("mean"),
        F.stddev("amount").alias("stddev"),
        F.expr("percentile_approx(amount, array(0.25, 0.5, 0.75), 10000)").alias(
            "quartiles"
        ),
    ).first()

    return {
        "clean_transaction_count": int(transaction_count),
        "valid_label_count": int(label_count),
        "labeled_transaction_count": int(labeled_count),
        "transactions_without_training_label": int(transaction_count - labeled_count),
        "label_join_coverage_percentage": round(
            100.0 * labeled_count / transaction_count, 6
        ),
        "class_distribution": class_distribution,
        "amount_statistics": {
            "min": float(amount_row["min"]),
            "max": float(amount_row["max"]),
            "mean": float(amount_row["mean"]),
            "stddev": float(amount_row["stddev"]),
            "quartiles": [float(value) for value in amount_row["quartiles"]],
        },
    }


def write_report(report: dict[str, Any], path: str) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary, destination)


def run_pipeline(
    spark: SparkSession,
    transactions_path: str,
    labels_path: str,
    output_path: str,
    report_path: str,
    labeled_output_path: str = DEFAULT_LABELED_OUTPUT_PATH,
) -> dict[str, Any]:
    print(f"[phase2] Reading transactions from {transactions_path}")
    raw_transactions = read_raw_transactions(spark, transactions_path)
    raw_labels = read_normalized_labels(spark, labels_path)

    print("[phase2] Profiling raw transactions with Spark")
    raw_profile = collect_raw_profile(raw_transactions)

    print("[phase2] Cleaning transactions and labels")
    cleaned_transactions = clean_transactions(raw_transactions).persist(
        StorageLevel.DISK_ONLY
    )
    cleaned_labels = clean_labels(raw_labels).persist(StorageLevel.DISK_ONLY)
    labeled = (
        cleaned_transactions.join(cleaned_labels, "transaction_id", "inner")
        .select(
            "transaction_id",
            "transaction_time",
            "client_id",
            "card_id",
            "amount",
            "use_chip",
            "merchant_id",
            "merchant_city",
            "merchant_state",
            "zip",
            "mcc",
            "errors",
            "class",
            "transaction_year",
        )
        .persist(StorageLevel.DISK_ONLY)
    )

    try:
        print("[phase2] Calculating class imbalance and amount statistics")
        clean_profile = collect_clean_profile(
            cleaned_transactions, cleaned_labels, labeled
        )
        clean_profile["dropped_invalid_or_duplicate_transactions"] = int(
            raw_profile["row_count"] - clean_profile["clean_transaction_count"]
        )

        print(f"[phase2] Writing all cleaned transactions to {output_path}")
        (
            cleaned_transactions.write.mode("overwrite")
            .partitionBy("transaction_year")
            .parquet(output_path)
        )

        print(
            "[phase2] Writing the supervised-learning subset to "
            f"{labeled_output_path}"
        )
        (
            labeled.write.mode("overwrite")
            .partitionBy("transaction_year")
            .parquet(labeled_output_path)
        )

        print("[phase2] Reading both Parquet datasets back for validation")
        validated_all = spark.read.parquet(output_path)
        validated_labeled = spark.read.parquet(labeled_output_path)
        all_parquet_count = validated_all.count()
        labeled_parquet_count = validated_labeled.count()
        if all_parquet_count != clean_profile["clean_transaction_count"]:
            raise RuntimeError(
                "All-transactions Parquet row count mismatch: "
                f"expected {clean_profile['clean_transaction_count']}, "
                f"got {all_parquet_count}"
            )
        if labeled_parquet_count != clean_profile["labeled_transaction_count"]:
            raise RuntimeError(
                "Labeled Parquet row count mismatch: "
                f"expected {clean_profile['labeled_transaction_count']}, "
                f"got {labeled_parquet_count}"
            )

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "inputs": {
                "transactions": transactions_path,
                "labels": labels_path,
            },
            "outputs": {
                "all_clean_transactions": output_path,
                "labeled_transactions": labeled_output_path,
            },
            "raw_profile": raw_profile,
            "clean_profile": clean_profile,
            "parquet_validation": {
                "all_clean_transactions": {
                    "row_count": int(all_parquet_count),
                    "column_count": len(validated_all.columns),
                    "columns": validated_all.columns,
                },
                "labeled_transactions": {
                    "row_count": int(labeled_parquet_count),
                    "column_count": len(validated_labeled.columns),
                    "columns": validated_labeled.columns,
                },
            },
        }
        write_report(report, report_path)

        print(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"[phase2] PASS report={report_path}")
        return report
    finally:
        labeled.unpersist()
        cleaned_labels.unpersist()
        cleaned_transactions.unpersist()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transactions", default=DEFAULT_TRANSACTIONS_PATH)
    parser.add_argument("--labels", default=DEFAULT_LABELS_PATH)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--labeled-output", default=DEFAULT_LABELED_OUTPUT_PATH)
    parser.add_argument("--report", default=DEFAULT_REPORT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_pipeline(
            spark,
            transactions_path=args.transactions,
            labels_path=args.labels,
            output_path=args.output,
            report_path=args.report,
            labeled_output_path=args.labeled_output,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
