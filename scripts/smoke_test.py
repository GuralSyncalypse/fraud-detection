"""Phase 1 smoke test: Spark cluster execution plus HDFS write/read."""

import sys

from pyspark.sql import SparkSession


HDFS_TEST_PATH = "hdfs:///financial/_health/spark-smoke"


def main() -> int:
    spark = (
        SparkSession.builder.appName("phase-1-infrastructure-smoke-test")
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        expected = 1000
        distributed_count = spark.range(expected).repartition(4).count()
        if distributed_count != expected:
            raise RuntimeError(
                f"Spark count mismatch: expected {expected}, got {distributed_count}"
            )

        source = spark.createDataFrame(
            [(1, "spark-to-hdfs-ok"), (2, "two-workers-ready")],
            ["id", "message"],
        )
        source.write.mode("overwrite").parquet(HDFS_TEST_PATH)
        hdfs_count = spark.read.parquet(HDFS_TEST_PATH).count()
        if hdfs_count != 2:
            raise RuntimeError(f"HDFS read mismatch: expected 2, got {hdfs_count}")

        print(f"PASS spark_master={spark.sparkContext.master}")
        print(f"PASS distributed_count={distributed_count}")
        print(f"PASS hdfs_path={HDFS_TEST_PATH} rows={hdfs_count}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

