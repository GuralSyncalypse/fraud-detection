"""Score the Kafka transaction stream with the trained Isolation Forest model."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from confluent_kafka import Consumer, Producer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("isolation-forest-scorer")

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "stream-kafka:19092")
INPUT_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "flink-transactions")
ALERTS_TOPIC = os.getenv("ML_ALERTS_TOPIC", "flink-ml-alerts")
MIN_ANOMALY_SCORE = float(os.getenv("ML_ANOMALY_SCORE_MIN", "0.05"))
MODEL_PATH = Path(os.getenv("ISOLATION_MODEL_PATH", "/workspace/output/models/isolation_forest.joblib"))
FEATURES = [
    "amount",
    "transaction_hour",
    "transaction_day_of_week",
    "transaction_month",
    "transaction_year_numeric",
    "is_weekend",
    "is_online",
    "has_error",
]


def transaction_features(transaction: dict) -> dict[str, float]:
    event_time = datetime.fromisoformat(str(transaction["transaction_time"]).replace("Z", "+00:00"))
    amount = float(transaction["amount"])
    if not math.isfinite(amount):
        raise ValueError("amount must be finite")
    merchant_state = transaction.get("merchant_state")
    errors = transaction.get("errors")
    return {
        "amount": amount,
        "transaction_hour": float(event_time.hour),
        # Spark dayofweek uses Sunday=1 through Saturday=7.
        "transaction_day_of_week": float((event_time.weekday() + 1) % 7 + 1),
        "transaction_month": float(event_time.month),
        "transaction_year_numeric": float(event_time.year),
        "is_weekend": float(event_time.weekday() >= 5),
        "is_online": float(merchant_state is None or not str(merchant_state).strip()),
        "has_error": float(errors is not None and bool(str(errors).strip())),
    }


def main() -> None:
    while not MODEL_PATH.is_file():
        logger.warning("Trained model not found at %s; waiting for Phase 3 training output", MODEL_PATH)
        time.sleep(10)

    model = joblib.load(MODEL_PATH)
    logger.info("Loaded trained Isolation Forest model from %s", MODEL_PATH)

    consumer = Consumer(
        {
            "bootstrap.servers": BROKERS,
            "group.id": "fraud-detection-isolation-forest",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": BROKERS, "client.id": "fraud-detection-isolation-forest"})
    consumer.subscribe([INPUT_TOPIC])
    logger.info("Scoring transactions from %s; alerts go to %s", INPUT_TOPIC, ALERTS_TOPIC)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.warning("Kafka read error: %s", message.error())
                continue

            try:
                transaction = json.loads(message.value().decode("utf-8"))
                features = transaction_features(transaction)
                row = pd.DataFrame([features], columns=FEATURES)
                prediction = int(model.predict(row)[0])
                anomaly_score = float(-model.decision_function(row)[0]) if prediction == -1 else 0.0
                if prediction == -1 and anomaly_score >= MIN_ANOMALY_SCORE:
                    alert = {
                        "transactionId": transaction.get("transactionId", "unknown"),
                        "accountId": transaction.get("accountId", "unknown"),
                        "amount": features["amount"],
                        "anomalyScore": anomaly_score,
                        "status": "NEEDS_REVIEW",
                        "reason": "ISOLATION_FOREST_ANOMALY",
                        "model": "Isolation Forest",
                        "occurredAt": transaction["transaction_time"],
                        "detectedAt": datetime.now(timezone.utc).isoformat(),
                    }
                    producer.produce(
                        ALERTS_TOPIC,
                        key=str(alert["transactionId"]).encode("utf-8"),
                        value=json.dumps(alert, separators=(",", ":")).encode("utf-8"),
                    )
                    if producer.flush(10):
                        raise RuntimeError("Kafka did not confirm ML alert delivery")
                    logger.info("Flagged %s with anomaly score %.5f", alert["transactionId"], alert["anomalyScore"])
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
                logger.warning("Skipping invalid transaction: %s", error)

            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    main()
