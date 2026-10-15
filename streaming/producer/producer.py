import json
import logging
import os
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("stream-producer")

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "stream-kafka:19092")
TOPIC = os.getenv("TRANSACTIONS_TOPIC", "flink-transactions")
TPS = max(0.2, float(os.getenv("TRANSACTIONS_PER_SECOND", "1")))
COUNTRIES = ("US", "VN", "SG", "TH")
STATES = ("CA", "NY", "TX", "FL", "WA", "ND")
MCC_CODES = (5411, 5499, 5311, 4829, 5732)


def main():
    producer = Producer({"bootstrap.servers": BROKERS, "client.id": "fraud-detection-demo"})
    sequence = 0
    try:
        while True:
            sequence += 1
            high_amount = sequence % 8 == 0
            account_id = f"ACC-{1000 + sequence % 50}"
            # Keep demo features close to common historical transactions so the model can distinguish outliers.
            event_time = datetime(2019, 12, 31, 13, random.randrange(60), random.randrange(60))
            online = sequence % 10 == 0
            transaction = {
                "transactionId": str(uuid.uuid4()),
                "accountId": account_id,
                "amount": round(random.uniform(1500, 5000) if high_amount else random.uniform(10, 80), 2),
                "currency": "USD",
                "country": random.choice(COUNTRIES),
                "transaction_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
                "use_chip": "Online Transaction" if online else random.choice(("Swipe Transaction", "Chip Transaction")),
                "merchant_state": None if online else random.choice(STATES),
                "mcc": random.choice(MCC_CODES),
                "errors": "Bad PIN" if sequence % 41 == 0 else None,
                "timestamp": int(time.time() * 1000),
                "scenario": "HIGH_AMOUNT" if high_amount else "NORMAL",
            }
            producer.produce(
                TOPIC,
                key=account_id.encode("utf-8"),
                value=json.dumps(transaction, separators=(",", ":")).encode("utf-8"),
            )
            producer.poll(0)
            logger.info("Sent %s transaction amount=%.2f", transaction["scenario"], transaction["amount"])
            time.sleep(1 / TPS)
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
