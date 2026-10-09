import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("prediction-sink")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "ad-fraud-kafka:9093"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "predictions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "prediction-s3-sink")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://floci:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "ad-fraud-data")
S3_PREFIX = os.getenv("S3_PREFIX", "predictions")
BATCH_SIZE = int(os.getenv("S3_BATCH_SIZE", "100"))
FLUSH_INTERVAL_SECONDS = int(os.getenv("S3_FLUSH_INTERVAL_SECONDS", "10"))

running = True


def stop_handler(signum, frame):
    global running
    logger.info("Shutdown signal received; flushing remaining records")
    running = False


signal.signal(signal.SIGTERM, stop_handler)
signal.signal(signal.SIGINT, stop_handler)


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


def upload_batch(s3, records):
    if not records:
        return

    now = datetime.now(timezone.utc)
    object_key = (
        f"{S3_PREFIX}/year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"predictions-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.jsonl"
    )

    body = "\n".join(
        json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        for record in records
    ) + "\n"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=object_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    logger.info(
        "Uploaded %d prediction records to s3://%s/%s",
        len(records),
        S3_BUCKET,
        object_key,
    )


def main():
    logger.info("Starting Kafka-to-S3 prediction sink")
    logger.info("Kafka: %s | Topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    logger.info("S3 endpoint: %s | Bucket: %s", AWS_ENDPOINT_URL, S3_BUCKET)

    s3 = create_s3_client()
    s3.head_bucket(Bucket=S3_BUCKET)
    logger.info("S3 bucket verified")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    batch = []
    last_flush = time.monotonic()

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for message in messages:
                    batch.append(message.value)

            time_to_flush = (
                time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS
            )

            if batch and (len(batch) >= BATCH_SIZE or time_to_flush or not running):
                # Upload first; commit offsets only after a successful upload.
                upload_batch(s3, batch)
                consumer.commit()
                batch.clear()
                last_flush = time.monotonic()

    finally:
        if batch:
            upload_batch(s3, batch)
            consumer.commit()

        consumer.close()
        logger.info("Prediction sink stopped")


if __name__ == "__main__":
    main()
