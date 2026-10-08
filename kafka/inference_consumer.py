import json
import os
import sys

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

sys.path.insert(0, "/opt")

from ml.inference import get_inference


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "ad-fraud-kafka:9093",
)

INPUT_TOPIC = "prediction-input"
OUTPUT_TOPIC = "predictions"

CONSUMER_GROUP = "ad-fraud-inference"


print("Starting ad-fraud inference consumer...")

inference = get_inference()

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value
    ).encode("utf-8"),
)

print(
    f"Kafka consumer ready: "
    f"{INPUT_TOPIC} -> {OUTPUT_TOPIC}"
)


for message in consumer:
    try:
        event = message.value

        # Convert the single Kafka event into the DataFrame
        # expected by ml.inference.AdFraudInference.predict().
        df = pd.DataFrame([event])

        predictions = inference.predict(df)

        row = predictions.iloc[0]

        result = {
            "event_id": event["event_id"],
            "timestamp": event["timestamp"],
            "event_time": event["event_time"],
            "ip": event["ip"],
            "app": event["app"],
            "device": event["device"],
            "os": event["os"],
            "channel": event["channel"],
            "fraud_probability": float(
                row["fraud_probability"]
            ),
            "prediction": int(
                row["prediction"]
            ),
        }

        producer.send(
            OUTPUT_TOPIC,
            value=result,
        )

        producer.flush()

        print(
            f"Prediction | "
            f"event_id={result['event_id']} | "
            f"probability={result['fraud_probability']:.6f} | "
            f"prediction={result['prediction']}"
        )

    except Exception as exc:
        print(
            f"ERROR processing Kafka message: {exc}"
        )
