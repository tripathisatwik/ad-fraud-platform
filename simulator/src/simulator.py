from datetime import datetime, timezone
import json
import random
import time
import uuid

import pandas as pd
from kafka import KafkaProducer

DATASET_PATH = "data/raw/train_sample.xlsx"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ad-events"

EVENT_COLUMNS = [
    "ip",
    "app",
    "device",
    "os",
    "channel",
]

BURST_PROBABILITY = 0.10
BURST_SIZE = 5
EVENT_INTERVAL_SECONDS = 1

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

def send_event(producer, event):
    producer.send(KAFKA_TOPIC,value=event)
    producer.flush()

def load_dataset():
    return pd.read_excel(DATASET_PATH)


def generate_normal_event(df):
    row = df.sample(n=1).iloc[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    for column in EVENT_COLUMNS:
        event[column] = int(row[column])

    return event


def generate_burst_events(df, ip):
    ip_rows = df[df["ip"] == ip]

    events = []

    for _ in range(BURST_SIZE):
        row = ip_rows.sample(n=1).iloc[0]

        event = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        for column in EVENT_COLUMNS:
            event[column] = int(row[column])

        events.append(event)

    return events


def main():
    print("Loading historical dataset...")
    df = load_dataset()
    print(f"Rows loaded: {len(df):,}\n")

    print("Connecting to Kafka...")
    producer = create_kafka_producer()
    print(f"Connected to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Target Topic: {KAFKA_TOPIC}\n")

    print("Starting event stream...")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            if random.random() < BURST_PROBABILITY:
                ip = int(df["ip"].sample(n=1).iloc[0])

                print(
                    f"\n--- Starting IP burst: {ip} "
                    f"({BURST_SIZE} events) ---",
                    flush=True,
                )

                burst_events = generate_burst_events(df, ip)

                for event in burst_events:
                    send_event(producer, event)
                    print(json.dumps(event))
                    time.sleep(EVENT_INTERVAL_SECONDS)

                print("--- IP burst complete ---\n", flush=True)

            else:
                event = generate_normal_event(df)

                send_event(producer, event)
                print(json.dumps(event))
                time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nSimulator stopped.")


if __name__ == "__main__":
    main()