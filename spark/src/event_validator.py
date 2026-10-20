
import json
from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

APP_NAME = "AdFraudEventValidator"
BOOTSTRAP_SERVERS = "ad-fraud-kafka:9093"
INPUT_TOPIC = "ad-events"
OUTPUT_TOPIC = "validated-ad-events"
CHECKPOINT_LOCATION = "/tmp/ad-fraud-event-validator-checkpoint-v1"

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

INPUT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("app", IntegerType(), True),
    StructField("device", IntegerType(), True),
    StructField("os", IntegerType(), True),
    StructField("channel", IntegerType(), True),
])

OUTPUT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("app", IntegerType(), True),
    StructField("device", IntegerType(), True),
    StructField("os", IntegerType(), True),
    StructField("channel", IntegerType(), True),
    StructField("is_late_event", BooleanType(), False),
])

# Keep the latest accepted event timestamp as Unix nanoseconds.
STATE_SCHEMA = StructType([
    StructField("latest_accepted_ns", LongType(), False),
])


def validate_ip_events(
    key: tuple,
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState,
):
    """Apply one strict timestamp ordering decision per IP."""

    frames = list(pdf_iter)
    if not frames:
        return

    pdf = pdf.sort_values(
    "kafka_offset",
    kind="mergesort",
    )

    # Stable tie-breaking makes same-batch processing deterministic.
    pdf["_sort_time"] = pd.to_datetime(
        pdf["event_time"], errors="coerce", utc=True
    )
    pdf["_sort_id"] = pdf["event_id"].fillna("").astype(str)

    pdf = pdf.sort_values(
        ["_sort_time", "_sort_id"],
        kind="mergesort",
        na_position="last",
    )

    latest_ns = int(state.get[0]) if state.exists else None
    output = []

    for _, row in pdf.iterrows():
        timestamp = row["timestamp"]
        event_id = row["event_id"]
        event_time = row["_sort_time"]

        # An invalid timestamp cannot safely advance IP state.
        if pd.isna(event_time):
            is_late = True
        else:
            event_ns = int(event_time.value)
            is_late = (
                latest_ns is not None and event_ns < latest_ns
            )

            # Only accepted events advance the per-IP watermark.
            if not is_late:
                latest_ns = event_ns

        output.append({
            "event_id": None if pd.isna(event_id) else str(event_id),
            "timestamp": None if pd.isna(timestamp) else str(timestamp),
            "ip": None if pd.isna(row["ip"]) else int(row["ip"]),
            "app": None if pd.isna(row["app"]) else int(row["app"]),
            "device": None if pd.isna(row["device"]) else int(row["device"]),
            "os": None if pd.isna(row["os"]) else int(row["os"]),
            "channel": None if pd.isna(row["channel"]) else int(row["channel"]),
            "is_late_event": bool(is_late),
        })

    if latest_ns is not None:
        state.update((latest_ns,))

    yield pd.DataFrame(output, columns=[
        field.name for field in OUTPUT_SCHEMA.fields
    ])


raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(
        from_json(col("value"), INPUT_SCHEMA).alias("event"),
        col("offset").alias("kafka_offset"),
    )
    .select("event.*", "kafka_offset")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

validated = (
    events
    .groupBy("ip")
    .applyInPandasWithState(
        func=validate_ip_events,
        outputStructType=OUTPUT_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="Update",
        timeoutConf="NoTimeout",
    )
)


def publish_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Key by IP. Preserve the original event fields and attach the
    # single authoritative lateness decision.
    kafka_rows = batch_df.selectExpr(
        "CAST(ip AS STRING) AS key",
        """
        to_json(named_struct(
            'event_id', event_id,
            'timestamp', timestamp,
            'ip', ip,
            'app', app,
            'device', device,
            'os', os,
            'channel', channel,
            'is_late_event', is_late_event
        )) AS value
        """,
    )

    (
        kafka_rows.write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .save()
    )

    print(
        f"Validator batch {batch_id}: "
        f"{batch_df.count()} events published"
    )


query = (
    validated.writeStream
    .foreachBatch(publish_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()