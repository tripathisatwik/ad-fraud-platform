import json
from typing import Iterator

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, from_json, lit, to_timestamp
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder
    .appName("AdFraudKafkaStream")
    .getOrCreate()
)
# Lower Spark log level to ERROR or WARN
spark.sparkContext.setLogLevel("ERROR")

# Suppress underlying Kafka/Log4j logger output
log4j = spark._jvm.org.apache.log4j
log4j.LogManager.getLogger("org.apache.kafka").setLevel(
    log4j.Level.ERROR
)
log4j.LogManager.getLogger("org.apache.spark.sql.execution.streaming").setLevel(
    log4j.Level.ERROR
)

KAFKA_BOOTSTRAP_SERVERS = "ad-fraud-kafka:9093"
KAFKA_TOPIC = "ad-events"
CHECKPOINT_LOCATION = "/tmp/ad-fraud-spark-stateful-checkpoint"

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("app", IntegerType(), True),
    StructField("device", IntegerType(), True),
    StructField("os", IntegerType(), True),
    StructField("channel", IntegerType(), True),
])

FEATURE_COLUMNS = [
    "hour",
    "day_of_week",
    "ip_clicks_before",
    "ip_app_clicks_before",
    "device_clicks_before",
    "app_clicks_before",
    "os_clicks_before",
    "channel_clicks_before",
    "app_channel_clicks_before",
    "app_device_clicks_before",
    "seconds_since_ip_click",
    "seconds_since_ip_first_seen",
    "distinct_apps_per_ip_before",
    "distinct_devices_per_ip_before",
    "distinct_channels_per_ip_before",
    "ip_clicks_last_1min",
    "ip_clicks_last_5min",
    "ip_clicks_last_10min",
    "ip_clicks_last_1h",
    "ip_app_clicks_last_1min",
    "ip_app_clicks_last_5min",
    "ip_app_clicks_last_10min",
    "ip_app_clicks_last_1h",
]

ROLLING_WINDOWS = {
    "1min": 60,
    "5min": 300,
    "10min": 600,
    "1h": 3600,
}
MAX_WINDOW_SECONDS = 3600

SCOPE_IP = "IP"
SCOPE_IP_APP = "IP_APP"
SCOPE_DEVICE = "DEVICE"
SCOPE_APP = "APP"
SCOPE_OS = "OS"
SCOPE_CHANNEL = "CHANNEL"
SCOPE_APP_CHANNEL = "APP_CHANNEL"
SCOPE_APP_DEVICE = "APP_DEVICE"

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw_stream
    .select(
        from_json(col("value").cast("string"), event_schema).alias("event")
    )
    .select("event.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

base_event_columns = [
    "event_id",
    "timestamp",
    "event_time",
    "ip",
    "app",
    "device",
    "os",
    "channel",
]


def make_scope(scope_type, scope_key):
    return (
        events
        .select(
            *base_event_columns,
            lit(scope_type).alias("scope_type"),
            scope_key.alias("scope_key"),
        )
    )


scoped_events = (
    make_scope(SCOPE_IP, col("ip").cast("string"))
    .unionByName(make_scope(SCOPE_IP_APP, concat_ws("|", col("ip").cast("string"), col("app").cast("string"))))
    .unionByName(make_scope(SCOPE_DEVICE, col("device").cast("string")))
    .unionByName(make_scope(SCOPE_APP, col("app").cast("string")))
    .unionByName(make_scope(SCOPE_OS, col("os").cast("string")))
    .unionByName(make_scope(SCOPE_CHANNEL, col("channel").cast("string")))
    .unionByName(make_scope(SCOPE_APP_CHANNEL, concat_ws("|", col("app").cast("string"), col("channel").cast("string"))))
    .unionByName(make_scope(SCOPE_APP_DEVICE, concat_ws("|", col("app").cast("string"), col("device").cast("string"))))
)

# Collections are serialized as JSON strings in the state tuple
STATE_SCHEMA = StructType([
    StructField("count", LongType(), True),
    StructField("first_seen", StringType(), True),
    StructField("last_click", StringType(), True),
    StructField("seen_apps", StringType(), True),
    StructField("seen_devices", StringType(), True),
    StructField("seen_channels", StringType(), True),
    StructField("click_times", StringType(), True),
])

OUTPUT_COLUMNS = [
    "event_id",
    "timestamp",
    "event_time",
    "ip",
    "app",
    "device",
    "os",
    "channel",
    "scope_type",
    "scope_key",
    *FEATURE_COLUMNS,
]

OUTPUT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("app", IntegerType(), True),
    StructField("device", IntegerType(), True),
    StructField("os", IntegerType(), True),
    StructField("channel", IntegerType(), True),
    StructField("scope_type", StringType(), False),
    StructField("scope_key", StringType(), False),
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("ip_clicks_before", LongType(), True),
    StructField("ip_app_clicks_before", LongType(), True),
    StructField("device_clicks_before", LongType(), True),
    StructField("app_clicks_before", LongType(), True),
    StructField("os_clicks_before", LongType(), True),
    StructField("channel_clicks_before", LongType(), True),
    StructField("app_channel_clicks_before", LongType(), True),
    StructField("app_device_clicks_before", LongType(), True),
    StructField("seconds_since_ip_click", DoubleType(), True),
    StructField("seconds_since_ip_first_seen", DoubleType(), True),
    StructField("distinct_apps_per_ip_before", LongType(), True),
    StructField("distinct_devices_per_ip_before", LongType(), True),
    StructField("distinct_channels_per_ip_before", LongType(), True),
    StructField("ip_clicks_last_1min", LongType(), True),
    StructField("ip_clicks_last_5min", LongType(), True),
    StructField("ip_clicks_last_10min", LongType(), True),
    StructField("ip_clicks_last_1h", LongType(), True),
    StructField("ip_app_clicks_last_1min", LongType(), True),
    StructField("ip_app_clicks_last_5min", LongType(), True),
    StructField("ip_app_clicks_last_10min", LongType(), True),
    StructField("ip_app_clicks_last_1h", LongType(), True),
])


def _empty_state():
    return {
        "count": 0,
        "first_seen": None,
        "last_click": None,
        "seen_apps": set(),
        "seen_devices": set(),
        "seen_channels": set(),
        "click_times": [],
    }


def _decode_state(state: GroupState):
    if not state.exists:
        return _empty_state()

    values = state.get
    return {
        "count": int(values[0] or 0),
        "first_seen": pd.Timestamp(values[1]).to_pydatetime() if values[1] else None,
        "last_click": pd.Timestamp(values[2]).to_pydatetime() if values[2] else None,
        "seen_apps": set(json.loads(values[3]) if values[3] else []),
        "seen_devices": set(json.loads(values[4]) if values[4] else []),
        "seen_channels": set(json.loads(values[5]) if values[5] else []),
        "click_times": [
            pd.Timestamp(v) for v in (json.loads(values[6]) if values[6] else [])
        ],
    }


def _encode_state(state):
    first_seen = state["first_seen"]
    first_seen_str = first_seen.isoformat() if hasattr(first_seen, "isoformat") else first_seen
    last_click = state["last_click"]
    last_click_str = last_click.isoformat() if hasattr(last_click, "isoformat") else last_click

    return (
        int(state["count"]),
        first_seen_str,
        last_click_str,
        json.dumps(sorted(state["seen_apps"])),
        json.dumps(sorted(state["seen_devices"])),
        json.dumps(sorted(state["seen_channels"])),
        json.dumps([
            ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            for ts in state["click_times"]
        ]),
    )


def _rolling_count(click_times, now, window_seconds):
    cutoff = now - pd.Timedelta(seconds=window_seconds)
    count = 0
    for ts in reversed(click_times):
        if ts <= cutoff:
            break
        count += 1
    return count


def _prune_click_times(click_times, now):
    cutoff = now - pd.Timedelta(seconds=MAX_WINDOW_SECONDS)
    while click_times and click_times[0] <= cutoff:
        click_times.pop(0)


def _empty_feature_record():
    return {feature: None for feature in FEATURE_COLUMNS}


def process_scope(
    key,
    pdf_iter: Iterator[pd.DataFrame],
    state: GroupState,
):
    scope_type = str(key[0])
    scope_key = str(key[1])

    current = _decode_state(state)
    pdfs = list(pdf_iter)
    if not pdfs:
        return

    pdf = pd.concat(pdfs, ignore_index=True)
    pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)
    pdf = pdf.sort_values(["event_time", "event_id"]).reset_index(drop=True)

    results = []

    for _, row in pdf.iterrows():
        event_id = str(row["event_id"])
        timestamp = row["timestamp"]
        event_time = pd.Timestamp(row["event_time"])

        ip = int(row["ip"])
        app = int(row["app"])
        device = int(row["device"])
        os_value = int(row["os"])
        channel = int(row["channel"])

        features = _empty_feature_record()

        if scope_type == SCOPE_IP:
            features["hour"] = int(event_time.hour)
            features["day_of_week"] = int(event_time.dayofweek)

        # Scoped cumulative counters (count before this event)
        if scope_type == SCOPE_IP:
            features["ip_clicks_before"] = current["count"]
        elif scope_type == SCOPE_IP_APP:
            features["ip_app_clicks_before"] = current["count"]
        elif scope_type == SCOPE_DEVICE:
            features["device_clicks_before"] = current["count"]
        elif scope_type == SCOPE_APP:
            features["app_clicks_before"] = current["count"]
        elif scope_type == SCOPE_OS:
            features["os_clicks_before"] = current["count"]
        elif scope_type == SCOPE_CHANNEL:
            features["channel_clicks_before"] = current["count"]
        elif scope_type == SCOPE_APP_CHANNEL:
            features["app_channel_clicks_before"] = current["count"]
        elif scope_type == SCOPE_APP_DEVICE:
            features["app_device_clicks_before"] = current["count"]

        # IP-level features
        if scope_type == SCOPE_IP:
            if current["last_click"] is None:
                features["seconds_since_ip_click"] = -1.0
            else:
                previous = pd.Timestamp(current["last_click"])
                features["seconds_since_ip_click"] = (event_time - previous).total_seconds()

            if current["first_seen"] is None:
                features["seconds_since_ip_first_seen"] = 0.0
            else:
                first = pd.Timestamp(current["first_seen"])
                features["seconds_since_ip_first_seen"] = (event_time - first).total_seconds()

            features["distinct_apps_per_ip_before"] = len(current["seen_apps"])
            features["distinct_devices_per_ip_before"] = len(current["seen_devices"])
            features["distinct_channels_per_ip_before"] = len(current["seen_channels"])

            features["ip_clicks_last_1min"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["1min"])
            features["ip_clicks_last_5min"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["5min"])
            features["ip_clicks_last_10min"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["10min"])
            features["ip_clicks_last_1h"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["1h"])

        if scope_type == SCOPE_IP_APP:
            features["ip_app_clicks_last_1min"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["1min"])
            features["ip_app_clicks_last_5min"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["5min"])
            features["ip_app_clicks_last_10min"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["10min"])
            features["ip_app_clicks_last_1h"] = _rolling_count(current["click_times"], event_time, ROLLING_WINDOWS["1h"])

        result = {
            "event_id": event_id,
            "timestamp": timestamp,
            "event_time": event_time.isoformat(),
            "ip": ip,
            "app": app,
            "device": device,
            "os": os_value,
            "channel": channel,
            "scope_type": scope_type,
            "scope_key": scope_key,
        }
        result.update(features)
        results.append(result)

        # Update state after generating features for this event
        current["count"] += 1
        if current["first_seen"] is None:
            current["first_seen"] = event_time.to_pydatetime()
        current["last_click"] = event_time.to_pydatetime()

        if scope_type == SCOPE_IP:
            current["seen_apps"].add(app)
            current["seen_devices"].add(device)
            current["seen_channels"].add(channel)

        if scope_type in {SCOPE_IP, SCOPE_IP_APP}:
            current["click_times"].append(event_time)
            current["click_times"].sort()
            _prune_click_times(current["click_times"], event_time)

    state.update(_encode_state(current))

    result_pdf = pd.DataFrame(results).reindex(columns=OUTPUT_COLUMNS)
    yield result_pdf


stateful_features = (
    scoped_events
    .groupBy("scope_type", "scope_key")
    .applyInPandasWithState(
        func=process_scope,
        outputStructType=OUTPUT_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="Update",
        timeoutConf="NoTimeout",
    )
)


def process_output_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    if pdf.empty:
        return

    # Combine partial records from all 8 scopes back into one row per event
    aggregation = {feature: "max" for feature in FEATURE_COLUMNS}
    grouped = (
        pdf
        .groupby(
            [
                "event_id",
                "timestamp",
                "event_time",
                "ip",
                "app",
                "device",
                "os",
                "channel",
            ],
            as_index=False,
        )
        .agg(aggregation)
        .sort_values(["event_time", "event_id"])
    )

    print(f"\n--- FEATURE BATCH {batch_id} ({len(grouped)} events) ---")
    print(
        grouped[
            [
                "event_id",
                "timestamp",
                "ip",
                "app",
                "device",
                "os",
                "channel",
                *FEATURE_COLUMNS,
            ]
        ].head(20).to_string(index=False)
    )
    print()


query = (
    stateful_features
    .writeStream
    .foreachBatch(process_output_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start()
)

query.awaitTermination()
query.awaitTermination()