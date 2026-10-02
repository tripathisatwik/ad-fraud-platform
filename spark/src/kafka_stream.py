from collections import defaultdict, deque

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = (
    SparkSession.builder
    .appName("AdFraudKafkaStream")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "ad-fraud-kafka:9093"
KAFKA_TOPIC = "ad-events"

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("app", IntegerType(), True),
    StructField("device", IntegerType(), True),
    StructField("os", IntegerType(), True),
    StructField("channel", IntegerType(), True),
])

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
        from_json(
            col("value").cast("string"),
            event_schema,
        ).alias("event")
    )
    .select("event.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

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

# Driver-side state across micro-batches
ip_count = defaultdict(int)
ip_app_count = defaultdict(int)
device_count = defaultdict(int)
app_count = defaultdict(int)
os_count = defaultdict(int)
channel_count = defaultdict(int)
app_channel_count = defaultdict(int)
app_device_count = defaultdict(int)

ip_first_seen = {}
ip_last_click = {}

ip_apps_seen = defaultdict(set)
ip_devices_seen = defaultdict(set)
ip_channels_seen = defaultdict(set)

ip_click_times = defaultdict(deque)
ip_app_click_times = defaultdict(deque)


def _rolling_count(ts_deque, now, window_seconds):
    cutoff = now - pd.Timedelta(seconds=window_seconds)
    count = 0
    for ts in reversed(ts_deque):
        if ts <= cutoff:
            break
        count += 1
    return count


def _prune_deque(ts_deque, now):
    cutoff = now - pd.Timedelta(seconds=MAX_WINDOW_SECONDS)
    while ts_deque and ts_deque[0] <= cutoff:
        ts_deque.popleft()


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    pdf["event_time"] = pd.to_datetime(pdf["event_time"], utc=True)
    pdf = pdf.sort_values(["event_time", "event_id"]).reset_index(drop=True)

    n = len(pdf)
    out = {feat: [None] * n for feat in FEATURE_COLUMNS}

    out_event_id = [None] * n
    out_timestamp = [None] * n
    out_event_time = [None] * n
    out_ip = [None] * n
    out_app = [None] * n
    out_device = [None] * n
    out_os = [None] * n
    out_channel = [None] * n

    for i, row in pdf.iterrows():
        ip = int(row["ip"])
        app = int(row["app"])
        device = int(row["device"])
        os_val = int(row["os"])
        ch = int(row["channel"])
        et = row["event_time"]

        out_event_id[i] = row["event_id"]
        out_timestamp[i] = row["timestamp"]
        out_event_time[i] = str(et)
        out_ip[i] = ip
        out_app[i] = app
        out_device[i] = device
        out_os[i] = os_val
        out_channel[i] = ch

        out["hour"][i] = et.hour
        out["day_of_week"][i] = et.dayofweek

        # Read counts before updating state
        out["ip_clicks_before"][i] = ip_count[ip]
        out["ip_app_clicks_before"][i] = ip_app_count[(ip, app)]
        out["device_clicks_before"][i] = device_count[device]
        out["app_clicks_before"][i] = app_count[app]
        out["os_clicks_before"][i] = os_count[os_val]
        out["channel_clicks_before"][i] = channel_count[ch]
        out["app_channel_clicks_before"][i] = app_channel_count[(app, ch)]
        out["app_device_clicks_before"][i] = app_device_count[(app, device)]

        if ip in ip_last_click:
            out["seconds_since_ip_click"][i] = (et - ip_last_click[ip]).total_seconds()
        else:
            out["seconds_since_ip_click"][i] = -1.0

        if ip in ip_first_seen:
            out["seconds_since_ip_first_seen"][i] = (et - ip_first_seen[ip]).total_seconds()
        else:
            out["seconds_since_ip_first_seen"][i] = 0.0

        out["distinct_apps_per_ip_before"][i] = len(ip_apps_seen[ip])
        out["distinct_devices_per_ip_before"][i] = len(ip_devices_seen[ip])
        out["distinct_channels_per_ip_before"][i] = len(ip_channels_seen[ip])

        ip_deque = ip_click_times[ip]
        out["ip_clicks_last_1min"][i] = _rolling_count(ip_deque, et, ROLLING_WINDOWS["1min"])
        out["ip_clicks_last_5min"][i] = _rolling_count(ip_deque, et, ROLLING_WINDOWS["5min"])
        out["ip_clicks_last_10min"][i] = _rolling_count(ip_deque, et, ROLLING_WINDOWS["10min"])
        out["ip_clicks_last_1h"][i] = _rolling_count(ip_deque, et, ROLLING_WINDOWS["1h"])

        ip_app_deque = ip_app_click_times[(ip, app)]
        out["ip_app_clicks_last_1min"][i] = _rolling_count(ip_app_deque, et, ROLLING_WINDOWS["1min"])
        out["ip_app_clicks_last_5min"][i] = _rolling_count(ip_app_deque, et, ROLLING_WINDOWS["5min"])
        out["ip_app_clicks_last_10min"][i] = _rolling_count(ip_app_deque, et, ROLLING_WINDOWS["10min"])
        out["ip_app_clicks_last_1h"][i] = _rolling_count(ip_app_deque, et, ROLLING_WINDOWS["1h"])

        # Update state for subsequent events
        ip_count[ip] += 1
        ip_app_count[(ip, app)] += 1
        device_count[device] += 1
        app_count[app] += 1
        os_count[os_val] += 1
        channel_count[ch] += 1
        app_channel_count[(app, ch)] += 1
        app_device_count[(app, device)] += 1

        if ip not in ip_first_seen:
            ip_first_seen[ip] = et
        ip_last_click[ip] = et

        ip_apps_seen[ip].add(app)
        ip_devices_seen[ip].add(device)
        ip_channels_seen[ip].add(ch)

        ip_deque.append(et)
        _prune_deque(ip_deque, et)
        ip_app_deque.append(et)
        _prune_deque(ip_app_deque, et)

    result = pd.DataFrame({
        "event_id": out_event_id,
        "timestamp": out_timestamp,
        "event_time": out_event_time,
        "ip": out_ip,
        "app": out_app,
        "device": out_device,
        "os": out_os,
        "channel": out_channel,
        **out,
    })

    result_sdf = spark.createDataFrame(result)
    print(f"\n--- Batch {batch_id} ({n} events) ---")
    result_sdf.select(
        "event_id",
        "timestamp",
        "ip",
        "app",
        "device",
        "os",
        "channel",
        *FEATURE_COLUMNS,
    ).show(20, truncate=False)


query = (
    events
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/tmp/ad-fraud-spark-checkpoint")
    .start()
)

query.awaitTermination()