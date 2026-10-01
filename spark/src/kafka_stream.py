import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    hour,
    dayofweek,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    MapType,
)
from pyspark.sql.streaming.state import GroupStateTimeout


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


print("Starting Spark Structured Streaming...")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic: {KAFKA_TOPIC}")


# ---------------------------------------------------------
# 1. Read events from Kafka
# ---------------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# ---------------------------------------------------------
# 2. Parse JSON
# ---------------------------------------------------------

events = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("event")
    )
    .select("event.*")
)


# ---------------------------------------------------------
# 3. Basic temporal features
# ---------------------------------------------------------

events = events.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

events = (
    events
    .withColumn("hour", hour(col("event_time")))
    .withColumn(
        "day_of_week",
        (dayofweek(col("event_time")) + 5) % 7
    )
)


# ---------------------------------------------------------
# 4. Stateful IP counter function
# ---------------------------------------------------------

def ip_stateful_features(key, pdf_iter, state):
    """
    Maintains cumulative historical counters for one IP.

    Every feature represents the state BEFORE the current event.
    """

    if state.exists:
        s = state.get
        ip_count = s[0]
        app_counts = s[1]
        device_counts = s[2]
        os_counts = s[3]
        channel_counts = s[4]
        ip_app_counts = s[5]
        ip_channel_counts = s[6]
        ip_device_counts = s[7]
    else:
        ip_count = 0
        app_counts = {}
        device_counts = {}
        os_counts = {}
        channel_counts = {}
        ip_app_counts = {}
        ip_channel_counts = {}
        ip_device_counts = {}

    output_rows = []

    for pdf in pdf_iter:
        for _, row in pdf.iterrows():

            app = int(row["app"])
            device = int(row["device"])
            os_value = int(row["os"])
            channel = int(row["channel"])

            ip_app_key = str(app)
            ip_device_key = str(device)
            ip_channel_key = str(channel)

            output_rows.append({
                "event_id": row["event_id"],
                "timestamp": row["timestamp"],
                "ip": int(row["ip"]),
                "app": app,
                "device": device,
                "os": os_value,
                "channel": channel,
                "hour": int(row["hour"]),
                "day_of_week": int(row["day_of_week"]),

                "ip_clicks_before": ip_count,

                "ip_app_clicks_before":
                    ip_app_counts.get(ip_app_key, 0),

                "device_clicks_before":
                    device_counts.get(ip_device_key, 0),

                "app_clicks_before":
                    app_counts.get(ip_app_key, 0),

                "os_clicks_before":
                    os_counts.get(str(os_value), 0),

                "channel_clicks_before":
                    channel_counts.get(str(channel), 0),

                "app_channel_clicks_before":
                    ip_channel_counts.get(
                        f"{app}_{channel}", 0
                    ),

                "app_device_clicks_before":
                    ip_device_counts.get(
                        f"{app}_{device}", 0
                    ),
            })

            # Update state AFTER generating features.
            ip_count += 1

            app_counts[ip_app_key] = (
                app_counts.get(ip_app_key, 0) + 1
            )

            device_counts[ip_device_key] = (
                device_counts.get(ip_device_key, 0) + 1
            )

            os_counts[str(os_value)] = (
                os_counts.get(str(os_value), 0) + 1
            )

            channel_counts[str(channel)] = (
                channel_counts.get(str(channel), 0) + 1
            )

            ip_app_counts[ip_app_key] = (
                ip_app_counts.get(ip_app_key, 0) + 1
            )

            ip_channel_key = f"{app}_{channel}"
            ip_channel_counts[ip_channel_key] = (
                ip_channel_counts.get(ip_channel_key, 0) + 1
            )

            ip_device_key = f"{app}_{device}"
            ip_device_counts[ip_device_key] = (
                ip_device_counts.get(ip_device_key, 0) + 1
            )

    state.update((
        ip_count,
        app_counts,
        device_counts,
        os_counts,
        channel_counts,
        ip_app_counts,
        ip_channel_counts,
        ip_device_counts,
    ))

    if output_rows:
        yield pd.DataFrame(output_rows)


# ---------------------------------------------------------
# 5. Define output and state schemas
# ---------------------------------------------------------

output_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("app", IntegerType(), True),
    StructField("device", IntegerType(), True),
    StructField("os", IntegerType(), True),
    StructField("channel", IntegerType(), True),
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
])

state_schema = StructType([
    StructField("ip_count", LongType(), False),
    StructField("app_counts", MapType(StringType(), LongType()), False),
    StructField("device_counts", MapType(StringType(), LongType()), False),
    StructField("os_counts", MapType(StringType(), LongType()), False),
    StructField("channel_counts", MapType(StringType(), LongType()), False),
    StructField("ip_app_counts", MapType(StringType(), LongType()), False),
    StructField("ip_channel_counts", MapType(StringType(), LongType()), False),
    StructField("ip_device_counts", MapType(StringType(), LongType()), False),
])


# ---------------------------------------------------------
# 6. Apply stateful processing by IP
# ---------------------------------------------------------

features = (
    events
    .groupBy("ip")
    .applyInPandasWithState(
        ip_stateful_features,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Update",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )
)


# ---------------------------------------------------------
# 7. Write features to console
# ---------------------------------------------------------

query = (
    features
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "/tmp/ad-fraud-spark-stateful-checkpoint"
    )
    .start()
)


query.awaitTermination()