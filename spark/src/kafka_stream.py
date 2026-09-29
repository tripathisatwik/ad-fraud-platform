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
)

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

events = events.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

events = (
    events
    .withColumn("hour", hour(col("event_time")))
    .withColumn(
        "day_of_week",
        dayofweek(col("event_time")) - 2
    )
)

feature_columns = [
    "event_id",
    "timestamp",
    "ip",
    "app",
    "device",
    "os",
    "channel",
    "hour",
    "day_of_week",
]


query = (
    events
    .select(*feature_columns)
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "/tmp/ad-fraud-spark-feature-checkpoint"
    )
    .start()
)


query.awaitTermination()