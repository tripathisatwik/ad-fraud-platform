
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("fraud-api")

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://floci:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "ad-fraud-data")
S3_PREFIX = os.getenv("S3_PREFIX", "predictions/")
MAX_OBJECTS = int(os.getenv("API_MAX_OBJECTS", "100"))
MAX_RECORDS = int(os.getenv("API_MAX_RECORDS", "10000"))

s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT_URL,
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)

app = FastAPI(
    title="Ad Fraud Detection API",
    version="1.0.0",
    description="Prediction retrieval and monitoring analytics.",
)

origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "*").split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def as_probability(record: dict[str, Any]) -> float:
    try:
        value = float(record.get("fraud_probability", 0))
        if 0 <= value <= 1:
            return value
    except (TypeError, ValueError):
        pass
    return 0.0


def is_flagged(record: dict[str, Any]) -> bool:
    try:
        return int(record.get("prediction", 0)) == 1
    except (TypeError, ValueError):
        return False


def record_timestamp(record: dict[str, Any]) -> str:
    return str(record.get("timestamp") or record.get("event_time") or "")


def load_predictions() -> dict[str, Any]:
    """Load recent JSONL prediction records from the configured S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
    ):
        objects.extend(page.get("Contents", []))

    objects.sort(
        key=lambda item: item["LastModified"],
        reverse=True,
    )
    objects = objects[:MAX_OBJECTS]

    records = []
    objects_read = 0

    for item in objects:
        if len(records) >= MAX_RECORDS:
            break

        try:
            response = s3.get_object(
                Bucket=S3_BUCKET,
                Key=item["Key"],
            )
            body = response["Body"]
            try:
                payload = body.read().decode("utf-8", errors="replace")
            finally:
                body.close()

            objects_read += 1

            # The S3 sink is expected to write newline-delimited JSON.
            for line in payload.splitlines():
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    log.warning(
                        "Skipping malformed JSON in S3 object %s",
                        item["Key"],
                    )
                    continue

                if (
                    isinstance(record, dict)
                    and "fraud_probability" in record
                    and "prediction" in record
                ):
                    records.append(record)

                    if len(records) >= MAX_RECORDS:
                        break

        except (BotoCoreError, ClientError) as exc:
            log.exception("Could not read S3 object %s", item["Key"])
            raise HTTPException(
                status_code=503,
                detail="Prediction storage is temporarily unavailable.",
            ) from exc

    records.sort(key=record_timestamp, reverse=True)

    return {
        "records": records,
        "objects_read": objects_read,
    }


@app.get("/health")
def health():
    # This confirms the API process is responding, not that S3 is reachable.
    return {"status": "ok"}


@app.get("/api/v1/predictions")
def get_predictions(
    limit: int = Query(default=25, ge=1, le=1000),
):
    data = load_predictions()
    records = data["records"][:limit]

    return {
        "count": len(records),
        "limit": limit,
        "objects_read": data["objects_read"],
        "predictions": records,
    }


@app.get("/api/v1/analytics/summary")
def get_summary():
    data = load_predictions()
    records = data["records"]
    total = len(records)
    flagged = sum(is_flagged(record) for record in records)
    probabilities = [as_probability(record) for record in records]

    return {
        "records_loaded": total,
        "objects_read": data["objects_read"],
        "flagged_count": flagged,
        "not_flagged_count": total - flagged,
        "flagged_rate": flagged / total if total else 0.0,
        "average_probability": (
            sum(probabilities) / total if total else 0.0
        ),
        "maximum_probability": max(probabilities, default=0.0),
        "latest_timestamp": (
            record_timestamp(records[0]) if records else None
        ),
        "data_source": "s3",
    }


@app.get("/api/v1/analytics/hourly")
def get_hourly(
    limit: int = Query(default=12, ge=1, le=168),
):
    data = load_predictions()
    buckets = defaultdict(
        lambda: {
            "events": 0,
            "flagged": 0,
            "probability_sum": 0.0,
        }
    )

    for record in data["records"]:
        timestamp = record_timestamp(record)
        if not timestamp:
            continue

        # Group ISO-formatted timestamps by their displayed hour.
        hour = timestamp[:13].replace("T", " ")
        bucket = buckets[hour]
        bucket["events"] += 1
        bucket["flagged"] += int(is_flagged(record))
        bucket["probability_sum"] += as_probability(record)

    rows = []
    for hour, bucket in sorted(buckets.items())[-limit:]:
        count = bucket["events"]
        rows.append(
            {
                "hour": hour,
                "events": count,
                "flagged": bucket["flagged"],
                "average_probability": (
                    bucket["probability_sum"] / count if count else 0.0
                ),
            }
        )

    return {
        "count": len(rows),
        "limit": limit,
        "objects_read": data["objects_read"],
        "hours": rows,
    }
