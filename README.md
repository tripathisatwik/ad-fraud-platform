# Real-Time Ad Fraud Detection Platform

## Overview

A real-time data engineering and machine learning platform that simulates digital advertising events, processes them as a stream, engineers fraud-related behavioral features, applies an XGBoost classification model, and exposes prediction results through an API.

## Goals

- Generate simulated advertising interaction events.
- Stream events through Apache Kafka.
- Process streaming data using Apache Spark Structured Streaming.
- Build behavioral features for fraud detection.
- Train an XGBoost classification model.
- Perform real-time fraud inference.
- Store raw and processed data in an S3-compatible data lake through Floci.
- Expose prediction and fraud-monitoring endpoints through FastAPI.
- Use AWS CLI to interact with locally emulated AWS services.

## Technology Stack

- Python
- Apache Kafka
- Apache Spark / PySpark
- XGBoost
- FastAPI
- Docker / Docker Compose
- Floci
- AWS CLI
- S3
- AWS Glue
- Amazon Athena
- CloudWatch
- AWS Secrets Manager
- AWS Systems Manager Parameter Store

## Project Status

Currently building the project step by step.

## Project Structure

```text
ad-fraud-platform/
├── docker-compose.yml
├── README.md
├── .gitignore
│
├── simulator/
├── kafka/
├── spark/
├── ml/
├── api/
├── aws/
├── analytics/
├── tests/
└── docs/

## Git Workflow

### Check repository status

```powershell
git status