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
- Store raw and processed data in an S3-compatible data lake.
- Expose prediction and fraud-monitoring endpoints through FastAPI.
- Use AWS CLI to interact with locally emulated AWS services.

## Technology Stack

- **Languages:** Python
- **Streaming & Processing:** Apache Kafka, Apache Spark / PySpark
- **Machine Learning:** XGBoost, scikit-learn, Pandas
- **API:** FastAPI
- **Infrastructure & Cloud:** Docker / Docker Compose, AWS CLI, S3, AWS Glue, Amazon Athena, CloudWatch, AWS Secrets Manager, AWS Systems Manager Parameter Store

## 🚀 Project Status & Progress

- [x] **Project Setup:** Dockerized environment and folder structure.
- [x] **Data Simulation:** Event generator for ad clicks.
- [x] **Machine Learning Baseline:** 
  - [x] Point-in-time feature engineering (preventing data leakage).
  - [x] Handling extreme class imbalance (0.2% fraud rate) using `scale_pos_weight`.
  - [x] Chronological train/val/test splits to simulate real-world concept drift.
  - [x] XGBoost model training and threshold optimization.
- [ ] **Streaming Pipeline:** Kafka producers and Spark Structured Streaming consumers.
- [ ] **Data Lake:** S3 integration and Delta/Iceberg table formatting.
- [ ] **API & Inference:** FastAPI endpoints for real-time scoring.
- [ ] **Analytics:** Athena/Glue integration for batch reporting.

## 🧠 Machine Learning Architecture

The ML module (`ml/`) focuses on detecting fraudulent ad clicks in a highly imbalanced dataset (~0.2% positive class). 

### Key Engineering Decisions:
1. **Point-in-Time Feature Engineering:** All historical counts and rolling windows are calculated strictly *before* the current event timestamp to prevent target leakage (time-travel).
2. **Behavioral "Burst" Detection:** Instead of just looking at lifetime clicks, the model uses short rolling windows (`1min`, `5min`, `10min`, `1h`) to detect sudden spikes in velocity, which is a strong signature for bot activity.
3. **Extreme Imbalance Handling:** Instead of oversampling (SMOTE), which creates fake discrete events, we use XGBoost's `scale_pos_weight` to mathematically penalize missed fraud cases without distorting the data distribution.
4. **Chronological Evaluation:** Data is split strictly by time (no random shuffling) to expose real-world **concept drift** (fraud patterns changing over time), ensuring the model is evaluated exactly as it will perform in production.

## Project Structure

```text
ad-fraud-platform/
├── docker-compose.yml
├── README.md
├── .gitignore
│
├── simulator/          # Python scripts to generate fake ad click events
├── kafka/              # Kafka configurations and topics
├── spark/              # PySpark streaming jobs and feature transformations
├── ml/                 # Machine Learning pipeline
│   ├── features.py     # Point-in-time & rolling window feature engineering
│   ├── train_model.py  # XGBoost training, tuning, and evaluation
│   └── models/         # Saved .json model artifacts and feature schemas
├── api/                # FastAPI application for real-time inference
├── aws/                # AWS CLI scripts and infrastructure as code
├── analytics/          # SQL queries for Athena/Glue batch analytics
├── tests/              # Unit and integration tests
└── docs/               # Architecture diagrams and documentation