# Transaction Fraud Pipeline

A real-time transaction monitoring pipeline built on **Docker Desktop + local Kubernetes** using the **PaySim** dataset.  
The project streams simulated banking transactions into **Kafka**, processes them with **Spark Structured Streaming**, writes curated outputs to **PostgreSQL**, and creates analytical SQL views for reporting and monitoring.

## Overview

This project simulates a production-style fraud monitoring pipeline:

- A Python producer reads transaction records from the PaySim dataset and publishes them to the Kafka topic `transactions`.
- Spark Structured Streaming consumes the Kafka stream, parses the payload, enriches the data, and classifies suspicious transactions.
- Processed records are written into PostgreSQL tables for downstream analysis.
- Kubernetes ConfigMap + Job are used to automatically create SQL tables, indexes, and views for analytics.

## Architecture

```text
PaySim CSV
   ↓
Python Producer
   ↓
Kafka topic: transactions
   ↓
Spark Structured Streaming
   ↓
PostgreSQL
   ├── bank_transactions
   ├── suspicious_transactions
   ├── transaction_summary
   ├── transaction_summary_hourly
   └── suspicious_transaction_summary_hourly
   
 