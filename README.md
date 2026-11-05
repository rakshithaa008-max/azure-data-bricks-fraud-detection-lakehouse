# Azure Databricks Fraud Detection Lakehouse

## Project Overview
A personal Azure Data Engineering portfolio project demonstrating transaction processing, data validation, fraud review flagging, Delta Lake tables, SQL analytics, and dashboard reporting using Azure Databricks.

## Technologies
- Microsoft Azure
- Azure Databricks
- PySpark
- Delta Lake
- Databricks SQL
- Databricks AI/BI Dashboards
- GitHub

## Architecture
Sample Transaction Data → Bronze Delta Table → Silver Delta Table → Gold Delta Tables → SQL KPIs → Monitoring Dashboard

## Implementation

**Bronze Layer:** Created sample transaction data using PySpark and stored it in a Delta table.

**Silver Layer:** Applied data quality checks, removed duplicate transaction IDs, and filtered invalid transaction records.

**Gold Layer:** Classified transactions above 1,000 as `HIGH_VALUE_REVIEW` and others as `NORMAL`. Created transaction-level, summary, and KPI Delta tables.

**Dashboard:** Built a Databricks dashboard to display transaction counts, flagged transactions, review rate, and fraud flag distribution.

## Sample Results

| Metric | Result |
|---|---|
| Total Transactions | 3 |
| Flagged Transactions | 1 |
| Review Rate | 33.33% |
| Normal Transactions | 2 |

## Repository Contents
- `01_eventhub_to_bronze.py` — Databricks notebook source covering the Bronze, Silver, Gold, and KPI transformations.
- `README.md` — Project documentation.

## Future Enhancements
- Integrate Azure Event Hubs for real-time transaction ingestion.
- Implement incremental Delta Lake processing.
- Add automated orchestration and monitoring.
- Expand data quality checks and test coverage.

## Note
This project uses demonstration transaction data. Fraud flags are based on a simple amount threshold and are intended for review, not proof of fraudulent activity.
