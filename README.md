# 🚨 Real-Time Banking Fraud Detection Platform

A real-time data engineering platform for detecting suspicious and fraudulent banking transactions using **Python, Apache Kafka, PySpark, PostgreSQL, Apache Airflow, Streamlit, Docker, and Git**.

The platform continuously generates banking transactions, streams them through Kafka, processes them using PySpark, applies fraud detection rules, stores the results in PostgreSQL, orchestrates pipeline validation with Airflow, and visualizes fraud analytics through a Streamlit dashboard.

---

## 🚀 Project Overview

The **Real-Time Banking Fraud Detection Platform** is an end-to-end data engineering project designed to simulate a real-time banking transaction monitoring system.

The system processes banking transactions through a streaming architecture:

```text
Transaction Generator
        ↓
      Kafka
        ↓
PySpark Streaming
        ↓
 Fraud Detection Rules
        ↓
   PostgreSQL
        ↓
 Streamlit Dashboard
```
Apache Airflow is used to orchestrate and validate the banking fraud pipeline.

```text

                    ┌──────────────────────┐
                    │ Transaction Generator│
                    │       Python         │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │        Kafka         │
                    │  bank-transactions   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    PySpark Stream    │
                    │ Fraud Detection Rules│
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │     PostgreSQL       │
                    │ Transactions/Alerts  │
                    └──────────┬───────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
          ┌─────────────────┐    ┌─────────────────┐
          │ Streamlit       │    │ Apache Airflow  │
          │ Dashboard       │    │ Orchestration   │
          └─────────────────┘    └─────────────────┘
```
