# 🚀 Crypto Streaming Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Kafka-Streaming-black?logo=apachekafka">
  <img src="https://img.shields.io/badge/Spark-Structured%20Streaming-orange?logo=apachespark">
  <img src="https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql">
  <img src="https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit">
  <img src="https://img.shields.io/badge/Docker-Compose-blue?logo=docker">
</p>

<p align="center">
  <b>Real-time crypto market data pipeline with alerting and dashboard</b>
</p>

---

## 🧠 Overview

This project builds a **real-time cryptocurrency data pipeline** using modern data engineering tools.

It ingests live crypto prices from the **CoinGecko API**, streams them through Kafka, processes them with Spark Structured Streaming, and stores results in PostgreSQL.

👉 A dashboard (Streamlit) allows real-time monitoring and alerting.

---

## 🏗️ Architecture

```mermaid
flowchart LR

    A[CoinGecko API] --> B[Kafka]

    B --> C[Spark Structured Streaming]

    C --> D[PostgreSQL]

    C --> E[Alerting Engine]

    D --> F[Streamlit Dashboard]

    E --> F

