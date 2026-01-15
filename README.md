# Real-Time Stock Market Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-black?style=for-the-badge&logo=apachekafka)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake)

## � Overview
This repository hosts a scalable, robust **Real-Time Data Engineering Pipeline** designed to process financial market data. The system ingests live stock quotes, streams them through a high-throughput event bus, and persists them into a data lake and data warehouse for advanced analytics.

The architecture follows modern data engineering practices, utilizing **Docker** for containerization and **Apache Kafka** for fault-tolerant streaming.

## 🏗️ Architecture
The pipeline consists of the following stages:

1.  **Ingestion**: A Python-based producer fetches real-time data from the **Finnhub API**.
2.  **Streaming**: Data is published to an **Apache Kafka** topic (`stock-quotes`).
3.  **Storage (Data Lake)**: A consumer service subscribes to the topic and dumps raw JSON records into **MinIO** (S3-compatible object storage).
4.  **Warehousing**: Data is loaded into **Snowflake** for structured querying.
5.  **Transformation**: **DBT** (Data Build Tool) models transform raw data into analytics-ready tables (Bronze/Silver/Gold).
6.  **Orchestration**: **Apache Airflow** manages the workflow schedules and dependencies.
7.  **Visualization**: **Power BI** connects to the Gold layer for dashboarding.

## �️ Technology Stack
*   **Language**: Python
*   **Streaming**: Apache Kafka & Zookeeper
*   **Object Storage**: MinIO
*   **Data Warehouse**: Snowflake
*   **Orchestration**: Apache Airflow
*   **Transformation**: dbt (data build tool)
*   **Infrastructure**: Docker & Docker Compose

## 🚀 Quick Start
### Prerequisites
*   Docker & Docker Compose installed.
*   API Key from [Finnhub](https://finnhub.io/).

### Installation
1.  **Clone the repository**:
    ```bash
    git clone https://github.com/SangharshWalde/Real-time-stock-Market-Data-Pipeline.git
    cd Real-time-stock-Market-Data-Pipeline
    ```

2.  **Environment Setup**:
    Create a `.env` file with your credentials:
    ```env
    API_KEY_HONNUB=<your_finnhub_key>
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    ```

3.  **Run Services**:
    ```bash
    docker-compose up -d
    ```

4.  **Start Pipeline**:
    ```bash
    # Start the Producer
    python source/producer.py

    # Start the Consumer
    python consumer/consumer.py
    ```

## 📂 Project Structure
*   `source/`: Contains the data ingestion scripts (Producers).
*   `consumer/`: Scripts for consuming Kafka messages and writing to storage.
*   `docker/`: Configuration for Airflow and other container services.
*   `dbt_stocks/`: SQL models and dbt configuration.
*   `dag/`: Airflow DAG definitions.

## 📈 Future Enhancements
*   Integration with Prometheus/Grafana for pipeline monitoring.
*   Deployment scripts for Kubernetes (K8s).
*   Advanced anomaly detection on stock prices.

---
*Developed by Sangharsh Walde*
