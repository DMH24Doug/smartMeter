# SmartMeter Big Data Pipeline

A complete real-time big data pipeline for Smart Meter IoT analytics using:

- **Hadoop HDFS** for distributed storage  
- **Hive (External Tables)** for schema-on-read  
- **PostgreSQL** Hive Metastore  
- **Trino** for fast SQL querying  
- **Grafana** for dashboards  
- **Python Simulator** for streaming smart meter readings  

This system ingests CSV readings → stores them in HDFS → exposes them via Hive → queries with Trino → visualizes in Grafana.

---

## 1. Architecture Overview


### Components

| Component | Purpose |
|----------|---------|
| HDFS NameNode | HDFS master |
| HDFS DataNode | HDFS worker |
| Hive Metastore (PostgreSQL) | Metadata & schema |
| HiveServer2 | Query engine for Hive |
| Trino | Fast SQL querying |
| Grafana | Visualization |
| Simulator | Generates streaming smart meter data |

---

## 2. Requirements

- Docker Desktop  
- Docker Compose  
- Python 3.10+ (optional for simulator)

---

## 3. Start the Full System

Run all services:

```bash
docker compose up -d
```
---

Verify running containers

```bash
docker ps
```
Expected Services

- hdfs-namenode
- hdfs-datanode
- hive-metastore-postgresql
- hive-metastore
- hive-server
- trino
- grafana
- simulator
