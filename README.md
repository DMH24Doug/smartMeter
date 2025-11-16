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

