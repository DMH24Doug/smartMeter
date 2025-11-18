# SmartMeter Big Data Pipeline

A complete real-time big data pipeline for Smart Meter IoT analytics using:

- **Hadoop HDFS** for distributed storage  
- **Hive (External Tables)** for schema-on-read
- **Spark** Handles batch, ETL, and streaming jobs
- **PostgreSQL** Hive Metastore  
- **Trino** for fast SQL querying  
- **Grafana** for dashboards  
- **Python Simulator** for streaming smart meter readings  

The pipeline ingests CSV smart meter data, stores it in HDFS, exposes it through Hive, processes and aggregates it with Spark, queries it via Trino, and visualizes the results in Grafana.

---

## Architecture Overview


### Components

| Component | Purpose |
|----------|---------|
| HDFS NameNode | HDFS master |
| HDFS DataNode | HDFS worker |
| Hive Metastore (PostgreSQL) | Metadata & schema |
| HiveServer2 | Query engine for Hive |
| Spark Master | Distributes Spark jobs across workers |
| Spark Worker | Assigns Spark tasks for distributed computation |
| Spark Client | Runs spark-submit, PySpark, and streaming jobs |
| Trino | Fast SQL querying |
| Grafana | Visualization |
| Simulator | Generates streaming smart meter data |

---

## Requirements

- Docker Desktop  
- Docker Compose  
- Python 3.10+ (optional for simulator)

---

# SmartMeter Big Data Pipeline

## 1. Start the System
Change drirectory to smartmeter directory:
```
cd smartMeter
```
Run Docker file Build:
```
docker compose build simulator
```
Run the full pipeline:
```bash
docker compose up -d
```

The first startup downloads a number of large images (Hadoop, Spark, Trino, and
Grafana). When everything is healthy you should see the following web UIs:

- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- HiveServer2 Thrift port: `localhost:10000`
- Trino Web UI: http://localhost:8085
- Grafana: http://localhost:3001 (anonymous admin access enabled)
  
Verify running services:
```bash
docker ps
```

## 2. HDFS Setup (Create Directories)
Open the namenode container:
```bash
docker exec -it namenode bash
```

Create required directories:
```bash
hdfs dfs -mkdir -p /user/dr.who
hdfs dfs -mkdir -p /user/dr.who/smartmeter
hdfs dfs -mkdir -p /user/dr.who/smartmeter_agg
```

Check:
```bash
hdfs dfs -ls /user/dr.who
```

Give Spark and Simulator read & write access:
```bash
hdfs dfs -chmod -R 777 /user/dr.who
```
## 3. Hive Database and Tables
Open HiveServer:
```bash
docker exec -it hive-server bash
beeline -u 'jdbc:hive2://localhost:10000/' -n root
```
Create schema:
```bash
CREATE DATABASE IF NOT EXISTS smartgrid;
USE smartgrid;
```
Create raw table:

```bash
CREATE EXTERNAL TABLE smartmeter_raw (
 meter_id STRING,
 ts STRING,
 voltage DOUBLE,
 current_amp DOUBLE,
 active_pow DOUBLE,
 reactive_pow DOUBLE,
 frequency DOUBLE,
 power_fact DOUBLE,
 total_energy_kwh DOUBLE
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:8020/user/dr.who/smartmeter'
TBLPROPERTIES ("skip.header.line.count"="1");
```

Check:
```bash
SELECT * FROM smartmeter_raw LIMIT 20;
```
## 4. Aggregated Table (Spark Output)
After Spark writes Parquet files:
```bash
docker exec -it trino trino

CREATE TABLE hive.smartgrid.smartmeter_agg (
 meter_id VARCHAR,
 window_start TIMESTAMP,
 window_end TIMESTAMP,
 avg_voltage DOUBLE,
 avg_current DOUBLE,
 avg_active_power DOUBLE,
 avg_reactive_power DOUBLE,
 avg_power_factor DOUBLE
)
WITH (
 external_location='hdfs://namenode:8020/user/dr.who/smartmeter_agg',
 format='PARQUET'
);
```

## 5. Trino Querying
Connect:
```bash
docker exec -it trino trino
```
Useful commands:
```bash
SHOW CATALOGS;
SHOW SCHEMAS FROM hive;
SHOW TABLES FROM hive.smartgrid;

SELECT * FROM hive.smartgrid.smartmeter_raw LIMIT 20;
SELECT * FROM hive.smartgrid.smartmeter_agg LIMIT 20;

DROP TABLE IF EXISTS hive.smartgrid.smartmeter_agg;
```
## 6. Spark Job (Structured Streaming)

Run:
```
docker exec -it spark-client /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 /opt/spark/jobs/streaming_job.py hdfs://namenode:8020/user/dr.who/smartmeter hdfs://namenode:8020/user/dr.who/smartmeter_agg
```

## 7. Grafana Setup
Default URL:
- http://localhost:3001

### Queries for Grafana Panels

**Average Voltage**
```
SELECT window_start AS time, meter_id, avg_voltage
FROM hive.smartgrid.smartmeter_agg
ORDER BY time, meter_id;
```

**Average Power Factor**
```
SELECT window_start AS time, meter_id, avg_power_factor
FROM hive.smartgrid.smartmeter_agg
ORDER BY time, meter_id;
```

**Average Current**
```
SELECT window_start AS time, meter_id, avg_current
FROM hive.smartgrid.smartmeter_agg
ORDER BY time, meter_id;
```

**Average Active Power**
```
SELECT window_start AS time, meter_id, avg_active_power
FROM hive.smartgrid.smartmeter_agg
ORDER BY time, meter_id;
```

## 8. Simulator Restart

Run:
```bash
docker restart simulator
```
Watch logs:
```bash
docker logs -f simulator
```
