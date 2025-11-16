# Smart Energy Streaming Stack

This repository packages a complete smart meter analytics playground built on
Docker. The simulated data flows through the following stages:

```
Simulator (Python) -> HDFS -> Spark Structured Streaming -> Parquet -> Hive -> Trino -> Grafana
```

## Repository Layout

- `docker-compose.yml` – container orchestration for Hadoop, Spark, Hive, Trino,
  Grafana, and the simulator.
- `simulator/` – Python smart meter data generator with optional WebHDFS upload.
- `spark/streaming_job.py` – PySpark Structured Streaming job that converts raw
  CSV batches into aggregated Parquet data.
- `config/hadoop/` – shared Hadoop configuration used by HDFS, Spark, Hive, and
  Trino containers.
- `config/hive/` – SQL helper to create the Hive external table on top of the
  Parquet output.
- `config/trino/catalog/` – Trino connector configuration for the Hive catalog.
- `grafana/datasources/` – Grafana provisioning so Trino is available as a data
  source on first launch.

## Prerequisites

- Docker Desktop (tested with WSL 2 backend)
- Docker Compose V2 (`docker compose` command)

## Bring the stack online

```powershell
cd smartEnergy_
docker compose build simulator
docker compose up -d
```

The first startup downloads a number of large images (Hadoop, Spark, Trino, and
Grafana). When everything is healthy you should see the following web UIs:

- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- HiveServer2 Thrift port: `localhost:10000`
- Trino Web UI: http://localhost:8085
- Grafana: http://localhost:3001 (anonymous admin access enabled)

## Smart meter simulator

The simulator container writes CSV batches to the shared `./data/raw` folder and
(optionally) uploads each file to HDFS via WebHDFS. Command line help is
available with:

```bash
docker compose run --rm simulator --help
```

You can change parameters such as batch size or the number of meters by editing
`docker-compose.yml` or overriding values at runtime, e.g.:

```bash
docker compose run --rm simulator \
  --meters 10 \
  --batch-size 60 \
  --interval 2 \
  --hdfs-url http://namenode:9870 \
  --hdfs-directory /data/raw/smartmeter
```

## Run the Spark streaming job

1. Open an interactive shell inside the Spark client container:
   ```bash
   docker compose exec spark-client /bin/bash
   ```
2. Submit the Structured Streaming job:
   ```bash
   /opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --deploy-mode client \
     /opt/spark/jobs/streaming_job.py \
     hdfs://namenode:8020/data/raw/smartmeter \
     hdfs://namenode:8020/data/processed/smartmeter_aggregates \
     --checkpoint hdfs://namenode:8020/data/checkpoints/smartmeter
   ```

The job watches for new CSV files landing in HDFS and writes five-minute window
aggregates (per meter) to Parquet.

## Prepare Hive and query with Trino

Use HiveServer2 (or Beeline) once the Spark job has produced Parquet files:

```bash
docker compose exec hive-server /opt/hive/bin/beeline \
  -u jdbc:hive2://localhost:10000/ \
  -n hive -p hive \
  -f /opt/hive/conf/init/create_hive_tables.sql
```

The script creates the `smart_energy.smartmeter_aggregates` external table
pointing at `hdfs://namenode:8020/data/processed/smartmeter_aggregates`.

Trino automatically exposes the Hive catalog; verify with:

```bash
docker compose exec trino trino --execute "SHOW TABLES FROM hive.smart_energy"
```

## Visualise in Grafana

Grafana comes pre-provisioned with a Trino data source. Create a new dashboard
and use the `smartmeter_aggregates` table to build panels such as:

```sql
SELECT
  window_start,
  meter_id,
  avg_active_power
FROM hive.smart_energy.smartmeter_aggregates
ORDER BY window_start DESC
LIMIT 100
```

## Tear down

```bash
docker compose down
```

Add `--volumes` to remove persistent metastore, Grafana, and HDFS data.
