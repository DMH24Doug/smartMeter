CREATE DATABASE IF NOT EXISTS smart_energy;

DROP TABLE IF EXISTS smart_energy.smartmeter_aggregates;

CREATE EXTERNAL TABLE smart_energy.smartmeter_aggregates (
    meter_id STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_voltage DOUBLE,
    avg_current DOUBLE,
    avg_active_power DOUBLE,
    avg_power_factor DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data/processed/smartmeter_aggregates';
