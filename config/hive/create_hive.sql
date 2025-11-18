-- Drop first so the script can be re-run safely
DROP TABLE IF EXISTS hive.smartgrid.smartmeter_raw;
DROP TABLE IF EXISTS hive.smartgrid.smartmeter_agg;

-- =====================================================
-- RAW TABLE
-- =====================================================
CREATE TABLE hive.smartgrid.smartmeter_raw (
    meter_id        VARCHAR,
    timestamp       VARCHAR,
    voltage         DOUBLE,
    current         DOUBLE,
    active_power    DOUBLE,
    reactive_power  DOUBLE,
    frequency       DOUBLE,
    power_factor    DOUBLE,
    total_energy_kwh DOUBLE
)
WITH (
    external_location = 'hdfs://namenode:8020/user/dr.who/smartmeter',
    format            = 'CSV',
    skip_header_line_count = 1
);

-- =====================================================
-- AGGREGATED TABLE
-- =====================================================
CREATE TABLE hive.smartgrid.smartmeter_agg (
    meter_id          VARCHAR,
    window_start      TIMESTAMP,
    window_end        TIMESTAMP,
    avg_voltage       DOUBLE,
    avg_current       DOUBLE,
    avg_active_power  DOUBLE,
    avg_reactive_power DOUBLE,
    avg_power_factor  DOUBLE
)
WITH (
    external_location = 'hdfs://namenode:8020/user/dr.who/smartmeter_agg',
    format            = 'PARQUET'
);
