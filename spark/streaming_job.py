"""Spark Structured Streaming job for smart meter data."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("meter_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("voltage", DoubleType(), False),
        StructField("current", DoubleType(), False),
        StructField("active_power", DoubleType(), False),
        StructField("reactive_power", DoubleType(), False),
        StructField("frequency", DoubleType(), False),
        StructField("power_factor", DoubleType(), False),
        StructField("total_energy_kwh", DoubleType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Structured streaming to parquet")
    parser.add_argument("input_path", help="HDFS path that receives simulator CSV files")
    parser.add_argument("output_path", help="HDFS path where parquet data is written")
    parser.add_argument(
        "--checkpoint",
        default="/tmp/spark-checkpoints/smartmeter",
        help="HDFS checkpoint location for streaming state",
    )
    parser.add_argument(
        "--trigger-interval",
        default="30 seconds",
        help="Trigger interval for micro-batches (e.g. '30 seconds')",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("SmartMeterStreaming")
        .enableHiveSupport()
        .getOrCreate()
    )

    raw_stream = (
        spark.readStream.option("header", True)
        .schema(SCHEMA)
        .csv(args.input_path)
    )

    # Convert the ISO timestamp into a proper timestamp column for aggregations.
    typed_stream = raw_stream.withColumn("event_time", col("timestamp").cast("timestamp"))

    # Produce windowed aggregates that are easier to visualise in Grafana.
    aggregates = (
        typed_stream.withWatermark("event_time", "5 minutes")
        .groupBy(window(col("event_time"), "5 minutes"), col("meter_id"))
        .agg(
            avg("voltage").alias("avg_voltage"),
            avg("current").alias("avg_current"),
            avg("active_power").alias("avg_active_power"),
            avg("power_factor").alias("avg_power_factor"),
        )
        .select(
            col("meter_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_voltage",
            "avg_current",
            "avg_active_power",
            "avg_power_factor",
        )
    )

    query = (
        aggregates.writeStream.outputMode("append")
        .format("parquet")
        .option("path", args.output_path)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
