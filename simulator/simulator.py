"""Smart meter data simulator.

This module generates synthetic smart meter measurements and writes them to CSV
files that can be shipped into HDFS. The script is designed to run inside a
Docker container so that files can be written to a shared volume that is later
watched by Spark Structured Streaming.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import requests


@dataclass
class MeterReading:
    """Represents a single measurement from an individual smart meter."""

    meter_id: str
    timestamp: str
    voltage: float
    current: float
    active_power: float
    reactive_power: float
    frequency: float
    power_factor: float
    total_energy_kwh: float

    @classmethod
    def from_measurement(
        cls, meter_id: str, epoch_seconds: float, energy_counter: float
    ) -> "MeterReading":
        """Generate a reading with realistic looking values."""

        timestamp = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()

        # Typical residential voltage and current variations.
        voltage = random.normalvariate(230.0, 3.5)
        current = max(random.normalvariate(5.0, 1.5), 0.0)

        # Basic electrical relations.
        active_power = voltage * current * random.uniform(0.85, 0.98)
        reactive_power = active_power * random.uniform(0.05, 0.2)
        frequency = random.normalvariate(50.0, 0.05)
        power_factor = min(max(active_power / (voltage * current + 1e-6), 0.0), 1.0)
        total_energy_kwh = max(energy_counter + active_power / 3600000.0, 0.0)

        return cls(
            meter_id=meter_id,
            timestamp=timestamp,
            voltage=round(voltage, 3),
            current=round(current, 3),
            active_power=round(active_power, 3),
            reactive_power=round(reactive_power, 3),
            frequency=round(frequency, 3),
            power_factor=round(power_factor, 3),
            total_energy_kwh=round(total_energy_kwh, 6),
        )


@dataclass
class SimulationConfig:
    """Parameters that control how data is generated."""

    meters: int
    batch_size: int
    interval: float
    output_dir: Path
    metadata_file: Path
    hdfs_url: Optional[str]
    hdfs_directory: Optional[str]

    def ensure_paths(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)


def parse_args(argv: List[str] | None = None) -> SimulationConfig:
    parser = argparse.ArgumentParser(description="Generate smart meter CSV data")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/data/raw"),
        help="Directory where CSV batches are written. Defaults to /data/raw",
    )
    parser.add_argument(
        "--metadata-file",
        type=Path,
        default=Path("/data/state/simulator_metadata.json"),
        help="Path used to persist cumulative energy between runs",
    )
    parser.add_argument(
        "--meters",
        type=int,
        default=50,
        help="Number of unique smart meters to simulate",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=120,
        help="Number of events produced per batch (per file)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds to wait between batch generation",
    )
    parser.add_argument(
        "--hdfs-url",
        help=(
            "Base WebHDFS URL including protocol and port (e.g. "
            "'http://namenode:9870'). If provided the generated file is also "
            "uploaded to HDFS."
        ),
    )
    parser.add_argument(
        "--hdfs-directory",
        default="/data/raw/smartmeter",
        help="Target directory in HDFS when --hdfs-url is specified",
    )

    args = parser.parse_args(argv)
    metadata_file = args.metadata_file
    return SimulationConfig(
        meters=args.meters,
        batch_size=args.batch_size,
        interval=args.interval,
        output_dir=args.output_dir,
        metadata_file=metadata_file,
        hdfs_url=args.hdfs_url,
        hdfs_directory=args.hdfs_directory,
    )


def load_energy_state(metadata_file: Path, meters: int) -> List[float]:
    if metadata_file.exists():
        try:
            state = json.loads(metadata_file.read_text())
            if len(state) == meters:
                return state
        except json.JSONDecodeError:
            pass
    return [0.0 for _ in range(meters)]


def persist_energy_state(metadata_file: Path, state: Iterable[float]) -> None:
    metadata_file.write_text(json.dumps(list(state), indent=2))


def build_meter_ids(meters: int) -> List[str]:
    return [f"MTR-{i:05d}" for i in range(1, meters + 1)]


def upload_to_hdfs(local_file: Path, config: SimulationConfig) -> None:
    if not config.hdfs_url:
        return

    target_path = f"{config.hdfs_directory.rstrip('/')}/{local_file.name}"
    create_url = (
        f"{config.hdfs_url.rstrip('/')}/webhdfs/v1{target_path}?op=CREATE&overwrite=true"
    )

    with local_file.open("rb") as fh:
        response = requests.put(create_url, allow_redirects=False)
        if response.status_code not in (201, 307):
            raise RuntimeError(
                f"Failed to initiate WebHDFS upload: {response.status_code} {response.text}"
            )

        upload_url = response.headers.get("Location") if response.status_code == 307 else create_url
        fh.seek(0)
        response = requests.put(upload_url, data=fh)
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to upload file to HDFS: {response.status_code} {response.text}"
            )

    print(f"Uploaded {local_file.name} to HDFS at {target_path}")


def write_batch(config: SimulationConfig, meter_ids: List[str], energy: List[float]) -> Path:
    epoch = time.time()
    timestamp = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = config.output_dir / f"smartmeter_{timestamp}.csv"

    readings: List[MeterReading] = []
    for _ in range(config.batch_size):
        meter_index = random.randrange(len(meter_ids))
        meter_id = meter_ids[meter_index]
        reading = MeterReading.from_measurement(meter_id, epoch, energy[meter_index])
        energy[meter_index] = reading.total_energy_kwh
        readings.append(reading)

    with output_file.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(asdict(readings[0]).keys()))
        writer.writeheader()
        for reading in readings:
            writer.writerow(asdict(reading))

    print(f"Wrote {len(readings)} records to {output_file}")
    return output_file


def main(argv: List[str] | None = None) -> None:
    config = parse_args(argv)
    config.ensure_paths()

    meter_ids = build_meter_ids(config.meters)
    energy = load_energy_state(config.metadata_file, config.meters)

    try:
        while True:
            written_file = write_batch(config, meter_ids, energy)
            upload_to_hdfs(written_file, config)
            persist_energy_state(config.metadata_file, energy)
            time.sleep(config.interval)
    except KeyboardInterrupt:
        persist_energy_state(config.metadata_file, energy)
        print("Simulation interrupted. State persisted.")


if __name__ == "__main__":
    main()
