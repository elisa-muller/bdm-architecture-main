"""
kafka-producer — Synthetic server-metrics generator
====================================================
Runs continuously inside the `kafka-producer` container.

Every INTERVAL_SECONDS seconds, publishes a batch of BATCH_SIZE synthetic
server sensor readings to the Kafka topic `server-metrics-raw`.

About 5 % of records carry intentional anomalies (impossible sensor values,
NULL server IDs) so the downstream cleanup DAG always has something to find.

Schema (one JSON message per record):
  event_id         str    UUID4
  server_id        str    "srv-001" … "srv-020"  (1 % are None)
  rack_id          str    "rack-A" … "rack-D"
  datacenter       str    "DC1" | "DC2"
  timestamp        str    ISO 8601 UTC
  cpu_temp_c       float  normal [40, 85] | anomaly (<0 or >120)
  memory_util_pct  float  normal [20, 95] | anomaly (<0 or >100)
  power_draw_w     float  normal [150, 500] | anomaly (<0)
  fan_rpm          int    [800, 4200]
  is_alert         bool
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Configuration (can be overridden via env vars) ────────────────────────────
KAFKA_BROKERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC",             "server-metrics-raw")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE",           "20"))
INTERVAL_SECONDS  = float(os.getenv("INTERVAL_SECONDS",   "5"))

# ── Topology: 20 servers across 4 racks in 2 data centres ────────────────────
SERVERS     = [f"srv-{i:03d}" for i in range(1, 21)]
RACKS       = ["rack-A", "rack-B", "rack-C", "rack-D"]
DATACENTERS = ["DC1", "DC2"]


def make_record() -> dict:
    """Generate a single synthetic server sensor reading."""
    is_anomaly  = random.random() < 0.05   # 5 % chance of bad sensor data
    has_null_id = random.random() < 0.01   # 1 % orphaned reading

    server_id = None if has_null_id else random.choice(SERVERS)

    if is_anomaly:
        kind = random.choice(["hot", "cold", "mem_high", "mem_low", "power"])
        cpu_temp = round(
            random.uniform(121.0, 200.0) if kind == "hot"
            else random.uniform(-30.0, -0.1) if kind == "cold"
            else random.uniform(40.0, 85.0),
            2,
        )
        memory_util = round(
            random.uniform(101.0, 150.0) if kind == "mem_high"
            else random.uniform(-10.0, -0.1) if kind == "mem_low"
            else random.uniform(20.0, 95.0),
            2,
        )
        power_draw = round(
            random.uniform(-200.0, -1.0) if kind == "power"
            else random.uniform(150.0, 500.0),
            2,
        )
    else:
        cpu_temp    = round(random.uniform(40.0, 85.0),  2)
        memory_util = round(random.uniform(20.0, 95.0),  2)
        power_draw  = round(random.uniform(150.0, 500.0), 2)

    return {
        "event_id":        str(uuid.uuid4()),
        "server_id":       server_id,
        "rack_id":         random.choice(RACKS),
        "datacenter":      random.choice(DATACENTERS),
        "timestamp":       datetime.now(tz=timezone.utc).isoformat(),
        "cpu_temp_c":      cpu_temp,
        "memory_util_pct": memory_util,
        "power_draw_w":    power_draw,
        "fan_rpm":         random.randint(800, 4200),
        "is_alert":        bool(is_anomaly or has_null_id),
    }


def connect(retries: int = 20, delay: float = 3.0) -> KafkaProducer:
    """Connect to Kafka, retrying on startup race conditions."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
            )
            print(f"Connected to Kafka at {KAFKA_BROKERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt}/{retries}), retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts")


def main() -> None:
    producer = connect()
    batch_num = 0

    while True:
        batch_num += 1
        records = [make_record() for _ in range(BATCH_SIZE)]
        anomalies = sum(1 for r in records if r["is_alert"])

        for rec in records:
            producer.send(KAFKA_TOPIC, value=rec)
        producer.flush()

        print(
            f"Batch {batch_num:>4d} | published {BATCH_SIZE} records "
            f"({anomalies} anomalies) → topic '{KAFKA_TOPIC}'"
        )
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
