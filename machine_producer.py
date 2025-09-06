#!/usr/bin/env python3
"""
machine_producer.py

Simulate NUM_MACHINES industrial machines and publish telemetry events to Kafka every TICK_SECONDS.

Configuration is loaded from environment variables (see .env.example) via python-dotenv.

Usage:
    cp .env.example .env        # edit .env
    source machine-env/bin/activate
    pip install -r requirements.txt
    python machine_producer.py
"""
from dotenv import load_dotenv
import os
import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# Load .env into environment
load_dotenv()

# ----------------------------
# Configuration (from .env)
# ----------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TELEMETRY_TOPIC", "machine_telemetry")
NUM_MACHINES = int(os.getenv("NUM_MACHINES", "50"))
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "5.0"))

# Example floor locations (assign machines to lines/rows)
FLOORS = [f"line_{i}" for i in range(1, 6)]  # line_1 ... line_5

# ----------------------------
# Kafka producer setup
# ----------------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def initialize_machines(num: int):
    """
    Initialize the machine list with baseline metrics.

    Returns a list of dicts representing machine state.
    """
    machines = []
    for i in range(1, num + 1):
        machines.append({
            "machine_id": f"mach_{i:03d}",
            "location": random.choice(FLOORS),
            "temperature": random.uniform(40, 65),   # degrees Celsius
            "rpm": random.uniform(800, 1200),        # motor rpm
            "vibration": random.uniform(0.1, 0.6),   # g
            "perf_score": random.uniform(0.7, 1.0)   # 0..1
        })
    return machines


def step_metrics(machine: dict):
    """
    Apply a bounded random walk to machine metrics (mutates in place).
    """
    machine["temperature"] += random.uniform(-0.5, 0.8)
    machine["temperature"] = max(20.0, min(120.0, machine["temperature"]))

    machine["rpm"] += random.uniform(-10, 10)
    machine["rpm"] = max(100.0, min(5000.0, machine["rpm"]))

    machine["vibration"] += random.uniform(-0.05, 0.05)
    machine["vibration"] = max(0.0, min(5.0, machine["vibration"]))

    machine["perf_score"] += random.uniform(-0.02, 0.02)
    machine["perf_score"] = max(0.0, min(1.0, machine["perf_score"]))


def build_event(machine: dict) -> dict:
    """
    Build a JSON-serializable telemetry event for given machine.
    """
    event = {
        "machine_id": machine["machine_id"],
        "location": machine["location"],
        "temperature": round(machine["temperature"], 2),
        "rpm": round(machine["rpm"], 1),
        "vibration": round(machine["vibration"], 3),
        "perf_score": round(machine["perf_score"], 3),
        "ts": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    }
    return event


def run():
    """Main loop: update metrics and publish one message per machine each tick."""
    machines = initialize_machines(NUM_MACHINES)
    print(f"Producer starting: {NUM_MACHINES} machines -> topic='{TOPIC}' tick={TICK_SECONDS}s")
    try:
        while True:
            start = time.time()
            for m in machines:
                step_metrics(m)
                ev = build_event(m)
                # key ensures per-machine ordering if topic is partitioned
                producer.send(TOPIC, key=ev["machine_id"].encode("utf-8"), value=ev)
            producer.flush()
            print(f"Published telemetry for {NUM_MACHINES} machines at {datetime.utcnow().isoformat()}")
            elapsed = time.time() - start
            time.sleep(max(0, TICK_SECONDS - elapsed))
    except KeyboardInterrupt:
        print("Producer stopping (keyboard interrupt)")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run()
