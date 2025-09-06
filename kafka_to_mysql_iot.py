#!/usr/bin/env python3
"""
kafka_to_mysql_iot.py

Kafka consumer that:
  - consumes telemetry from the TELEM_TOPIC (machine_telemetry)
  - upserts latest state to `machine_latest`
  - appends every reading to `machine_history`
  - runs simple rule-based anomaly detection and inserts alerts into `iot_alerts` (MySQL only)

Configuration is read from environment variables (.env via python-dotenv). Example keys:
  KAFKA_BOOTSTRAP, TELEMETRY_TOPIC, CONSUMER_GROUP,
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB,
  THRESHOLD_TEMPERATURE, THRESHOLD_VIBRATION, THRESHOLD_PERF_SCORE

Notes:
  - This version intentionally does not produce alerts to any Kafka topic (alerts are stored in MySQL only).
  - For higher throughput, consider batching DB writes or using Kafka Connect (JDBC sink).
"""

import os
import json
import time
import signal
from datetime import datetime
from typing import Optional, Tuple

from dotenv import load_dotenv
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error as MySQLError

# Load configuration from .env
load_dotenv()

# ----------------------------
# Config (from environment / .env)
# ----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TELE_TOPIC = os.getenv("TELEMETRY_TOPIC", "machine_telemetry")
GROUP_ID = os.getenv("CONSUMER_GROUP", "iot-mysql-writer")

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "your_user"),
    "password": os.getenv("MYSQL_PASSWORD", "your_password"),
    "database": os.getenv("MYSQL_DB", "iot_monitor"),
    # autocommit is disabled; we explicitly commit/rollback
    "autocommit": False,
}

# Thresholds for simple anomaly detection - can be overridden via env
THRESHOLDS = {
    "temperature": float(os.getenv("THRESHOLD_TEMPERATURE", "85.0")),
    "vibration": float(os.getenv("THRESHOLD_VIBRATION", "1.5")),
    "perf_score": float(os.getenv("THRESHOLD_PERF_SCORE", "0.5")),
}

# Consumer loop control
POLL_TIMEOUT_MS = 1000  # how long the consumer.poll blocks (ms)
SHUTDOWN = False


# ----------------------------
# SQL statements
# ----------------------------
UPSERT_SQL = """
INSERT INTO machine_latest
(machine_id, location, temperature, rpm, vibration, perf_score, ts)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  location=VALUES(location),
  temperature=VALUES(temperature),
  rpm=VALUES(rpm),
  vibration=VALUES(vibration),
  perf_score=VALUES(perf_score),
  ts=VALUES(ts)
"""

HIST_SQL = """
INSERT INTO machine_history
(machine_id, location, temperature, rpm, vibration, perf_score, ts)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""

ALERT_INSERT_SQL = """
INSERT INTO iot_alerts (machine_id, alert_type, alert_desc, metric_val, ts)
VALUES (%s,%s,%s,%s,%s)
"""


# ----------------------------
# Helper functions
# ----------------------------
def parse_iso(ts_str: Optional[str]) -> datetime:
    """
    Parse ISO8601 timestamp string from the producer into a naive UTC datetime.
    Falls back to current UTC time on error.
    """
    if not ts_str:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        # fallback to now if parsing fails
        return datetime.utcnow()


def connect_mysql_with_retry(retries: int = 5, delay: float = 2.0):
    """
    Attempt to connect to MySQL with a retry loop. Returns a live connection object.
    Raises RuntimeError if unable to connect after retries.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            print(f"[mysql] Connected to {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
            return conn
        except MySQLError as e:
            last_err = e
            print(f"[mysql] Connection attempt {attempt}/{retries} failed: {e}")
            time.sleep(delay)
    raise RuntimeError(f"Failed to connect to MySQL after {retries} attempts. Last error: {last_err}")


def handle_alert_inserts(cursor, machine_id: str, alerts: Tuple[Tuple[str, str, float], ...], ts: datetime):
    """
    Insert generated alerts into iot_alerts table using provided DB cursor.
    'alerts' is a list/tuple of (alert_type, description, metric_value).
    """
    for alert_type, desc, metric_val in alerts:
        cursor.execute(ALERT_INSERT_SQL, (machine_id, alert_type, desc, metric_val, ts))


def detect_anomalies(ev: dict) -> Tuple[Tuple[Tuple[str, str, float], ...], bool]:
    """
    Rule-based anomaly detection for a single telemetry event.
    Returns a tuple (alerts, has_alerts_flag).
    Each alert is a tuple: (alert_type, description, metric_value)
    """
    alerts = []
    temp = ev.get("temperature")
    vib = ev.get("vibration")
    perf = ev.get("perf_score")

    if temp is not None and temp > THRESHOLDS["temperature"]:
        alerts.append(("temperature_high", f"Temperature {temp}C > {THRESHOLDS['temperature']}", temp))
    if vib is not None and vib > THRESHOLDS["vibration"]:
        alerts.append(("vibration_high", f"Vibration {vib}g > {THRESHOLDS['vibration']}", vib))
    if perf is not None and perf < THRESHOLDS["perf_score"]:
        alerts.append(("perf_degraded", f"Performance {perf} < {THRESHOLDS['perf_score']}", perf))

    return tuple(alerts), bool(alerts)


# ----------------------------
# Signal handling for graceful shutdown
# ----------------------------
def _signal_handler(signum, frame):
    global SHUTDOWN
    print(f"[signal] Received signal {signum}; initiating shutdown...")
    SHUTDOWN = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ----------------------------
# Main consumer loop
# ----------------------------
def main():
    # Connect to MySQL
    conn = connect_mysql_with_retry()
    cursor = conn.cursor()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        TELE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )
    print(f"[kafka] Consumer started on topic '{TELE_TOPIC}' (group: {GROUP_ID})")

    try:
        # Main poll loop
        while not SHUTDOWN:
            # consumer is iterable; this inner loop yields messages available now and returns when none for the timeout
            for msg in consumer:
                # If shutdown requested, break quickly
                if SHUTDOWN:
                    break

                ev = msg.value  # already deserialized dict
                # Safe extraction with defaults
                machine_id = ev.get("machine_id")
                location = ev.get("location")
                temperature = ev.get("temperature")
                rpm = ev.get("rpm")
                vibration = ev.get("vibration")
                perf_score = ev.get("perf_score")
                ts = parse_iso(ev.get("ts"))

                try:
                    # Upsert latest snapshot
                    cursor.execute(
                        UPSERT_SQL,
                        (machine_id, location, temperature, rpm, vibration, perf_score, ts),
                    )
                    # Append to history
                    cursor.execute(
                        HIST_SQL,
                        (machine_id, location, temperature, rpm, vibration, perf_score, ts),
                    )

                    # Anomaly detection -> insert alerts into DB only
                    alerts, has_alerts = detect_anomalies(ev)
                    if has_alerts:
                        handle_alert_inserts(cursor, machine_id, alerts, ts)
                        print(f"[alert] Detected {len(alerts)} alert(s) for {machine_id} @ {ts.isoformat()}")

                    # Commit per-message (simple). For higher throughput batch commits.
                    conn.commit()

                except Exception as db_exc:
                    # Rollback on any DB write error to avoid partial state
                    conn.rollback()
                    print(f"[db] Error writing to DB for machine {machine_id}: {db_exc}")

            # small sleep to avoid busy-loop spinning; consumer.timeout allows outer loop to continue
            time.sleep(0.1)

    except Exception as exc:
        print(f"[error] Unexpected exception in consumer loop: {exc}")
    finally:
        print("[shutdown] Closing consumer and DB connection...")
        try:
            consumer.close()
        except Exception:
            pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        print("[shutdown] Done.")


if __name__ == "__main__":
    main()
