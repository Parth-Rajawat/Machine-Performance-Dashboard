CREATE DATABASE IF NOT EXISTS iot_monitor;
USE iot_monitor;

CREATE TABLE IF NOT EXISTS machine_latest (
  machine_id VARCHAR(64) PRIMARY KEY,
  location VARCHAR(128),
  temperature DOUBLE,
  rpm DOUBLE,
  vibration DOUBLE,
  perf_score DOUBLE,
  ts DATETIME NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS machine_history (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  machine_id VARCHAR(64),
  location VARCHAR(128),
  temperature DOUBLE,
  rpm DOUBLE,
  vibration DOUBLE,
  perf_score DOUBLE,
  ts DATETIME,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS iot_alerts (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  machine_id VARCHAR(64),
  alert_type VARCHAR(64),
  alert_desc TEXT,
  metric_val DOUBLE,
  ts DATETIME DEFAULT CURRENT_TIMESTAMP
);
