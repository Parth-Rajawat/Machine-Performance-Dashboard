# Machine-Performance-Dashboard
# IoT Machine Telemetry Pipeline (Kafka + MySQL + Grafana)

A real-time data engineering project simulating IoT machine telemetry.  
It ingests synthetic sensor data into **Kafka**, consumes it via a Python service, stores both latest & historical states in **MySQL**, and visualizes KPIs & alerts in **Grafana** dashboard.

---
## 📌 Architecture
<img width="1297" height="389" alt="Architecture" src="https://github.com/user-attachments/assets/4d6306be-4b59-466c-a333-900b0d956409" />



**Flow:**
1. **Synthetic Data Generator (Producer)** → Publishes JSON telemetry (temperature, RPM, vibration, perf_score) for 50+ machines into Kafka (`machine_telemetry` topic).
2. **Kafka Broker** → Manages topic partitions (keyed by `machine_id`).
3. **Kafka Consumer (Python)** → Consumes messages and writes:
   - `machine_latest`: Current snapshot of each machine
   - `machine_history`: Full telemetry history
   - `iot_alerts`: Threshold-based alerts
4. **Dashboard (Grafana)** → Live monitoring with KPIs, machine drilldowns, and alert tables.

---

## 🚀 Features

- **Real-time streaming** with Kafka producer & consumer
- **50+ machines, 5s frequency → ~36,000+ events/hour**
- **Threshold-based alerts** (temp > 85°C, vibration > 1.5g, perf < 0.5)
- **MySQL schema** optimized for both snapshot and historical queries
- **Grafana dashboard** for industry-standard monitoring (sub-5s latency)
- **.env config** for portability (Kafka, MySQL, refresh rate, thresholds)

---

## 🛠️ Tech Stack

- **Streaming**: Apache Kafka  
- **Ingestion**: Python (kafka-python)  
- **Storage**: MySQL  
- **Dashboards**: Grafana
- **Config**: python-dotenv  


