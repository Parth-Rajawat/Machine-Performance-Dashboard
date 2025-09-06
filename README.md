# Machine-Performance-Dashboard
# IoT Machine Telemetry Pipeline (Kafka + MySQL + Grafana)

A real-time data engineering project simulating IoT machine telemetry. It ingests synthetic sensor data into **Kafka**, consumes it via a Python service, stores both latest & historical states in **MySQL**, and visualizes KPIs & alerts in **Grafana** dashboard.

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

## ⚙️ Setup & Run

### 1️⃣ Prerequisites
- Python 3.8+
- MySQL running (with `iot_monitor` DB)
- Kafka broker running (on `localhost:9092`)

### 2️⃣ Install dependencies
```bash
pip install -r requirements.txt
```

### 3️⃣ Configure environment
Create a .env file:
```env
# Kafka
KAFKA_BOOTSTRAP=localhost:9092
TELEMETRY_TOPIC=machine_telemetry

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DB=iot_monitor

# Producer
NUM_MACHINES=50
TICK_SECONDS=5.0

# Consumer
CONSUMER_GROUP=iot-mysql-writer

# Streamlit
STREAMLIT_REFRESH_SEC=3

# Thresholds
THRESHOLD_TEMPERATURE=85.0
THRESHOLD_VIBRATION=1.5
THRESHOLD_PERF_SCORE=0.5
```

### 4️⃣ Initialize database
```
mysql -u root -p < database_setup.sql
```
