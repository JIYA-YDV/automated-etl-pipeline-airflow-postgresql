# Automated ETL Pipeline — Weather Data | Airflow + PostgreSQL

Python 3.10 | Apache Airflow 2.8.1 | PostgreSQL | pytest
![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-blue)
![Tests](https://img.shields.io/badge/Tests-27%20passing-brightgreen)

## Overview
Production-style ETL pipeline that ingests **live weather data** for 5 global cities
from OpenWeatherMap API every hour, validates data quality, transforms and loads
into PostgreSQL — fully orchestrated and monitored with Apache Airflow.

## Architecture
```
OpenWeatherMap API
        ↓
   [EXTRACT]  → fetch_weather_for_city() × 5 cities
        ↓
   [VALIDATE] → 5 quality checks per record (25 total)
        ↓
   [TRANSFORM]→ Kelvin→Celsius, meters→km, weather categorization
        ↓
   [LOAD]     → PostgreSQL (raw_weather + transformed_weather tables)
        ↓
   [MONITOR]  → Apache Airflow UI — schedule, logs, alerts
```

## Tech Stack
| Tool | Purpose |
|------|---------|
| Python 3.10 | Core pipeline logic |
| Apache Airflow 2.8.1 | Orchestration + scheduling |
| PostgreSQL 18.1 | Data storage |
| WSL2 + Ubuntu 22.04 | Linux environment on Windows |
| OpenWeatherMap API | Live weather data source |
| pytest | Unit testing |

## Cities Monitored
Kathmandu 🇳🇵 | Delhi 🇮🇳 | London 🇬🇧 | New York 🇺🇸 | Tokyo 🇯🇵

## Pipeline Features
- Hourly automated runs via Airflow scheduler
- 25 data quality checks per pipeline run
- Raw and transformed data stored separately
- Full pipeline logging in PostgreSQL
- Error handling with automatic retries (2 retries, 5min delay)
- Unit tested with pytest (27 tests, 100% passing)

## Project Structure
```
automated-etl-pipeline-airflow-postgresql/
├── dags/
│   └── weather_etl_dag.py        # Airflow DAG - pipeline orchestration
├── src/
│   ├── extract.py                # OpenWeatherMap API ingestion
│   ├── transform.py              # Data cleaning + transformation
│   ├── quality_checks.py         # Data validation functions
│   └── load.py                   # PostgreSQL insertion logic
├── sql/
│   ├── create_tables.sql         # Database schema
│   └── queries.sql               # Sample analytical queries
├── tests/
│   ├── test_transform.py         # Transform unit tests
│   └── test_quality_checks.py    # Quality check unit tests
├── config/
│   └── config.py                 # Configuration management
└── .env.example
```

## Data Quality Checks
Every record is validated for:
- No null values in critical fields
- Temperature within realistic range (-90°C to 60°C)
- Humidity between 0-100%
- Wind speed non-negative and realistic
- City name not empty

## Database Schema
```sql
raw_weather          -- Original API response data
transformed_weather  -- Cleaned, converted data
quality_logs         -- Per-field validation results
pipeline_logs        -- Run history and status
```
## Sample Analytics
With the data loaded into PostgreSQL, we can answer business questions such as:
- What is the average temperature trend per city over the last 24 hours?
- Which city experienced the highest humidity fluctuations?
- Identification of "Extreme Weather" events based on custom thresholds.
  
## Screenshots
### Airflow DAG — All Tasks Successful
<img width="1914" height="1024" alt="Screenshot 2026-03-05 223715" src="https://github.com/user-attachments/assets/4b046f07-6848-445d-9f9f-82a634549773" />
<img width="1905" height="907" alt="Airflow DAG running successfully " src="https://github.com/user-attachments/assets/3929112d-1608-46ac-a95c-f80462aa71d0" />
<img width="1907" height="915" alt="image" src="https://github.com/user-attachments/assets/e72011f6-56d9-4f86-b7a3-10c518514c60" />


### PostgreSQL Data
<img width="1917" height="950" alt="image" src="https://github.com/user-attachments/assets/74a58448-fe84-4d5f-ae24-3625a73c160b" />

## How to Run Locally

### Prerequisites
- Python 3.10
- PostgreSQL
- WSL2 (Windows) or Linux/Mac

### Setup
```bash
# Clone repo
git clone https://github.com/JIYA-YDV/automated-etl-pipeline-airflow-postgresql
cd automated-etl-pipeline-airflow-postgresql

# Create virtual environment
python3 -m venv airflow-env
source airflow-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your API key and DB credentials

# Initialize Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init

# Start Airflow
airflow webserver --port 8080 &
airflow scheduler &
```

### Run Tests
```bash
pytest tests/ -v
```

## Key Learnings
- Built end-to-end ETL pipeline from scratch
- Implemented pipeline orchestration with Apache Airflow DAGs
- Designed PostgreSQL schema for raw and transformed data layers
- Applied data quality validation patterns used in production
- Configured WSL2 + Linux environment for data engineering workflow

## Author
Jiya Yadav | MSc IT | Aspiring Data Engineer
