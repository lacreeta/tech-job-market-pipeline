# Tech Job Market Pipeline

A daily data pipeline that extracts, transforms, and loads tech job market data into PostgreSQL using Apache Airflow and Docker.

## Overview

This project automates the collection and processing of technology job listings from public sources. It:
- **Extracts** job offers from APIs or web scraping sources
- **Transforms** and cleans the data for consistency and quality
- **Loads** the processed data into a PostgreSQL database
- **Runs daily** using Apache Airflow orchestration

## Project Structure

```
.
├── dags/                    # Airflow DAG definitions
├── src/                     # Python source code
│   ├── extract/            # Data extraction logic
│   ├── transform/          # Data transformation & cleaning
│   ├── load/               # Database loading logic
│   ├── report/             # Report generation
│   ├── schemas/            # Data schema definitions
│   └── utils/              # Utility functions
├── sql/                    # SQL scripts and migrations
├── data/                   # Data storage
│   ├── raw/               # Raw extracted data
│   └── processed/         # Processed transformed data
├── config/                # Configuration files
├── plugins/               # Airflow plugins
├── tests/                 # Test suites
├── logs/                  # Airflow logs
├── docker-compose.yml     # Docker services definition
├── requirements.txt       # Python dependencies
└── .env                   # Environment variables
```

## Getting Started

### Prerequisites

- Docker and Docker Compose installed ([Install Docker](https://docs.docker.com/get-docker/))
- At least 4GB of RAM and 10GB of disk space available
- Linux/macOS or Windows with WSL2

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd tech-job-market-pipeline
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```


3. **Build and start services**
   ```bash
   docker compose up -d
   ```

### Accessing Services

| Service | URL | Default Login |
|---------|-----|---|
| Airflow WebUI | `http://localhost:8080` | Configured in `.env` |
| PostgreSQL (Airflow) | `localhost:5432` | Internal Airflow metadata database |
| PostgreSQL (Job Market) | `localhost:5432` | Configured in `.env` |

## Services

### Apache Airflow
- **API Server**: REST API for programmatic access to Airflow
- **Scheduler**: Orchestrates DAG execution on defined schedules
- **DAG Processor**: Monitors DAG files for changes
- **PostgreSQL**: Airflow metadata database

### Job Market Database
- Dedicated PostgreSQL instance for job market data storage
- Isolated from Airflow metadata database

## Development

### Installing Dependencies

```bash
# Inside the Airflow container
docker compose exec airflow-apiserver pip install -r requirements.txt
```

### Creating a New DAG

Create a new Python file in `dags/`:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'my_job_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Define your tasks here
    pass
```

### Running Tests

```bash
docker compose exec airflow-apiserver pytest tests/
```

## Pipeline Workflow

1. **Extract Phase** (`src/extract/`)
   - Fetch job listings from data sources
   - Validate and store raw data in `data/raw/`

2. **Transform Phase** (`src/transform/`)
   - Clean and normalize data
   - Apply business logic and validations
   - Handle missing values and duplicates

3. **Load Phase** (`src/load/`)
   - Write processed data to job_market PostgreSQL database
   - Maintain data integrity and relationships

4. **Report Phase** (`src/report/`)
   - Generate insights and summaries
   - Create data quality reports

## Database Schema

Job market database schemas are defined in `src/schemas/`. Migrations are managed in `sql/`.

## Monitoring & Logs

- Airflow Logs: `logs/` directory
- Airflow WebUI: Dashboard for monitoring DAG execution and task status
- Check service health:
  ```bash
  docker compose ps
  ```

## Troubleshooting

### Services won't start
```bash
# Check resource availability (minimum 4GB RAM)
# Ensure ports 8080 and 5432 are not blocked

# View logs
docker compose logs -f
```

### Reset Airflow
```bash
# Stop all services
docker compose down

# Remove volumes (WARNING: deletes all data)
docker volume rm tech-job-market-pipeline_postgres-db-volume tech-job-market-pipeline_job-db-volume

# Restart
docker compose up -d
```

### Connect to Job Market DB
```bash
docker compose exec job_market psql -U "$JOB_DB_USER" -d "$JOB_DB_NAME"
```

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker & Docker Compose](https://docs.docker.com/)


## Contact & Contribution

- **Email**: andresgonzalezmillan03@gmail.com
- **LinkedIn**: [Andres Gonzalez](https://www.linkedin.com/in/andr%C3%A9s-gonz%C3%A1lez-a53bb929a/)
- **GitHub**: [@yourprofile](https://github.com/lacreeta)

For contributions, please open an issue or submit a pull request.
