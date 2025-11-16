# NASA APOD ETL Pipeline - MLOps Capstone Project

This project implements a complete MLOps pipeline that integrates Apache Airflow, DVC (Data Version Control), PostgreSQL, and Docker to create a reproducible ETL workflow for NASA's Astronomy Picture of the Day (APOD) data.

## 🎯 Project Overview

The pipeline executes five sequential steps:
1. **Extract (E)**: Retrieves daily data from NASA APOD API
2. **Transform (T)**: Cleans and structures the JSON data into a pandas DataFrame
3. **Load (L)**: Persists data to both PostgreSQL database and CSV file
4. **Version (DVC)**: Adds CSV file to DVC version control
5. **Commit (Git)**: Commits DVC metadata to Git repository

## 🏗️ Architecture

- **Orchestration**: Apache Airflow 2.8.0
- **Containerization**: Docker with custom Airflow image
- **Database**: PostgreSQL 15
- **Data Versioning**: DVC 3.41.1
- **Code Versioning**: Git

## 📁 Project Structure

```
MLOPS-A3/
├── dags/
│   └── apod_etl_pipeline.py      # Main Airflow DAG
├── scripts/
│   ├── extract_data.py           # Data extraction module
│   ├── transform_data.py         # Data transformation module
│   ├── load_data.py              # Data loading module
│   ├── dvc_operations.py         # DVC operations module
│   └── git_operations.py         # Git operations module
├── data/                         # Data storage directory
├── Dockerfile                    # Custom Airflow image
├── docker-compose.yml            # Local development setup
├── requirements.txt              # Python dependencies
├── api_key.txt                   # NASA API key (optional)
└── README.md                     # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Git installed
- (Optional) NASA API key from https://api.nasa.gov/

### Local Development Setup

1. **Clone the repository** (if not already done):
   ```bash
   git clone <your-repo-url>
   cd MLOPS-A3
   ```

2. **Set up API key** (optional, DEMO_KEY will be used if not provided):
   - Edit `api_key.txt` with your NASA API key
   - Or leave it empty to use DEMO_KEY

3. **Build and start services**:
   ```bash
   docker-compose up --build
   ```

4. **Initialize Airflow database** (first time only):
   ```bash
   docker-compose run airflow-init
   ```

5. **Access Airflow UI**:
   - Open http://localhost:8080
   - Default credentials: `airflow` / `airflow`

6. **Trigger the DAG**:
   - Navigate to the DAGs page
   - Find `nasa_apod_etl_pipeline`
   - Toggle it ON and trigger manually

### Verify Pipeline Execution

1. **Check PostgreSQL**:
   ```bash
   docker-compose exec postgres psql -U airflow -d apod_db -c "SELECT * FROM apod_data ORDER BY date DESC LIMIT 5;"
   ```

2. **Check CSV file**:
   ```bash
   docker-compose exec airflow-webserver cat /opt/airflow/data/apod_data.csv
   ```

3. **Check DVC files**:
   ```bash
   docker-compose exec airflow-webserver ls -la /opt/airflow/data/
   ```

4. **Check Git commits**:
   ```bash
   docker-compose exec airflow-webserver git log --oneline
   ```

## 🔧 Configuration

### PostgreSQL Connection

Default connection parameters (can be modified in `scripts/load_data.py`):
- Host: `postgres`
- Port: `5432`
- Database: `apod_db`
- User: `airflow`
- Password: `airflow`

### DVC Configuration

DVC is initialized automatically in the pipeline. The CSV file and its metadata are stored in:
- CSV: `/opt/airflow/data/apod_data.csv`
- DVC metadata: `/opt/airflow/data/apod_data.csv.dvc`

### Airflow Configuration

The DAG runs daily by default. To modify the schedule, edit `schedule_interval` in `dags/apod_etl_pipeline.py`.

## 📊 Data Schema

The `apod_data` table in PostgreSQL has the following schema:

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| date | DATE | APOD date (unique) |
| title | TEXT | Image title |
| url | TEXT | Image URL |
| explanation | TEXT | Image explanation |
| media_type | VARCHAR(50) | Media type (image/video) |
| hdurl | TEXT | HD image URL |
| copyright | TEXT | Copyright information |
| service_version | VARCHAR(50) | API service version |
| extraction_timestamp | TIMESTAMP | When data was extracted |
| created_at | TIMESTAMP | Record creation timestamp |

## 🐳 Deployment to Astronomer

To deploy to Astronomer Cloud:

1. **Install Astronomer CLI**:
   ```bash
   curl -sSL install.astronomer.io | sudo bash
   ```

2. **Login to Astronomer**:
   ```bash
   astro auth login
   ```

3. **Initialize Astronomer project**:
   ```bash
   astro dev init
   ```

4. **Deploy**:
   ```bash
   astro deploy
   ```

The Dockerfile is already configured with all necessary dependencies for Astronomer deployment.

## 🧪 Testing

### Test Individual Modules

```bash
# Test extraction
docker-compose exec airflow-webserver python /opt/airflow/scripts/extract_data.py

# Test transformation
docker-compose exec airflow-webserver python /opt/airflow/scripts/transform_data.py

# Test loading
docker-compose exec airflow-webserver python /opt/airflow/scripts/load_data.py
```

### Test DVC Operations

```bash
docker-compose exec airflow-webserver python /opt/airflow/scripts/dvc_operations.py
```

## 📝 Key Learnings

### Orchestration Mastery
- Defined complex, dependent workflows using Apache Airflow DAGs
- Implemented proper task dependencies and error handling
- Used XCom for inter-task data passing

### Data Integrity
- Concurrent loading to both relational database (PostgreSQL) and file storage (CSV)
- Implemented upsert logic to handle duplicate dates
- Data validation at transformation stage

### Data Lineage
- Integrated DVC for data versioning alongside Git
- Ensured traceability between code and data versions
- Created reproducible data artifacts

### Containerized Deployment
- Built custom Docker image with all dependencies
- Configured multi-service Docker Compose setup
- Prepared for Astronomer Cloud deployment

## 🔍 Troubleshooting

### DAG not appearing in Airflow UI
- Check DAG syntax: `docker-compose exec airflow-scheduler airflow dags list`
- Check logs: `docker-compose logs airflow-scheduler`

### PostgreSQL connection errors
- Verify PostgreSQL is healthy: `docker-compose ps postgres`
- Check connection parameters in `scripts/load_data.py`

### DVC initialization errors
- Ensure DVC is installed: `docker-compose exec airflow-webserver dvc --version`
- Check permissions on data directory

### Git commit errors
- Git operations are non-blocking (pipeline continues even if Git fails)
- Check Git configuration: `docker-compose exec airflow-webserver git config --list`

## 📚 References

- [NASA APOD API](https://api.nasa.gov/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [DVC Documentation](https://dvc.org/doc)
- [Astronomer Documentation](https://docs.astronomer.io/)

## 👥 Authors

MLOps Team - Capstone Project

## 📄 License

This project is for educational purposes.

