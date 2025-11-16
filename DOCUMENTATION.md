# 🎓 MLOps Assignment 3: NASA APOD Data Pipeline Documentation

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Pipeline Execution Flow](#pipeline-execution-flow)
5. [Verification Steps](#verification-steps)
6. [Key Learnings](#key-learnings)

---

## Project Overview

This project implements a complete ETL pipeline that:
- Extracts daily data from NASA APOD API
- Transforms and validates the data
- Loads data to PostgreSQL and CSV
- Versions data with DVC
- Commits DVC metadata to Git

**Tech Stack:** Apache Airflow, PostgreSQL, DVC, Docker, Astronomer

---

## Prerequisites

- Docker & Docker Compose installed
- Git installed
- (Optional) NASA API key from https://api.nasa.gov/

---

## Setup Instructions

### Step 1: Clone Repository
```bash
git clone <your-repo-url>
cd MLOPS-A3
```

**Screenshot 1:** Terminal showing successful git clone

### Step 2: Configure API Key (Optional)
Edit `api_key.txt` with your NASA API key, or leave empty to use DEMO_KEY.

**Screenshot 2:** api_key.txt file content

### Step 3: Build and Start Services
```bash
docker-compose up --build -d
```

**Screenshot 3:** Terminal showing docker-compose build output

### Step 4: Initialize Airflow Database
```bash
docker-compose run airflow-init
```

**Screenshot 4:** Terminal showing Airflow initialization

### Step 5: Access Airflow UI
- Open http://localhost:8080
- Login: `airflow` / `airflow`

**Screenshot 5:** Airflow login page

---

## Pipeline Execution Flow

### Step 1: Access Airflow DAGs
Navigate to the DAGs page in Airflow UI.

**Screenshot 6:** Airflow DAGs list showing `nasa_apod_etl_pipeline`

### Step 2: Enable and Trigger DAG
1. Toggle the DAG ON (if paused)
2. Click "Trigger DAG" button

**Screenshot 7:** DAG enabled and trigger button highlighted

### Step 3: Monitor Pipeline Execution

#### Task 1: Extract APOD Data
- Connects to NASA APOD API
- Retrieves daily structured data

**Screenshot 8:** Extract task running (green) in Graph View

#### Task 2: Transform Data
- Selects fields: date, title, URL, explanation, etc.
- Creates clean pandas DataFrame

**Screenshot 9:** Transform task running (green) in Graph View

#### Task 3: Load Data
- Loads to PostgreSQL table `apod_data`
- Saves to CSV file `apod_data.csv`

**Screenshot 10:** Load task running (green) in Graph View

#### Task 4: Version with DVC
- Initializes DVC if needed
- Adds CSV to DVC version control
- Creates `apod_data.csv.dvc` metadata file

**Screenshot 11:** DVC version task running (green) in Graph View

#### Task 5: Commit to Git
- Commits DVC metadata file to Git
- Links code version to data version

**Screenshot 12:** Git commit task running (green) in Graph View

**Screenshot 13:** Complete pipeline Graph View showing all tasks successful (green)

**Screenshot 14:** Tree View showing task dependencies and execution times

---

## Verification Steps

### Verify PostgreSQL Data
```bash
docker-compose exec postgres psql -U airflow -d apod_db -c "SELECT date, title FROM apod_data ORDER BY date DESC LIMIT 5;"
```

**Screenshot 15:** Terminal showing PostgreSQL query results

### Verify CSV File
```bash
docker-compose exec airflow-webserver cat /opt/airflow/data/apod_data.csv
```

**Screenshot 16:** Terminal showing CSV file content

### Verify DVC Files
```bash
docker-compose exec airflow-webserver ls -la /opt/airflow/data/ | grep dvc
```

**Screenshot 17:** Terminal showing DVC metadata files

### Verify Git Commit
```bash
docker-compose exec airflow-webserver git log --oneline -5
```

**Screenshot 18:** Terminal showing Git commit history

### Verify in Airflow Logs
Click on any task → View Log → Check execution logs

**Screenshot 19:** Airflow task log showing successful execution

---

## Key Learnings

### ✅ Orchestration Mastery
- Defined complex workflows with Airflow DAGs
- Implemented task dependencies and error handling
- Used XCom for inter-task data passing

**Screenshot 20:** DAG code showing task dependencies

### ✅ Data Integrity
- Concurrent loading to PostgreSQL and CSV
- Upsert logic for duplicate handling
- Data validation at transformation stage

**Screenshot 21:** Load script showing dual storage implementation

### ✅ Data Lineage
- DVC tracks data versions
- Git commits link code to data versions
- Full traceability for reproducibility

**Screenshot 22:** DVC metadata file and Git commit showing version linkage

### ✅ Containerized Deployment
- Custom Docker image with all dependencies
- Docker Compose for local development
- Ready for Astronomer Cloud deployment

**Screenshot 23:** Dockerfile showing dependency installation

---

## Deployment to Astronomer (Optional)

### Step 1: Install Astronomer CLI
```bash
curl -sSL install.astronomer.io | sudo bash
```

### Step 2: Login and Deploy
```bash
astro auth login
astro deploy
```

**Screenshot 24:** Astronomer deployment output

---

## Conclusion

This pipeline successfully demonstrates:
- ✅ End-to-end ETL workflow orchestration
- ✅ Data persistence in multiple storage systems
- ✅ Data versioning with DVC
- ✅ Code-data linkage through Git
- ✅ Containerized, reproducible deployment

**Screenshot 25:** Final project structure in file explorer

---

## Appendix: Project Structure

```
MLOPS-A3/
├── dags/
│   └── apod_etl_pipeline.py      # Main Airflow DAG
├── scripts/
│   ├── extract_data.py           # Step 1: Extraction
│   ├── transform_data.py         # Step 2: Transformation
│   ├── load_data.py             # Step 3: Loading
│   ├── dvc_operations.py         # Step 4: DVC versioning
│   └── git_operations.py         # Step 5: Git commit
├── data/                         # Data storage
├── Dockerfile                    # Custom Airflow image
├── docker-compose.yml            # Local setup
└── requirements.txt              # Dependencies
```

