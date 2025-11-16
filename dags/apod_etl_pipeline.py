"""
NASA APOD ETL Pipeline DAG
This DAG implements a complete ETL pipeline with 5 sequential steps:
1. Extract data from NASA APOD API
2. Transform data into clean format
3. Load data to PostgreSQL and CSV
4. Version data with DVC
5. Commit DVC metadata to Git
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')

from extract_data import extract_apod_data, load_api_key
from transform_data import transform_apod_data, validate_dataframe
from load_data import load_to_postgres, load_to_csv, get_postgres_connection_params
from dvc_operations import initialize_dvc, add_file_to_dvc
from git_operations import git_commit

# Default arguments for the DAG
default_args = {
    'owner': 'mlops_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'nasa_apod_etl_pipeline',
    default_args=default_args,
    description='NASA APOD ETL Pipeline with DVC and Git versioning',
    schedule=timedelta(days=1),  # Run daily (Airflow 3.x uses 'schedule' instead of 'schedule_interval')
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'nasa', 'apod', 'dvc', 'mlops'],
)

# Define paths
CSV_FILE_PATH = "/opt/airflow/data/apod_data.csv"
REPO_PATH = "/opt/airflow"

# Connection and Variable IDs for Astronomer
POSTGRES_CONN_ID = "mlops-a3"  # Change this to match your Astronomer connection ID
NASA_API_KEY_VAR = "NASA_API_KEY"  # Airflow Variable name for NASA API key

# Step 1: Extract data from NASA APOD API
def extract_task(**context):
    """Extract data from NASA APOD API"""
    api_key = load_api_key(variable_key=NASA_API_KEY_VAR)
    print(f"API Key loaded: {api_key[:10] + '...' if api_key and len(api_key) > 10 else 'None'}")
    
    raw_data = extract_apod_data(api_key)
    
    if raw_data is None:
        raise ValueError("Failed to extract data from NASA APOD API. Response was None.")
    
    print(f"Extracted data keys: {list(raw_data.keys()) if isinstance(raw_data, dict) else 'Not a dict'}")
    
    # Store raw data in XCom for next task
    context['ti'].xcom_push(key='raw_apod_data', value=raw_data)
    return raw_data

extract = PythonOperator(
    task_id='extract_apod_data',
    python_callable=extract_task,
    dag=dag,
)

# Step 2: Transform data
def transform_task(**context):
    """Transform raw data into clean DataFrame format"""
    import pandas as pd
    
    # Get raw data from previous task
    raw_data = context['ti'].xcom_pull(key='raw_apod_data', task_ids='extract_apod_data')
    
    if raw_data is None:
        raise ValueError("No data received from extract task. XCom pull returned None.")
    
    print(f"Received raw data with keys: {list(raw_data.keys()) if isinstance(raw_data, dict) else 'Not a dict'}")
    
    # Transform data
    df = transform_apod_data(raw_data)
    
    # Validate DataFrame
    if not validate_dataframe(df):
        raise ValueError("DataFrame validation failed")
    
    # Convert all datetime/Timestamp columns to strings for JSON serialization
    df_for_xcom = df.copy()
    for col in df_for_xcom.columns:
        # Check for pandas datetime types
        if pd.api.types.is_datetime64_any_dtype(df_for_xcom[col]):
            df_for_xcom[col] = df_for_xcom[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Check for object dtype that might contain datetime objects
        elif df_for_xcom[col].dtype == 'object':
            # Check if any value in the column is a datetime
            non_null_vals = df_for_xcom[col].dropna()
            if not non_null_vals.empty:
                first_val = non_null_vals.iloc[0]
                if isinstance(first_val, (pd.Timestamp, datetime)):
                    # Convert all datetime objects in this column to strings
                    df_for_xcom[col] = df_for_xcom[col].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, (pd.Timestamp, datetime)) and x is not None else x
                    )
    
    # Store DataFrame as JSON for XCom (convert to dict)
    # Use date_format='iso' to ensure all dates are strings
    df_dict = df_for_xcom.to_dict('records')
    
    # Final safety check: convert any remaining datetime objects in the dict
    def convert_datetime_to_str(obj):
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, dict):
            return {k: convert_datetime_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_datetime_to_str(item) for item in obj]
        return obj
    
    df_dict = convert_datetime_to_str(df_dict)
    context['ti'].xcom_push(key='transformed_data', value=df_dict)
    context['ti'].xcom_push(key='df_columns', value=list(df.columns))
    
    return df_dict

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

# Step 3: Load data to PostgreSQL and CSV
def load_task(**context):
    """Load data to both PostgreSQL and CSV file"""
    import pandas as pd
    
    # Get transformed data from previous task
    df_dict = context['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')
    df_columns = context['ti'].xcom_pull(key='df_columns', task_ids='transform_data')
    
    if df_dict is None:
        raise ValueError("No transformed data received from transform task. XCom pull returned None.")
    
    if df_columns is None:
        raise ValueError("No column information received from transform task.")
    
    print(f"Received transformed data: {len(df_dict)} record(s)")
    print(f"Columns: {df_columns}")
    
    # Reconstruct DataFrame
    df = pd.DataFrame(df_dict)
    
    # Ensure columns are in correct order
    if df_columns:
        df = df[df_columns]
    
    # Get PostgreSQL connection parameters (uses Airflow Connection in Astronomer)
    pg_params = get_postgres_connection_params(conn_id=POSTGRES_CONN_ID)
    
    # Load to PostgreSQL
    load_to_postgres(df, pg_params)
    
    # Load to CSV (append mode to accumulate historical data)
    load_to_csv(df, CSV_FILE_PATH, mode='a')
    
    return f"Data loaded to PostgreSQL and {CSV_FILE_PATH}"

load = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    dag=dag,
)

# Step 4: Version data with DVC
def dvc_version_task(**context):
    """Add CSV file to DVC version control"""
    # Ensure data directory exists
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}. Load task must complete successfully first.")
    
    print(f"CSV file exists: {CSV_FILE_PATH}")
    
    # Initialize DVC if needed
    initialize_dvc(REPO_PATH)
    
    # Add file to DVC
    dvc_file_path = add_file_to_dvc(CSV_FILE_PATH, REPO_PATH)
    
    # Store DVC file path in XCom for next task
    context['ti'].xcom_push(key='dvc_file_path', value=dvc_file_path)
    
    return dvc_file_path

dvc_version = PythonOperator(
    task_id='version_data_with_dvc',
    python_callable=dvc_version_task,
    dag=dag,
)

# Step 5: Commit DVC metadata to Git
def git_commit_task(**context):
    """Commit DVC metadata file to Git"""
    # Get DVC file path from previous task
    dvc_file_path = context['ti'].xcom_pull(key='dvc_file_path', task_ids='version_data_with_dvc')
    
    if dvc_file_path is None:
        raise ValueError("No DVC file path received from version_data_with_dvc task. XCom pull returned None.")
    
    print(f"Received DVC file path: {dvc_file_path}")
    
    # Extract relative path for Git
    if dvc_file_path.startswith(REPO_PATH):
        relative_path = dvc_file_path[len(REPO_PATH) + 1:]
    else:
        relative_path = dvc_file_path
    
    print(f"Relative path for Git: {relative_path}")
    
    # Commit to Git
    commit_message = f"Add DVC metadata for APOD data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    git_commit(relative_path, commit_message, REPO_PATH)
    
    return f"Committed {relative_path} to Git"

git_commit_op = PythonOperator(
    task_id='commit_dvc_to_git',
    python_callable=git_commit_task,
    dag=dag,
)

# Define task dependencies
extract >> transform >> load >> dvc_version >> git_commit_op

