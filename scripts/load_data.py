"""
Data Loading Module
Loads transformed data to PostgreSQL and CSV file
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional
import os


def create_postgres_table(connection_params: dict) -> None:
    """
    Create the APOD data table in PostgreSQL if it doesn't exist
    
    Args:
        connection_params: Dictionary with PostgreSQL connection parameters
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS apod_data (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        title TEXT,
        url TEXT,
        explanation TEXT,
        media_type VARCHAR(50),
        hdurl TEXT,
        copyright TEXT,
        service_version VARCHAR(50),
        extraction_timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date)
    );
    """
    
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("PostgreSQL table 'apod_data' created or already exists")
    except Exception as e:
        print(f"Error creating PostgreSQL table: {e}")
        raise


def load_to_postgres(df: pd.DataFrame, connection_params: dict) -> None:
    """
    Load DataFrame to PostgreSQL database
    
    Args:
        df: DataFrame to load
        connection_params: Dictionary with PostgreSQL connection parameters
    """
    try:
        # Ensure table exists
        create_postgres_table(connection_params)
        
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Prepare data for insertion
        columns = ['date', 'title', 'url', 'explanation', 'media_type', 
                  'hdurl', 'copyright', 'service_version', 'extraction_timestamp']
        
        # Select only columns that exist in DataFrame
        available_columns = [col for col in columns if col in df.columns]
        df_subset = df[available_columns].copy()
        
        # Convert date to string format for PostgreSQL (handle both datetime and string)
        if 'date' in df_subset.columns:
            if pd.api.types.is_datetime64_any_dtype(df_subset['date']):
                df_subset['date'] = df_subset['date'].dt.strftime('%Y-%m-%d')
            elif df_subset['date'].dtype == 'object':
                # Already a string, just ensure format
                df_subset['date'] = pd.to_datetime(df_subset['date']).dt.strftime('%Y-%m-%d')
        
        # Convert extraction_timestamp to string (handle both datetime and string)
        if 'extraction_timestamp' in df_subset.columns:
            if pd.api.types.is_datetime64_any_dtype(df_subset['extraction_timestamp']):
                df_subset['extraction_timestamp'] = df_subset['extraction_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif df_subset['extraction_timestamp'].dtype == 'object':
                # Already a string, keep as is or ensure format
                pass  # Already in string format from XCom
        
        # Prepare values for insertion
        values = [tuple(row) for row in df_subset.values]
        columns_str = ','.join(available_columns)
        
        # Use ON CONFLICT to handle duplicates
        update_columns = [col for col in available_columns if col != 'date']
        update_str = ','.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        # Use execute_values for bulk insert with ON CONFLICT
        template = f"({','.join(['%s'] * len(available_columns))})"
        insert_query = f"""
        INSERT INTO apod_data ({columns_str})
        VALUES %s
        ON CONFLICT (date) 
        DO UPDATE SET {update_str}
        """
        
        execute_values(cursor, insert_query, values, template=template)
        conn.commit()
        
        print(f"Successfully loaded {len(df)} record(s) to PostgreSQL")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise


def load_to_csv(df: pd.DataFrame, file_path: str, mode: str = 'a') -> None:
    """
    Load DataFrame to CSV file
    
    Args:
        df: DataFrame to load
        file_path: Path to CSV file
        mode: File mode ('w' for write, 'a' for append)
    """
    try:
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        
        # Check if file exists to determine header
        header = not os.path.exists(file_path) or mode == 'w'
        
        # Append or write to CSV
        df.to_csv(file_path, mode=mode, header=header, index=False)
        
        print(f"Successfully loaded {len(df)} record(s) to CSV: {file_path}")
        
    except Exception as e:
        print(f"Error loading data to CSV: {e}")
        raise


def get_postgres_connection_params(
    conn_id: str = "mlops-a3",
    host: str = None,
    port: int = None,
    database: str = None,
    user: str = None,
    password: str = None
) -> dict:
    """
    Get PostgreSQL connection parameters from Airflow Connection or use defaults
    
    Args:
        conn_id: Airflow connection ID (default: "postgres_default")
        host, port, database, user, password: Fallback values if connection not found
    
    Returns:
        Dictionary with connection parameters
    """
    try:
        # Try to get connection from Airflow
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(conn_id)
        
        # Parse connection URI or use individual fields
        if conn.host:
            return {
                'host': conn.host,
                'port': conn.port or 5432,
                'database': conn.schema or database or 'apod_db',
                'user': conn.login or user or 'airflow',
                'password': conn.password or password or 'airflow'
            }
    except Exception as e:
        print(f"Could not load Airflow connection '{conn_id}': {e}. Using fallback values.")
    
    # Fallback to default values (for local development)
    return {
        'host': host or "postgres",
        'port': port or 5432,
        'database': database or "apod_db",
        'user': user or "airflow",
        'password': password or "airflow"
    }


if __name__ == "__main__":
    # Test loading
    test_df = pd.DataFrame({
        'date': ['2024-01-01'],
        'title': ['Test Title'],
        'url': ['https://example.com'],
        'explanation': ['Test explanation']
    })
    
    # Test CSV loading
    load_to_csv(test_df, '/tmp/test_apod_data.csv', mode='w')
    print("CSV loading test completed")

