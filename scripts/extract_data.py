"""
Data Extraction Module
Extracts data from NASA APOD API
"""
import requests
import json
import os
from datetime import datetime
from typing import Dict, Optional


def extract_apod_data(api_key: Optional[str] = None) -> Dict:
    """
    Extract data from NASA APOD API
    
    Args:
        api_key: NASA API key. If None, uses DEMO_KEY
        
    Returns:
        Dictionary containing APOD data
    """
    if api_key is None:
        api_key = "DEMO_KEY"
    
    url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        print(f"Successfully extracted APOD data for date: {data.get('date', 'N/A')}")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error extracting data from NASA APOD API: {e}")
        raise


def load_api_key(
    variable_key: str = "NASA_API_KEY",
    file_path: str = "/opt/airflow/api_key.txt"
) -> Optional[str]:
    """
    Load API key from Airflow Variable or file (fallback for local development)
    
    Args:
        variable_key: Airflow Variable key name (default: "NASA_API_KEY")
        file_path: Fallback file path for local development
        
    Returns:
        API key string or None if not found
    """
    # Try to get from Airflow Variables first
    try:
        from airflow.models import Variable
        api_key = Variable.get(variable_key, default_var=None)
        if api_key:
            print(f"Loaded API key from Airflow Variable: {variable_key}")
            return api_key
    except Exception as e:
        print(f"Could not load Airflow Variable '{variable_key}': {e}. Trying file fallback.")
    
    # Fallback to file (for local development)
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                api_key = f.read().strip()
                if api_key:
                    print(f"Loaded API key from file: {file_path}")
                    return api_key
    except Exception as e:
        print(f"Error loading API key from file: {e}")
    
    return None


if __name__ == "__main__":
    # Test extraction
    api_key = load_api_key()
    data = extract_apod_data(api_key)
    print(json.dumps(data, indent=2))

