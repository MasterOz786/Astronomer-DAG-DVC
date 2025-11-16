"""
Data Transformation Module
Transforms raw APOD JSON data into clean DataFrame format
"""
import pandas as pd
from typing import Dict, List
from datetime import datetime


def transform_apod_data(raw_data: Dict) -> pd.DataFrame:
    """
    Transform raw APOD JSON data into a clean DataFrame
    
    Args:
        raw_data: Raw JSON data from NASA APOD API
        
    Returns:
        Cleaned pandas DataFrame with selected fields
    """
    # Select fields of interest
    fields_of_interest = {
        'date': raw_data.get('date', ''),
        'title': raw_data.get('title', ''),
        'url': raw_data.get('url', ''),
        'explanation': raw_data.get('explanation', ''),
        'media_type': raw_data.get('media_type', ''),
        'hdurl': raw_data.get('hdurl', ''),
        'copyright': raw_data.get('copyright', ''),
        'service_version': raw_data.get('service_version', '')
    }
    
    # Create DataFrame from single record
    df = pd.DataFrame([fields_of_interest])
    
    # Ensure date column is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Add extraction timestamp
    df['extraction_timestamp'] = datetime.now()
    
    print(f"Transformed data: {len(df)} record(s) with {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    
    return df


def validate_dataframe(df: pd.DataFrame) -> bool:
    """
    Validate the transformed DataFrame
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_columns = ['date', 'title', 'url', 'explanation']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Warning: Missing required columns: {missing_columns}")
        return False
    
    if df.empty:
        print("Warning: DataFrame is empty")
        return False
    
    return True


if __name__ == "__main__":
    # Test transformation
    test_data = {
        'date': '2024-01-01',
        'title': 'Test Title',
        'url': 'https://example.com/image.jpg',
        'explanation': 'Test explanation',
        'media_type': 'image'
    }
    df = transform_apod_data(test_data)
    print(df)
    print(f"Validation: {validate_dataframe(df)}")

