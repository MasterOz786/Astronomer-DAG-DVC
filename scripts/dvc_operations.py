"""
DVC Operations Module
Handles DVC versioning operations
"""
import subprocess
import os
from typing import Optional


def initialize_dvc(repo_path: str = "/opt/airflow") -> None:
    """
    Initialize DVC repository if not already initialized
    
    Args:
        repo_path: Path to repository root
    """
    try:
        dvc_path = os.path.join(repo_path, ".dvc")
        if not os.path.exists(dvc_path):
            result = subprocess.run(
                ["dvc", "init", "--no-scm"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode == 0:
                print("DVC repository initialized successfully")
            else:
                print(f"DVC init output: {result.stdout}")
                print(f"DVC init error: {result.stderr}")
        else:
            print("DVC repository already initialized")
    except Exception as e:
        print(f"Error initializing DVC: {e}")
        raise


def add_file_to_dvc(file_path: str, repo_path: str = "/opt/airflow") -> str:
    """
    Add file to DVC version control
    
    Args:
        file_path: Path to file to add to DVC (can be absolute or relative)
        repo_path: Path to repository root
        
    Returns:
        Path to the created .dvc metadata file
    """
    try:
        # Ensure DVC is initialized
        initialize_dvc(repo_path)
        
        # Convert to absolute path if needed
        if not os.path.isabs(file_path):
            file_path = os.path.join(repo_path, file_path)
        
        # Get relative path from repo_path for DVC command
        try:
            rel_path = os.path.relpath(file_path, repo_path)
        except ValueError:
            # If paths are on different drives (Windows), use absolute path
            rel_path = file_path
        
        print(f"Adding file to DVC: {rel_path} (absolute: {file_path})")
        
        # Add file to DVC using relative path
        result = subprocess.run(
            ["dvc", "add", rel_path],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True
        )
        
        # DVC creates .dvc file in the same directory as the file
        dvc_file_path = os.path.join(os.path.dirname(file_path), f"{os.path.basename(file_path)}.dvc")
        
        print(f"Successfully added {file_path} to DVC")
        print(f"DVC metadata file created: {dvc_file_path}")
        print(f"DVC output: {result.stdout}")
        if result.stderr:
            print(f"DVC stderr: {result.stderr}")
        
        return dvc_file_path
        
    except subprocess.CalledProcessError as e:
        print(f"Error adding file to DVC: {e}")
        print(f"Error output: {e.stderr}")
        print(f"Command output: {e.stdout}")
        raise
    except Exception as e:
        print(f"Unexpected error in DVC add: {e}")
        raise


def get_dvc_status(repo_path: str = "/opt/airflow") -> str:
    """
    Get DVC status
    
    Args:
        repo_path: Path to repository root
        
    Returns:
        Status output as string
    """
    try:
        result = subprocess.run(
            ["dvc", "status"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False
        )
        return result.stdout
    except Exception as e:
        print(f"Error getting DVC status: {e}")
        return ""


if __name__ == "__main__":
    # Test DVC operations
    test_file = "/tmp/test_apod_data.csv"
    if os.path.exists(test_file):
        dvc_file = add_file_to_dvc(test_file)
        print(f"Created DVC file: {dvc_file}")

