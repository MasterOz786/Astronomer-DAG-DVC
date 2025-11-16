"""
Git Operations Module
Handles Git versioning operations
"""
import subprocess
import os
from typing import Optional


def git_commit(file_path: str, message: str, repo_path: str = "/opt/airflow") -> None:
    """
    Commit a file to Git repository
    
    Args:
        file_path: Path to file to commit
        repo_path: Path to repository root
        message: Commit message
    """
    try:
        # Check if file exists
        full_path = os.path.join(repo_path, file_path) if not os.path.isabs(file_path) else file_path
        if not os.path.exists(full_path):
            print(f"Warning: File {full_path} does not exist, skipping Git commit")
            return
        
        # Check if Git is initialized
        git_dir = os.path.join(repo_path, ".git")
        if not os.path.exists(git_dir):
            print("Git repository not initialized. Initializing...")
            subprocess.run(
                ["git", "init"],
                cwd=repo_path,
                check=True
            )
            # Configure Git user (required for commits)
            subprocess.run(
                ["git", "config", "user.email", "airflow@example.com"],
                cwd=repo_path,
                check=False
            )
            subprocess.run(
                ["git", "config", "user.name", "Airflow"],
                cwd=repo_path,
                check=False
            )
        
        # Add file to staging
        subprocess.run(
            ["git", "add", file_path],
            cwd=repo_path,
            check=True
        )
        
        # Commit
        result = subprocess.run(
            ["git", "commit", "-m", message],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            print(f"Successfully committed {file_path} to Git")
            print(f"Commit message: {message}")
        else:
            # Check if there are changes to commit
            if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr.lower():
                print(f"No changes to commit for {file_path}")
            else:
                print(f"Git commit output: {result.stdout}")
                print(f"Git commit error: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, "git commit", result.stderr)
        
    except subprocess.CalledProcessError as e:
        print(f"Error committing to Git: {e}")
        print(f"Error output: {e.stderr if hasattr(e, 'stderr') else ''}")
        # Don't raise - Git operations are not critical for pipeline execution
    except Exception as e:
        print(f"Unexpected error in Git commit: {e}")
        # Don't raise - Git operations are not critical for pipeline execution


def git_status(repo_path: str = "/opt/airflow") -> str:
    """
    Get Git status
    
    Args:
        repo_path: Path to repository root
        
    Returns:
        Status output as string
    """
    try:
        result = subprocess.run(
            ["git", "status"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False
        )
        return result.stdout
    except Exception as e:
        print(f"Error getting Git status: {e}")
        return ""


if __name__ == "__main__":
    # Test Git operations
    test_file = "test.dvc"
    if os.path.exists(test_file):
        git_commit(test_file, "Test commit")

