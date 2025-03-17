import logging
import json
from datetime import datetime
import psutil  
import os
import glob

def setup_logger(job_name):
    """
    Configures a logger with a custom log file name based on the table_name (no timestamp).
    """
    # Ensure logs directory exists
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)

    # Define log file name with table_name only (no timestamp)
    log_file = os.path.join(log_dir, f"etl_{job_name}.log")

    # Set up logging configuration (append mode)
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode='a'  # 'a' means append mode (not overwrite)
    )

def log_etl_status(stage, status, **kwargs):
    """
    Logs ETL step information in a structured format.
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "status": status,
        **kwargs,
    }
    logging.info(json.dumps(log_entry))


def log_error(stage, error_message):
    """
    Logs error details in a structured format.
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "status": "failed",
        "error_message": error_message,
    }
    logging.error(json.dumps(log_entry))


def log_system_metrics(stage, status, **kwargs):
    """
    Logs system performance metrics like memory and CPU usage during each ETL stage.
    """
    memory_usage = psutil.virtual_memory().percent  # Percentage of memory used
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage percentage
    kwargs['memory_usage'] = memory_usage
    kwargs['cpu_usage'] = cpu_usage
    log_etl_status(stage, status, **kwargs)


def cleanup_old_logs(log_dir, days_to_keep=30):
    """
    Removes logs older than `days_to_keep` days.
    """
    now = datetime.now()

    # Loop through each log file in the directory
    for log_file in glob.glob(os.path.join(log_dir, "*.log")):
        # Get the file's last modified time
        file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
        
        # If the file is older than `days_to_keep` days, delete it
        if (now - file_time).days > days_to_keep:
            os.remove(log_file)
            print(f"🧹 Deleted old log file: {log_file}")