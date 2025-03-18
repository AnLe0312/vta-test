import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

import time
import psutil
import logging
import traceback
from datetime import datetime, timedelta

# ================================
# 1. Setup Logger
# ================================

def setup_logger(job_name):
    """Sets up a logger for the ETL job, saving logs per job_name."""
    log_filename = f"{current_path}/etl_log_{job_name}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(job_name)
    return logger


# ================================
# 2. Log Cleanup
# ================================

def cleanup_logs(log_dir="logs", days_to_keep=30):
    """Removes log files older than the specified number of days."""
    now = datetime.now()
    cutoff_date = now - timedelta(days=days_to_keep)

    for filename in os.listdir(log_dir):
        if filename.startswith("etl_log_") and filename.endswith(".log"):
            file_path = os.path.join(log_dir, filename)
            file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))

            if file_modified_time < cutoff_date:
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted old log file: {filename}")
                except Exception as e:
                    logging.error(f"Failed to delete {filename}: {e}")


# ================================
# 3. Performance Tracking Decorator (with error handling)
# ================================

def track_performance(stage_name, retries=3, backoff=2):
    """
    Decorator to track time, memory, and CPU usage of ETL stages,
    with error handling and retries.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = args[-1]  # Assumes last argument is the logger

            # Track time, memory, and CPU at the start
            process = psutil.Process()
            start_memory = process.memory_info().rss / 1024 / 1024
            start_cpu = psutil.cpu_percent(interval=None)
            start_time = time.time()

            logger.info(f"Starting {stage_name}")

            # Retry logic
            attempts = 0
            while attempts < retries:
                try:
                    # Run the ETL function
                    result = func(*args, **kwargs)

                    # Track performance metrics after the function
                    end_memory = process.memory_info().rss / 1024 / 1024
                    end_cpu = psutil.cpu_percent(interval=None)
                    duration = time.time() - start_time
                    memory_used = end_memory - start_memory
                    cpu_used = end_cpu - start_cpu

                    # Log the performance metrics
                    logger.info(
                        f"Completed {stage_name} in {duration:.2f} seconds, "
                        f"memory used: {memory_used:.2f} MB, CPU used: {cpu_used:.2f}%"
                    )
                    return result

                except Exception as e:
                    attempts += 1
                    logger.error(
                        f"Error in {stage_name} (Attempt {attempts}/{retries}): {e}",
                        exc_info=True
                    )

                    if attempts < retries:
                        logger.warning(f"Retrying {stage_name} in {backoff ** attempts} seconds...")
                        time.sleep(backoff ** attempts)
                    else:
                        logger.critical(f"Failed {stage_name} after {retries} attempts.")
                        raise

        return wrapper

    return decorator


# ================================
# 4. ETL Pipeline Runner (With Error Recovery)
# ================================

def run_etl_pipeline(job_name, extract, transform, load):
    """Runs the ETL pipeline with logging, performance tracking, and error handling."""
    logger = setup_logger(job_name)
    logger.info(f"ETL Pipeline started for job: {job_name}")

    # Clean up old logs first
    cleanup_logs()

    try:
        # Track total pipeline performance (CPU, memory, time)
        pipeline_start_time = time.time()
        process = psutil.Process()
        pipeline_start_memory = process.memory_info().rss / 1024 / 1024
        pipeline_start_cpu = psutil.cpu_percent(interval=None)

        # Run ETL steps
        data = extract(logger)
        if data:
            transformed_data = transform(data, logger)
            if transformed_data:
                load(transformed_data, logger)

        # Capture total performance stats
        total_duration = time.time() - pipeline_start_time
        total_memory_used = (process.memory_info().rss / 1024 / 1024) - pipeline_start_memory
        total_cpu_used = psutil.cpu_percent(interval=None) - pipeline_start_cpu

        logger.info(
            f"ETL Pipeline completed for job: {job_name} in {total_duration:.2f} seconds, "
            f"total memory used: {total_memory_used:.2f} MB, total CPU used: {total_cpu_used:.2f}%"
        )

    except Exception as e:
        logger.critical(f"ETL Pipeline failed for job: {job_name} — Error: {e}", exc_info=True)
