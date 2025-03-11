import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

from modules.db_connector import get_clickhouse_connection
from modules.gcs_handler import read_gcs_file
import yaml
import logging
import uuid
from datetime import datetime

logging.basicConfig(filename=f'{parent_path}/logs/process.log', level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def log_job(job_id, job_name, job_status, total_records, last_success=None):
    log_entry = {
        "id": job_id,
        "job_name": job_name,
        "time": datetime.now().isoformat(),
        "job_status": job_status,
        "total_records": total_records,
        "last_success": last_success
    }
    logging.info(log_entry)

def load_data_into_clickhouse(table_name, df, job_id, job_name):
    client = get_clickhouse_connection()
    cols = ", ".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    query = f"INSERT INTO {table_name} ({cols}) VALUES"
    try:
        client.execute(query, values)
        log_job(job_id, job_name, "SUCCESS", len(df), datetime.now().isoformat())
    except Exception as e:
        log_job(job_id, job_name, f"FAILED: {str(e)}", len(df))

def main(file_key, table_name):
    job_id = str(uuid.uuid4())
    job_name = file_key
    with open(f'{parent_path}/config/config.yaml', "r") as f:
        config = yaml.safe_load(f)
    bucket_name = config["gcs"]["bucket_name"]
    file_path = config["gcs"]["files"].get(file_key)
    if not file_path:
        log_job(job_id, job_name, "FAILED: No file path found", 0)
        return
    log_job(job_id, job_name, "STARTED", 0)
    df = read_gcs_file(bucket_name, file_path)
    load_data_into_clickhouse(table_name, df, job_id, job_name)

if __name__ == "__main__":
    file_key = sys.argv[1] 
    table_name = sys.argv[2]  
    main(file_key, table_name)