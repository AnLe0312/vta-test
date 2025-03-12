import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

from modules.db_connector import get_clickhouse_connection
from modules.gcs_handler import read_gcs_file
from modules.generate_query import generate_query
from modules.schema_handler import fetch_table_schema, prepare_data_for_load
import yaml
import logging
import sys
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(filename=f'{parent_path}/logs/process.log', level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def log_job(job_id, job_name, job_status, total_records, last_success=None):
    """
    Log job details for tracking pipeline execution.
    """
    log_entry = {
        "id": job_id,
        "job_name": job_name,
        "time": datetime.now().isoformat(),
        "job_status": job_status,
        "total_records": total_records,
        "last_success": last_success
    }
    logging.info(log_entry)

def load_data_into_clickhouse(database_name, table_name, df, job_id, job_name):
    """
    Load data into ClickHouse using dynamically generated queries.
    """
    client = get_clickhouse_connection(database_name)

    # Fetch schema from ClickHouse
    schema = fetch_table_schema(database_name, table_name)

    # Validate and transform data
    df_prepared = prepare_data_for_load(df, schema)

    # Generate the INSERT query dynamically
    query = generate_query(query_type="INSERT", database_name=database_name, table=table_name, data=df_prepared)

    try:
        client.execute(query)
        log_job(job_id, job_name, "SUCCESS", len(df), datetime.now().isoformat())
    except Exception as e:
        log_job(job_id, job_name, f"FAILED: {str(e)}", len(df))
        logging.error(f"Job {job_id} failed: {e}")

def main(file_key, database_name, table_name):
    """
    Main function to extract, transform, and load data from GCS to ClickHouse.
    """
    job_id = str(uuid.uuid4())
    job_name = file_key

    # Load configuration
    with open(f'{parent_path}/config/storage_config.yaml', "r") as f:
        config = yaml.safe_load(f)

    bucket_name = config["gcs"]["bucket_name"]
    file_path = config["gcs"]["files"].get(file_key)

    if not file_path:
        log_job(job_id, job_name, "FAILED: No file path found", 0)
        return

    log_job(job_id, job_name, "STARTED", 0)

    # Read data from GCS
    df = read_gcs_file(bucket_name, file_path)

    # Load into ClickHouse
    load_data_into_clickhouse(database_name, table_name, df, job_id, job_name)

if __name__ == "__main__":
    file_key = "custinvoicejour" 
    database_name = "prod_source"  
    table_name = "custinvoicejour"
    main(file_key, database_name, table_name)
