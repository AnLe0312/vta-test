import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

from datetime import datetime
from logs.etl_logger import setup_logger, log_etl_status, log_error, log_system_metrics
from modules.gcs_handler import get_file_last_modified_time, read_gcs_file
from modules.generate_query import generate_query
from modules.schema_handler import fetch_table_updated_at, fetch_table_schema, transform_dataframe_to_schema, prepare_sql_data
import yaml
import logging


def main(job_name, file_key, database_name, table_name, query_type):
    """
    Main function to extract, transform, and load data from GCS to ClickHouse.
    """
    # Setup custom logger per table
    setup_logger(job_name)

    job_start_time = datetime.now()
    log_etl_status("ETL Job", "STARTED", file_key=file_key, database=database_name, table=table_name)

    # Load configuration
    with open(f'{current_path}/models/storage_models.yaml', "r") as f:
        config = yaml.safe_load(f)

    bucket_name = config["gcs"]["bucket_name"]
    file_path = config["gcs"]["files"].get(file_key)

    # Get last modified time from GCS
    gcs_last_modified_time = get_file_last_modified_time(bucket_name, file_path)
    gcs_last_modified_time = datetime.strptime(gcs_last_modified_time, '%Y-%m-%d %H:%M:%S')

    # Fetch updated_at timestamp from ClickHouse
    table_updated_at = fetch_table_updated_at(database_name, table_name)

    # Check if table_updated_at is None or if gcs_last_modified_time is greater than table_updated_at
    if table_updated_at is None or gcs_last_modified_time > table_updated_at:
        try:
            extract_start_time = datetime.now()
            # Read data from GCS
            df = read_gcs_file(bucket_name, file_path)
            extract_end_time = datetime.now()

            extract_duration = (extract_end_time - extract_start_time).total_seconds()
            total_records_extracted = len(df)
            log_system_metrics("EXTRACT", "Completed", file_key=file_key, database=database_name, table=table_name,
                                total_records=total_records_extracted, execution_time=extract_duration)
            
            transform_start_time = datetime.now()
            # Fetch schema from ClickHouse
            schema = fetch_table_schema(database_name, table_name)

            # Transform data
            df = transform_dataframe_to_schema(df, schema)
            transform_end_time = datetime.now()
            
            transform_duration = (transform_end_time - transform_start_time).total_seconds()
            total_records_transformed = len(df)
            log_system_metrics("TRANSFORM", "Completed", file_key=file_key, database=database_name, table=table_name,
                                total_records=total_records_transformed, execution_time=transform_duration)

            load_start_time = datetime.now()
            # Convert to ClickHouse VALUES string
            data = prepare_sql_data(df)

            # Generate the INSERT query dynamically
            _ins_qr = generate_query(query_type=query_type, database_name=database_name, table=table_name, data=data, interval='12 DAY')
            load_end_time = datetime.now()

            load_duration = (load_end_time - load_start_time).total_seconds()
            total_records_loaded = len(df)  
            log_system_metrics("LOAD", "Completed", file_key=file_key, database=database_name, table=table_name,
                                total_records=total_records_loaded, execution_time=load_duration)
        except Exception as e:
            error_message = str(e)
            log_system_metrics("ETL Job", "failed", file_key=file_key, database=database_name, table=table_name,
                               error_message=error_message)
            log_error("ETL Job", error_message)
            logging.error(f"Error processing ETL job: {error_message}")
    else:
        logging.info(f"Skipping processing of {file_path}. The file is not newer than the table.")

if __name__ == "__main__":
    job_name = "gc_custinvoicejour"
    file_key = "custinvoicejour" 
    database_name = "prod_source"  
    table_name = "custinvoicejour"
    query_type= "INSERT"
    main(job_name, file_key, database_name, table_name, query_type)