import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

from datetime import datetime
from logs.etl_logger import setup_logger
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
    ### EXTRACT Step
        # Read data from GCS
        df = read_gcs_file(bucket_name, file_path)


    ### TRANSFORM Step
        # Fetch schema from ClickHouse
        schema = fetch_table_schema(database_name, table_name)
        # Transform data
        df = transform_dataframe_to_schema(df, schema)


    ### LOAD Step
        # Convert to ClickHouse VALUES string
        data = prepare_sql_data(df)
        # Generate the INSERT query dynamically
        _ins_qr = generate_query(query_type=query_type, database_name=database_name, table=table_name, data=data, interval='12 DAY')
    else:
        logging.info(f"Skipping processing of {file_path}. The file is not newer than the table.")

if __name__ == "__main__":
    job_name = "gc_custinvoicejour"
    file_key = "custinvoicejour" 
    database_name = "prod_source"  
    table_name = "custinvoicejour"
    query_type= "INSERT"
    main(job_name, file_key, database_name, table_name, query_type)