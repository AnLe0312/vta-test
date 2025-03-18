import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

import yaml
from datetime import datetime
from modules.gcs_handler import get_file_last_modified_time, read_gcs_file
from modules.generate_query import generate_query
from modules.schema_handler import fetch_table_updated_at, fetch_table_schema, transform_dataframe_to_schema, prepare_sql_data
from logs.etl_logger import setup_logger, track_performance, run_etl_pipeline

# Setup job details
job_name = "gc_custinvoicejour"
file_key = "custinvoicejour"
database_name = "prod_source"
table_name = "custinvoicejour"
query_type = "INSERT"
current_path = os.path.dirname(os.path.realpath(__file__))

# Load configuration
with open(f'{current_path}/models/storage_models.yaml', "r") as f:
    config = yaml.safe_load(f)

bucket_name = config["gcs"]["bucket_name"]
file_path = config["gcs"]["files"].get(file_key)

# Setup logger
logger = setup_logger(job_name)


@track_performance("Extract", retries=3, backoff=2)
def extract(logger):
    """Extract data from GCS."""
    gcs_last_modified_time = get_file_last_modified_time(bucket_name, file_path)
    gcs_last_modified_time = datetime.strptime(gcs_last_modified_time, '%Y-%m-%d %H:%M:%S')
    table_updated_at = fetch_table_updated_at(database_name, table_name)

    # Check if the GCS file is newer than the table
    if table_updated_at is None or gcs_last_modified_time > table_updated_at:
        return read_gcs_file(bucket_name, file_path)
    else:
        logger.info(f"Skipping processing of {file_path}. The file is not newer than the table.")
        return None


@track_performance("Transform", retries=3, backoff=2)
def transform(data, logger):
    """Transform data according to table schema."""
    if data is None:
        raise ValueError("No data to transform")
    schema = fetch_table_schema(database_name, table_name)
    return transform_dataframe_to_schema(data, schema)


@track_performance("Load", retries=3, backoff=2)
def load(data, logger):
    """Load data into ClickHouse."""
    if data is None:
        raise ValueError("No data to load")
    prepared_data = prepare_sql_data(data)
    query = generate_query(query_type=query_type, database_name=database_name, table=table_name, data=prepared_data, interval='12 DAY')
    logger.info(f"Generated query: {query[:100]}...")


# Run ETL pipeline
if __name__ == "__main__":
    run_etl_pipeline(job_name, extract, transform, load)
