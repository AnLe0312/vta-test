import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

import pandas as pd
import yaml
from datetime import datetime
from modules.gcs_handler import read_gcs_file
from modules.generate_query import generate_query
from modules.schema_handler import fetch_table_last_synced_at, fetch_table_schema, transform_dataframe_to_schema, prepare_sql_data, get_max_modified_datetime_from_schema, update_last_synced_at_in_schema
from logs.etl_logger import setup_logger, track_performance, run_etl_pipeline

# Setup job details
job_name = "daily_SRC_custinvoicejour"
file_key = "custinvoicejour"
database_name = "prod_source"
table_name = "custinvoicejour"
query_type = "INSERT"

# Setup logger
logger = setup_logger(job_name)

# Load configuration
with open(f'{current_path}/models/storage_models.yaml', "r") as f:
    config = yaml.safe_load(f)

bucket_name = config["gcs"]["bucket_name"]
file_path = config["gcs"]["files"].get(file_key)
if not file_path:
    logger.critical(f"File path for key '{file_key}' not found in configuration.")
    raise ValueError(f"Missing file path for key: {file_key}")

# Global variables to store MODIFIEDDATETIME range for update
extracted_mod_min = None
extracted_mod_max = None

@track_performance("Extract", retries=3, backoff=2)
def extract(logger):
    """Extract data from GCS and filter based on MODIFIEDDATETIME."""
    global extracted_mod_min, extracted_mod_max

    df = read_gcs_file(bucket_name, file_path)
    
    if 'MODIFIEDDATETIME' not in df.columns:
        logger.error(f"MODIFIEDDATETIME column not found in the file {file_path}.")
        return None
    
    df['MODIFIEDDATETIME'] = pd.to_datetime(df['MODIFIEDDATETIME'], errors='coerce')

    df.sort_values(by='MODIFIEDDATETIME', inplace=True)
    df.reset_index(drop=True, inplace=True)

    table_last_synced_at = fetch_table_last_synced_at(database_name, table_name)
    
    # Return the entire file or filter based on table_updated_at
    if table_last_synced_at is None or df['MODIFIEDDATETIME'].max() >= table_last_synced_at:
        extracted_data = df[df['MODIFIEDDATETIME'] >= table_last_synced_at].copy().reset_index(drop=True) if table_last_synced_at else df
        
        # Store min & max MODIFIEDDATETIME for later update
        extracted_mod_min = extracted_data['MODIFIEDDATETIME'].min()
        extracted_mod_max = extracted_data['MODIFIEDDATETIME'].max()

        logger.info(f"Extracted {len(extracted_data)} records from GCS.")
        logger.debug(f"MODIFIEDDATETIME range in extracted data: {extracted_mod_min} → {extracted_mod_max}")

        return extracted_data
    logger.info(f"Skipping processing of {file_path}. The file is not newer than the table.")
    return None


@track_performance("Transform", retries=3, backoff=2)
def transform(data, logger):
    """Transform data according to table schema."""
    if data is None or data.empty:
        raise ValueError("No data to transform")
    schema = fetch_table_schema(database_name, table_name)
    transformed_data = transform_dataframe_to_schema(data, schema, logger=logger)
    logger.info(f"Transformed {len(transformed_data)} records.")
    return transformed_data


@track_performance("Load", retries=3, backoff=2)
def load(data, logger):
    """Load data into ClickHouse."""
    global extracted_mod_min, extracted_mod_max

    if data is None or data.empty:
        raise ValueError("No data to load")
    prepared_data = prepare_sql_data(data)
    if not prepared_data:
        logger.error("No prepared data to load.")
        return
    logger.info(f"Loading {len(data)} records into {database_name}.{table_name}.")
    query = generate_query(query_type=query_type, database_name=database_name, table=table_name, data=prepared_data, logger=logger)
    logger.info(f"Query of type '{query_type}' for table {database_name}.{table_name} has been generated.")

    # After successful insert, update last_modified in schema using actual ClickHouse data
    new_last_synced_at = get_max_modified_datetime_from_schema(database_name, table_name)
    update_last_synced_at_in_schema(database_name, table_name, new_last_synced_at, extracted_mod_min, extracted_mod_max, logger=logger)

# Run ETL pipeline
if __name__ == "__main__":
    run_etl_pipeline(job_name, extract, transform, load)