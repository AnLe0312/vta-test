import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

import pandas as pd
import yaml
from datetime import datetime
from modules.gcs_handler import read_gcs_file
from modules.generate_query import generate_query
from modules.schema_handler import fetch_table_updated_at, fetch_table_schema, transform_dataframe_to_schema, prepare_sql_data
from logs.etl_logger import setup_logger, track_performance, run_etl_pipeline

# Setup job details
job_name = "monthly_gc_custinvoicejour"
file_key = "custinvoicejour"
database_name = "prod_source"
table_name = "custinvoicejour"
query_type = "INSERT"

# Load configuration
with open(f'{current_path}/models/storage_models.yaml', "r") as f:
    config = yaml.safe_load(f)

bucket_name = config["gcs"]["bucket_name"]
file_path = config["gcs"]["files"].get(file_key)

# Setup logger
logger = setup_logger(job_name)


@track_performance("Extract", retries=3, backoff=2)
def extract(logger):
    """Extract data from GCS and filter based on MODIFIEDDATETIME."""
    df = read_gcs_file(bucket_name, file_path)
    
    if 'MODIFIEDDATETIME' not in df.columns:
        logger.error(f"MODIFIEDDATETIME column not found in the file {file_path}.")
        return None
    
    df['MODIFIEDDATETIME'] = pd.to_datetime(df['MODIFIEDDATETIME'], errors='coerce')
    table_updated_at = fetch_table_updated_at(database_name, table_name)

    threshold_date = table_updated_at - pd.DateOffset(months=1)
    
    # Return the entire file or filter based on table_updated_at
    if df['MODIFIEDDATETIME'].max() > threshold_date:
        extracted_data = df[df['MODIFIEDDATETIME'] > threshold_date].copy().reset_index(drop=True)
        logger.info(f"Extracted {len(extracted_data)} records from GCS.")
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
    if data is None or data.empty:
        raise ValueError("No data to load")
    prepared_data = prepare_sql_data(data)
    if not prepared_data:
        logger.error("No prepared data to load.")
        return
    logger.info(f"Loading {len(data)} records into {database_name}.{table_name}.")
    query = generate_query(query_type=query_type, database_name=database_name, table=table_name, data=prepared_data, interval='12 DAY', logger=logger)
    logger.info(f"Query of type '{query_type}' for table {database_name}.{table_name} has been generated.")


# Run ETL pipeline
if __name__ == "__main__":
    run_etl_pipeline(job_name, extract, transform, load)
