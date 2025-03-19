import logging
import yaml
import re
import os
import sys
import time
from datetime import datetime, timedelta
from requests.exceptions import Timeout

# Global paths
current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
logs_path = os.path.join(parent_path, 'logs', '_modules', 'generatequery')
sys.path.append(parent_path)

from modules.db_connector import get_clickhouse_connection


# Helper function to load YAML config
def load_yaml_config():
    config_path = os.path.join(parent_path, 'models', 'clickhouse_models.yaml')
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            logging.debug(f"Loaded YAML config successfully from {config_path}")
            return config
    except Exception as e:
        logging.error(f"Failed to load YAML config: {e}", exc_info=True)
        raise


# Get primary key column from config
def get_primary_key_col_from_yaml(database_name, table_name):
    config = load_yaml_config()
    try:
        primary_key = config['clickhouse']['databases'][database_name]['tables'][table_name].get('primary_key')
        if not primary_key:
            raise ValueError(f"Primary key not found for table {table_name} in database {database_name}.")
        logging.debug(f"Primary key for {database_name}.{table_name}: {primary_key}")
        return primary_key
    except KeyError as e:
        logging.error(f"Database {database_name} or table {table_name} not found in the configuration.", exc_info=True)
        raise ValueError(f"Database {database_name} or table {table_name} not found in the configuration.") from e


# Parse values from a row (handling different formats like quotes and NULL)
def parse_values_row(row):
    pattern = re.compile(
        r"""(
        '(?:''|[^'])*'                          |   # String in single quotes
        "[^"]*"                                 |   # String in double quotes
        NULL                                    |   # NULL value
        [+-]?\d+(\.\d+)?                        |   # Integer, float, negative numbers
        \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}     |   # Datetime format '%Y-%m-%d %H:%M:%S'
        ''                                      |   # Empty string ''
        [^,]+                                       # Unquoted text values
        )
        """,
        re.VERBOSE
    )
    return [match.group(0) for match in pattern.finditer(row)]


# Retry logic for executing queries
def execute_with_retries(query, client, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            logging.debug(f"Executing query (Attempt {attempt + 1}): {query}")
            client.query(query)
            logging.info("✅ Data inserted successfully!")
            return
        except Timeout as e:
            logging.warning(f"⏳ Timeout error (Attempt {attempt + 1}): {e}")
            attempt += 1
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                logging.error("❌ Max retries reached, operation failed.", exc_info=True)
                raise
        except Exception as e:
            logging.error(f"🚨 Error inserting batch (Attempt {attempt + 1}): {e}", exc_info=True)
            raise


# Batch insert function
def insert_in_batches(data, client, database_name, table, col_names_str, batch_size=1000):
    total_rows = len(data.split("\n"))
    logging.debug(f"Starting batch insert: {total_rows} rows to insert.")
    
    for start in range(0, total_rows, batch_size):
        batch_data = "\n".join(data.split("\n")[start:start + batch_size])
        batch_insert_query = f"INSERT INTO {database_name}.{table} ({col_names_str}) VALUES {batch_data};"
        logging.debug(f"Inserting batch {start} to {start + batch_size}")
        execute_with_retries(batch_insert_query, client)


# Generate SQL query for INSERT or DELETE
def generate_query(query_type, database_name, table, condition=None, data=None, interval='15 DAY'):

    client = get_clickhouse_connection(database_name)

    # Get column names and types
    describe_query = f"DESCRIBE TABLE {database_name}.{table}"
    logging.debug(f"Fetching column names: {describe_query}")

    try:
        query_result = client.query(describe_query)
        column_info = [(col[0], col[1]) for col in query_result.result_rows]  # Column name and type
        column_names = [col[0] for col in column_info]

        if not column_names:
            raise ValueError(f"Table {database_name}.{table} has no columns.")

        primary_key_col = "RECID" if "RECID" in column_names else column_names[0]
        try:
            primary_key_col = get_primary_key_col_from_yaml(database_name, table)
        except ValueError as e:
            logging.warning(f"Using default primary key {primary_key_col} due to error: {e}")

        col_names_str = ", ".join(column_names)

        if query_type == 'INSERT':
            if not data:
                raise ValueError("Data must be provided for INSERT queries.")

            logging.debug(f"Raw data received for INSERT: {data}")

            data_rows = [row.strip() for row in re.split(r"\),\s*\(", data.strip("()")) if row.strip()]
            logging.debug(f"Parsed data rows count: {len(data_rows)}")

            # Get primary key index and type
            pk_index = column_names.index(primary_key_col)
            pk_type = next((col_type for col_name, col_type in column_info if col_name == primary_key_col), None)

            valid_rows = []

            # Primary key validation function
            def is_valid_pk(value, pk_type):
                """ Validate primary key value against its data type """
                try:
                    # Try direct type conversion
                    if pk_type.startswith("UInt"):
                        return isinstance(int(value), int) and int(value) >= 0
                    if pk_type.startswith("Int"):
                        return isinstance(int(value), int)
                    if pk_type.startswith("Float"):
                        return isinstance(float(value), float)
                    if pk_type.startswith("String"):
                        return isinstance(str(value), str)
                    logging.warning(f"Unknown primary key type '{pk_type}' for value '{value}'")
                    return False
                except ValueError:
                    return False

            # 🛠️ Process each data row
            for row in data_rows:
                try:
                    parsed_row = parse_values_row(row)
                    pk_value = parsed_row[pk_index]

                    # Check if the primary key value matches its type
                    if is_valid_pk(pk_value, pk_type):
                        valid_rows.append(parsed_row)
                    else:
                        logging.error(f"Invalid primary key value '{pk_value}' (type: '{pk_type}') at row: {row}. Skipping.")
                except Exception as e:
                    logging.error(f"Error parsing row: {row}. Skipping. Error: {e}")
                    continue

            logging.debug(f"Valid rows count: {len(valid_rows)}")

            if not valid_rows:
                logging.info("✨ No valid rows to insert.")
                return

            new_data_string = ",\n".join([f"({','.join(map(str, row))})" for row in valid_rows])
            insert_in_batches(new_data_string, client, database_name, table, col_names_str)

            # Optional: Optimize the table after insert/update
            optimize_query = f"OPTIMIZE TABLE {database_name}.{table} FINAL"
            logging.debug(f"Optimizing table with query: {optimize_query}")
            client.query(optimize_query)

        elif query_type == 'DELETE':
            if not condition:
                raise ValueError("Condition must be provided for DELETE queries.")
            delete_query = f"ALTER TABLE {database_name}.{table} DELETE WHERE {condition}"
            logging.debug(f"Executing DELETE query: {delete_query}")
            client.query(delete_query)

        else:
            raise ValueError("Invalid query type. Choose from 'INSERT' or 'DELETE'.")

    except Exception as e:
        logging.error(f"Error generating query: {e}", exc_info=True)
        raise