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

# Setup logging per table
def setup_logging(table_name):
    log_filename = os.path.join(logs_path, f"query_{table_name}.log")
    logging.basicConfig(
        filename=log_filename, 
        level=logging.DEBUG, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='a'  # 'a' means append mode (not overwrite)
    )
    logging.debug(f"Logging initialized for table: {table_name}")


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


# Update records in batches
def update_in_batches(update_queries, client, batch_size=1000):
    for start in range(0, len(update_queries), batch_size):
        batch_queries = update_queries[start:start + batch_size]
        for update_query in batch_queries:
            execute_with_retries(update_query, client)


# Generate SQL query for INSERT or DELETE
def generate_query(query_type, database_name, table, condition=None, data=None, interval='15 DAY'):

    setup_logging(table)

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

            pk_index = column_names.index(primary_key_col)

            parsed_rows = []
            for row in data_rows:
                try:
                    parsed_row = parse_values_row(row)
                    parsed_rows.append(parsed_row)
                except Exception as e:
                    logging.error(f"Error parsing row: {row}. Skipping this row. Error: {e}")
                    continue  # Skip this row and continue with the next

            logging.debug(f"Parsed rows count: {len(parsed_rows)}")

            pk_values = []
            valid_rows = []  # To hold rows that passed validation
            for row in parsed_rows:
                try:
                    # Validate each row based on column types
                    valid = True
                    for i, (col_name, col_type) in enumerate(column_info):
                        if "UInt" in col_type:  # Check if column is supposed to be an integer (UInt)
                            try:
                                # Try converting to int (UInt64 or other UInt type)
                                row[i] = int(row[i])  # Modify the row to hold valid data
                            except ValueError:
                                logging.error(f"Type mismatch: Cannot convert '{row[i]}' to {col_type} in column '{col_name}'. Skipping this row.")
                                logging.info(f"Invalid row (skipping): {row}")  # Log the row that failed validation
                                valid = False
                                break  # Exit validation loop early if there's a mismatch
                    if valid:
                        valid_rows.append(row)  # Only add valid rows
                    else:
                        continue  # Skip invalid rows
                except Exception as e:
                    logging.error(f"Error validating row: {row}. Skipping this row. Error: {e}")
                    logging.info(f"Error row: {row} - {e}")  # Log the row that failed due to an exception
                    continue  # Skip this row and continue with the next

            logging.debug(f"Valid rows count: {len(valid_rows)}")

            if not valid_rows:
                logging.info("✨ No valid rows to insert.")
                return

            # Extract primary key values for checking existing data
            for row in valid_rows:
                pk_value = row[pk_index]
                if isinstance(pk_value, str):
                    pk_values.append(pk_value.strip(" '\""))  # Strip quotes for strings
                else:
                    pk_values.append(str(pk_value))  # For numbers, just convert to string

            logging.debug(f"Extracted PK values sample: {pk_values[:5]}")

            # Check if the table has any existing data by fetching some rows
            existing_data_query = f"""
                SELECT {primary_key_col}
                FROM {database_name}.{table}
                LIMIT 1
            """
            existing_data = client.query(existing_data_query).result_rows

            # If no data exists, treat all rows as new records
            if not existing_data:
                logging.info("✨ No existing data found. Inserting all records as new.")
                new_data_string = ",\n".join([f"({','.join(map(str, row))})" for row in valid_rows])
                insert_in_batches(new_data_string, client, database_name, table, col_names_str)
                return

            # Fetch existing rows based on the primary key and filter those modified in the last n days or months
            existing_data_query = f"""
                SELECT {primary_key_col}, MODIFIEDDATETIME
                FROM {database_name}.{table}
                WHERE MODIFIEDDATETIME >= NOW() - INTERVAL {interval}
            """
            existing_data = client.query(existing_data_query).result_rows

            # Convert existing data to a dictionary for fast lookups
            existing_data_dict = {row[0]: row[1] for row in existing_data}

            # Prepare new and updated rows
            new_rows = []
            updated_rows = []

            for row in valid_rows:
                pk_value = row[pk_index]

                # Ensure primary key type matches existing data
                try:
                    pk_type = type(next(iter(existing_data_dict.keys())))
                    pk_value = pk_type(pk_value)
                except Exception as e:
                    print(f"⚠️ Type casting error for primary key: {e}")
                    continue

                # Determine if the row is new or updated
                if pk_value not in existing_data_dict:
                    new_rows.append(row)  # New record, needs to be inserted
                else:
                    try:
                        # Parse the MODIFIEDDATETIME value
                        modified_datetime = row[column_names.index('MODIFIEDDATETIME')].strip("'\"")
                        datetime_type = type(next(iter(existing_data_dict.values())))
                        modified_datetime = datetime.strptime(modified_datetime, '%Y-%m-%d %H:%M:%S') \
                            if datetime_type == datetime else str(modified_datetime)
                    except Exception as e:
                        print(f"⚠️ Date casting error: {e}")
                        continue

                    # Check if the row has a more recent modification time
                    if modified_datetime > existing_data_dict[pk_value]:
                        updated_rows.append(row)  # Updated record, needs to be updated

            logging.info(f"New records: {len(new_rows)}, Updated records: {len(updated_rows)}")

            if new_rows:
                new_data_string = ",\n".join([f"({','.join(map(str, row))})" for row in new_rows])
                insert_in_batches(new_data_string, client, database_name, table, col_names_str)

            if updated_rows:
                update_queries = []
                for row in updated_rows:
                    pk_value = row[pk_index]
                    set_values = ", ".join([f"{column_names[i]} = '{row[i]}'" for i in range(len(row))])
                    update_query = f"ALTER TABLE {database_name}.{table} UPDATE {set_values} WHERE {primary_key_col} = '{pk_value}'"
                    update_queries.append(update_query)

                update_in_batches(update_queries, client)

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