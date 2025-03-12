from modules.db_connector import get_clickhouse_connection
import logging
import os
from datetime import datetime
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

# Setup path
os.makedirs(f'{parent_path}/logs/duplicate_log', exist_ok=True)
os.makedirs(f'{parent_path}/logs/error_log', exist_ok=True)

def generate_query(query_type, database_name, table, condition=None, data=None):
    """
    Generate ClickHouse queries dynamically.
    :param query_type: Type of query ('INSERT', 'DELETE')
    :param database_name: Database schema name
    :param table: Table name
    :param condition: Condition for DELETE queries
    :param data: Data for INSERT queries
    :return: SQL query as a string
    """
    client = get_clickhouse_connection(database_name)

    # Get column names dynamically
    describe_query = f"DESCRIBE TABLE {database_name}.{table}"
    column_names = [col[0] for col in client.execute(describe_query)]

    if not column_names:
        raise ValueError(f"Table {database_name}.{table} has no columns.")

    primary_key_col = "RECID" if "RECID" in column_names else column_names[0]  # Default to first column if no RECID

    col_names_str = ", ".join([f"`{col}`" for col in column_names])

    try:
        # Generate and run INSERT query
        if query_type == 'INSERT':
            if not data:
                raise ValueError("Data must be provided for INSERT queries.")
            
            insert_query = f"""
            INSERT INTO {database_name}.{table} ({col_names_str}) VALUES {data};
            """
            client.execute(insert_query)

            # Check for duplicates immediately after INSERT
            duplicate_check_query = f"""
            SELECT *
            FROM {database_name}.{table}
            WHERE {primary_key_col} IN (
                SELECT {primary_key_col}
                FROM {database_name}.{table}
                GROUP BY {primary_key_col}
                HAVING COUNT(*) > 1
            )
            """
            duplicates = client.execute(duplicate_check_query)

            # Log duplicates if any are found
            if duplicates:
                date = datetime.now().strftime("%Y_%m_%d")
                log_filename = f'{parent_path}/logs/duplicate_log/dup_{table}_{date}.log'
                logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

                for dup_row in duplicates:
                    logging.info(f"Duplicate row: {dup_row}")

                print(f"Logged {len(duplicates)} duplicate rows to {log_filename}")

            # Run OPTIMIZE TABLE to clean up duplicates if possible
            optimize_query = f"OPTIMIZE TABLE {database_name}.{table} FINAL;"
            client.execute(optimize_query)

        # Generate DELETE query
        elif query_type == 'DELETE':
            if not condition:
                raise ValueError("Condition must be provided for DELETE queries.")
            delete_query = f"""
            ALTER TABLE {database_name}.{table} DELETE WHERE {condition};
            """
            client.execute(delete_query)

            # Run OPTIMIZE after DELETE too
            optimize_query = f"OPTIMIZE TABLE {database_name}.{table} FINAL;"
            client.execute(optimize_query)

        else:
            raise ValueError("Invalid query type. Choose from 'INSERT' or 'DELETE'.")

    except Exception as e:
        # Log any errors
        date = datetime.now().strftime("%Y_%m_%d")
        log_filename = f'{parent_path}/logs/error_log/error_{table}_{date}.log'
        logging.basicConfig(filename=log_filename, level=logging.ERROR, format='%(asctime)s - %(message)s')
        logging.error(str(e))
        raise Exception("Error generating query")