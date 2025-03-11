from modules.db_connector import get_clickhouse_connection
import logging
import os
from datetime import datetime

# Ensure log directories exist
os.makedirs("logs/duplicate_log", exist_ok=True)
os.makedirs("logs/error_log", exist_ok=True)

def generate_query(query_type, schema, table, condition=None, data=None):
    """
    Generate ClickHouse queries dynamically.

    :param query_type: Type of query ('INSERT', 'DELETE')
    :param schema: Database schema name
    :param table: Table name
    :param condition: Condition for DELETE queries
    :param data: Data for INSERT queries
    :return: SQL query as a string
    """
    client = get_clickhouse_connection()

    # Get column names dynamically
    describe_query = f"DESCRIBE TABLE {schema}.{table}"
    column_names = [col[0] for col in client.execute(describe_query)]

    if not column_names:
        raise ValueError(f"Table {schema}.{table} has no columns.")

    primary_key_col = column_names[0]  # Assume first column is primary key

    col_names_str = ", ".join([f"`{col}`" for col in column_names])
    update_columns = ", ".join([f"`{col}` = new.`{col}`" for col in column_names])

    try:
        # Check for duplicates
        duplicate_check_query = f"""
        SELECT {primary_key_col}, COUNT(*) AS count
        FROM {schema}.{table}
        GROUP BY {primary_key_col}
        HAVING COUNT(*) > 1
        """
        duplicates = client.execute(duplicate_check_query)

        if duplicates:
            date = datetime.now().strftime("%Y_%m_%d")
            log_filename = f'logs/duplicate_log/{table}_{date}.log'
            logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(message)s')

            for dup in duplicates:
                logging.info(f"Duplicate ID: {dup[0]}, Count: {dup[1]}")

            print(f"Logged {len(duplicates)} duplicate IDs to {log_filename}")

        # Generate query
        if query_type == 'INSERT':
            if not data:
                raise ValueError("Data must be provided for INSERT queries.")
            query = f"""
            INSERT INTO {schema}.{table} ({col_names_str}) VALUES {data};
            OPTIMIZE TABLE {schema}.{table} FINAL;
            """
        
        elif query_type == 'DELETE':
            if not condition:
                raise ValueError("Condition must be provided for DELETE queries.")
            query = f"""
            ALTER TABLE {schema}.{table} DELETE WHERE {condition};
            OPTIMIZE TABLE {schema}.{table} FINAL;
            """
        
        else:
            raise ValueError("Invalid query type. Choose from 'INSERT' or 'DELETE'.")

        return query

    except Exception as e:
        date = datetime.now().strftime("%Y_%m_%d")
        log_filename = f'logs/error_log/error_{date}.log'
        logging.basicConfig(filename=log_filename, level=logging.ERROR, format='%(asctime)s - %(message)s')
        logging.error(str(e))
        raise Exception("Error generating query")
