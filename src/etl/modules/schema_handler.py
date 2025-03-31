import pandas as pd
import numpy as np
import math
import os
import sys 

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

from modules.db_connector import get_clickhouse_connection
from datetime import datetime


def fetch_table_last_synced_at(database_name, table_name):
    """
    Fetches the 'last_synced_at' timestamp from the given table in ClickHouse.
    You may need to adjust this query depending on how the 'last_synced_at' is stored.
    """
    query = f"""
    SELECT MAX(last_synced_at)
    FROM {database_name}.{table_name}
    """
    conn = get_clickhouse_connection(database_name)
    result = conn.query(query).result_rows
    return result[0][0] if result else None


# Fetch ClickHouse table schema
def fetch_table_schema(database_name, table_name):
    client = get_clickhouse_connection(database_name)
    query = f"DESCRIBE TABLE {database_name}.{table_name}"
    schema = client.query(query)
    schema_data = schema.result_rows

    # Return a dictionary with column name as key, and type, nullable, default_type, and default_expression as values
    return {
        col[0]: {
            "type": col[1],
            "nullable": "Nullable" in col[1],
            "default_type": col[2], 
            "default_expression": col[3] 
        }
        for col in schema_data
    }


# Transform DataFrame columns to match ClickHouse schema
def transform_dataframe_to_schema(df, schema, logger=None):
    # Handle Date and DateTime columns to replace years in the 1900s (19xx) with NaT
    missing_columns = []

    for col in df.columns:
        if col not in schema:
            logger.warning(f"Column '{col}' is missing from the ClickHouse schema and will be skipped.")
            missing_columns.append(col)
            continue  # Skip the column if it's missing from the schema
        # If the column is Date or DateTime, replace dates with years in the 1900s (19xx) with None
        if df[col].dtype in ["datetime64[ns]", "datetime64[ns, UTC]"]:
            mask = df[col].dt.year < 2000
            df[col] = df[col].astype("object")  # Ensure object type for mixed values
            df.loc[mask, col] = None

    # Remove missing columns from df
    if missing_columns:
        df = df.drop(columns=missing_columns)
        if logger:
            logger.info(f"Dropped {len(missing_columns)} column(s) not in schema: {missing_columns}")

    new_columns = {}
    # Transform DataFrame columns to match ClickHouse schema
    for col, props in schema.items():
        dtype = props["type"]
        nullable = props["nullable"]
        default_type = props["default_type"]
        default_expression = props["default_expression"]

        # Handle missing columns based on type and default behavior
        if col not in df.columns:
            if default_type == "DEFAULT" and default_expression:
                # Handle DateTime type (e.g., 'now()')
                if "DateTime" in dtype:
                    if "now()" in default_expression:
                        new_columns[col] = [pd.to_datetime(datetime.now())] * len(df) # Set current timestamp for DateTime
                    else:
                        new_columns[col] = [np.nan] * len(df) # For other DateTime expressions, we handle as NaN for now
                # Handle numeric types (Int32, Float64, etc.)
                elif "Int" in dtype or "Float" in dtype:
                    try:
                        # Try to evaluate the default expression as a number
                        new_columns[col] = [eval(default_expression)] * len(df)
                    except:
                        # If evaluation fails, set NaN or 0 based on nullability
                        new_columns[col] = [np.nan if nullable else 0] * len(df)
                # Handle string types (String, etc.)
                elif "String" in dtype:
                    new_columns[col] = [str(default_expression)] * len(df) # Use the expression as a string
                else:
                    # Handle any other types based on the expression
                    new_columns[col] = [np.nan if nullable else 0] * len(df)
            else:
                # For other missing columns, assign NaN if nullable or 0 if not nullable
                new_columns[col] = [np.nan if nullable else 0] * len(df)

            if logger:
                logger.info(f"Added missing column '{col}' with default or null values.")
            continue  

        # Skip the column if it was initialized in new_columns (since it doesn't exist in df)
        if col not in df.columns:
            continue

        # Transform columns to match schema types
        if dtype == "String":
            df[col] = df[col].astype(str).fillna('')
        elif dtype in ["Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        elif dtype in ["Float32", "Float64"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        elif dtype == "Boolean":
            df[col] = df[col].astype('bool', errors='ignore').fillna(False)
        elif dtype in ["Enum8", "Enum16", "UUID"]:
            df[col] = df[col].astype(str).fillna('unknown')
        elif dtype == "Date":
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            df[col] = df[col].where(df[col].notnull(), None)
        elif dtype == "DateTime":
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].where(df[col].notnull(), None)

        # Handle nullable fields properly
        if nullable:
            df[col] = df[col].where(pd.notnull(df[col]), None)

    if new_columns:
        df = pd.concat([df, pd.DataFrame(new_columns)], axis=1)

    # Ensure the column order matches the schema
    ordered_columns = [col for col in schema.keys() if col in df.columns]
    df = df[ordered_columns]

    if logger:
        logger.info(f"Final DataFrame shape after schema transformation: {df.shape}")

    return df


# Format values to match ClickHouse syntax.
def format_value(value):
    if isinstance(value, pd.Timestamp):
        # Format Timestamp to 'YYYY-MM-DD HH:MM:SS'
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif value is None or (isinstance(value, float) and (math.isnan(value) or np.isinf(value))):
        # Ensure NULL is passed correctly
        return 'NULL'
    elif isinstance(value, bool):
        # Handle booleans properly (0 or 1)
        return '1' if value else '0'
    elif isinstance(value, (int, np.integer)):
        # Ensure integers are clean
        return str(int(value))
    elif isinstance(value, (float, np.float64, np.float32)):
        # Ensure floats have a dot and at least one decimal digit (ClickHouse syntax)
        return f"{float(value):.6f}"
    elif isinstance(value, str):
        # Escape single quotes in strings and handle special characters
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"
    else:
        # Catch-all for unknown types
        return 'NULL'


# Convert DataFrame into ClickHouse VALUES string.
def prepare_sql_data(df):
    """
    Handles NULLs, types, and formatting issues.
    """
    sql_rows = []
    for index, row in df.iterrows():
        try:
            formatted_row = f"({','.join(map(format_value, row))})"
            sql_rows.append(formatted_row)
        except Exception as e:
            print(f"❗ Error formatting row {index}: {e}")
            continue

    if not sql_rows:
        raise ValueError("No rows were successfully formatted for insertion.")

    return ',\n'.join(sql_rows)


def get_max_modified_datetime_from_schema(database_name, table_name):
    """Get MAX(MODIFIEDDATETIME) from ClickHouse table to track ETL status."""
    query = f"""
        SELECT MAX(MODIFIEDDATETIME)
        FROM {database_name}.{table_name}
    """
    conn = get_clickhouse_connection(database_name)
    result = conn.query(query).result_rows
    return result[0][0] if result else None


def update_last_synced_at_in_schema(database_name, table_name, new_last_synced_at, mod_min, mod_max, logger=None):
    """Update the last_synced_at column in the ClickHouse table."""
    if not new_last_synced_at:
        logger.warning("No new last_synced_at value to update.")
        return

    query = f"""
        ALTER TABLE {database_name}.{table_name}
        UPDATE last_synced_at = %(new_modified)s
        WHERE MODIFIEDDATETIME BETWEEN %(mod_min)s AND %(mod_max)s
    """
    params = {  'new_modified': new_last_synced_at, 
                'mod_min': mod_min,
                'mod_max': mod_max }

    conn = get_clickhouse_connection(database_name)
    conn.command(query, parameters=params)
    
    logger.info(f"Updated last_synced_at = {new_last_synced_at} for rows where MODIFIEDDATETIME BETWEEN {mod_min} AND {mod_max} in table {database_name}.{table_name}.")