import pandas as pd
import logging
import re
from modules.db_connector import get_clickhouse_connection

# Fetch ClickHouse table schema
def fetch_table_schema(database_name, table_name):
    client = get_clickhouse_connection(database_name)
    query = f"DESCRIBE TABLE {database_name}.{table_name}"
    schema = client.execute(query)

    return {col[0]: {"type": col[1], "nullable": "Nullable" in col[1]} for col in schema}


# Validate DataFrame against schema
def validate_data(df, schema):
    errors = []
    for col, props in schema.items():
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
        else:
            dtype = props['type']
            nullable = props['nullable']

            # Check data type compatibility
            if dtype in ["Int32", "Int64", "UInt8", "UInt64"]:
                if not pd.api.types.is_integer_dtype(df[col]):
                    errors.append(f"Column {col} has incorrect data type. Expected: {dtype}")
            elif dtype in ["Float32", "Float64", "Decimal"]:
                if not pd.api.types.is_float_dtype(df[col]):
                    errors.append(f"Column {col} has incorrect data type. Expected: {dtype}")
            elif dtype == "Date" or dtype == "DateTime":
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    errors.append(f"Column {col} has incorrect data type. Expected: {dtype}")
            elif dtype == "String" and not pd.api.types.is_string_dtype(df[col]):
                errors.append(f"Column {col} has incorrect data type. Expected: String")
            
            # Check for null values in non-nullable columns
            if not nullable and df[col].isnull().any():
                errors.append(f"Non-nullable column {col} contains null values")
                
    return errors


# Transform DataFrame columns to match ClickHouse schema
def transform_dataframe_to_schema(df, schema):
    for col, props in schema.items():
        dtype = props["type"]
        nullable = props["nullable"]

        # Handle missing columns by adding defaults or nulls
        if col not in df.columns:
            logging.warning(f"Column {col} missing in data. Filling with None")
            df[col] = None if nullable else 0

        # Transform columns to match schema types
        if dtype == "String":
            df[col] = df[col].astype(str).fillna("")
        elif dtype in ["Int32", "Int64", "UInt8", "UInt64"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        elif dtype in ["Float32", "Float64"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)
        elif "Decimal" in dtype:
            precision, scale = map(int, re.findall(r"\d+", dtype))
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).round(scale)
        elif dtype == "Date":
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date.fillna(pd.NaT)
        elif dtype == "DateTime":
            df[col] = pd.to_datetime(df[col], errors='coerce').fillna(pd.NaT)

        # Handle nullable fields properly
        if nullable:
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    return df


def prepare_data_for_load(df, schema):
    # Validate DataFrame
    errors = validate_data(df, schema)
    if errors:
        logging.error("Data validation failed with errors:")
        for error in errors:
            logging.error(f"  - {error}")
        raise ValueError("Data validation failed. Check logs for details.")

    # Transform DataFrame to match schema
    df_transformed = transform_dataframe_to_schema(df, schema)
    return df_transformed