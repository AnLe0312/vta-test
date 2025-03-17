import pandas as pd
import numpy as np
import re
import yaml
import os
import sys 
import math
import time

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

from modules.db_connector import get_clickhouse_connection
from modules.gcs_handler import read_gcs_file
from modules.schema_handler import fetch_table_schema, transform_dataframe_to_schema, prepare_sql_data
from modules.generate_query import generate_query, get_primary_key_col_from_yaml, parse_values_row
from datetime import datetime
from requests.exceptions import Timeout

client = get_clickhouse_connection("prod_source")

# Load configuration
with open(f'{parent_path}/models/storage_models.yaml', "r") as f:
      config = yaml.safe_load(f)

bucket_name = config["gcs"]["bucket_name"]
file_path = config["gcs"]["files"].get("custinvoicejour")

# Read data from GCS
df = read_gcs_file(bucket_name, file_path)
# df = pd.DataFrame(df)

# Fetch schema from ClickHouse
schema = fetch_table_schema("prod_source", "custinvoicejour")

# Transform data
df = transform_dataframe_to_schema(df, schema)
df = df.head(3)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
# print(df[df["RECID"] == 5644714290][["COVSTATUS"]])
# df = df[df["RECID"] == 5644714290]
# for index, row in df.iterrows():
#       print(row)
# print(len(df.columns))

# Convert to ClickHouse VALUES string
data = prepare_sql_data(df)

database_name = "prod_source"
table_name = "custinvoicejour"

# Generate the INSERT query dynamically
# query = generate_query(query_type="INSERT", database_name=database_name, table=table_name, data=data)

# Take the first batch for testing (optional)
# first_tuple = data.split("\n")[0]

describe_query = f"DESCRIBE TABLE {database_name}.{table_name}"
query_result = client.query(describe_query)
column_names = [col[0] for col in query_result.result_rows]
column_info = [(col[0], col[1]) for col in query_result.result_rows]  # Column name and type
column_names = [col[0] for col in column_info]

if not column_names:
      raise ValueError(f"Table {database_name}.{table_name} has no columns.")

primary_key_col = get_primary_key_col_from_yaml(database_name, table_name)

col_names_str = ", ".join(column_names)

data_rows = [row.strip() for row in re.split(r"\),\s*\(", data.strip("()")) if row.strip()]

parsed_rows = []
for row in data_rows:
      parsed_row = parse_values_row(row)
      parsed_rows.append(parsed_row)

pk_index = column_names.index(primary_key_col)
# pk_values = [parse_values_row(row)[pk_index].strip(" '\"") for row in data_rows]

# batch_size = 3  # You can adjust this based on your query size limits
# existing_data = []
# for start in range(0, len(pk_values), batch_size):
#       batch_pk_values = pk_values[start:start + batch_size]
#       existing_data_query = f"""
#             SELECT *
#             FROM {database_name}.{table_name}
#             WHERE {primary_key_col} IN ('{"', '".join(batch_pk_values)}')
#       """
#       existing_data += client.query(existing_data_query).result_rows

# # Now you can compare existing_data as before
# existing_hash_set = {hash(tuple(row)) for row in existing_data}

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
                    valid = False
                    break  # Exit validation loop early if there's a mismatch
        if valid:
            valid_rows.append(row)  # Only add valid rows
        else:
            continue  # Skip invalid rows
    except Exception as e:
        continue  # Skip this row and continue with the next

# Extract primary key values for checking existing data
for row in valid_rows:
    pk_value = row[pk_index]
    if isinstance(pk_value, str):
        pk_values.append(pk_value.strip(" '\""))  # Strip quotes for strings
    else:
        pk_values.append(str(pk_value))  # For numbers, just convert to string
        
# Query to fetch existing data from the last 12 days
existing_data_query = f"""
    SELECT {primary_key_col}, MODIFIEDDATETIME
    FROM {database_name}.{table_name}
    WHERE MODIFIEDDATETIME >= NOW() - INTERVAL 12 DAY
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
print(type(pk_value))
    # # Determine if the row is new or updated
    # if pk_value not in existing_data_dict:
    #     new_rows.append(row)  # New record, needs to be inserted
    # else:
    #     try:
    #         # Parse the MODIFIEDDATETIME value
    #         modified_datetime = row[column_names.index('MODIFIEDDATETIME')].strip("'\"")
    #         datetime_type = type(next(iter(existing_data_dict.values())))
    #         modified_datetime = datetime.strptime(modified_datetime, '%Y-%m-%d %H:%M:%S') \
    #             if datetime_type == datetime else str(modified_datetime)
    #     except Exception as e:
    #         print(f"⚠️ Date casting error: {e}")
    #         continue

    #     # Check if the row has a more recent modification time
    #     if modified_datetime > existing_data_dict[pk_value]:
    #         updated_rows.append(row)  # Updated record, needs to be updated