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
file_path = config["gcs"]["files"].get("salesline")

# Read data from GCS
df = read_gcs_file(bucket_name, file_path)
# df = pd.DataFrame(df)

# Fetch schema from ClickHouse
schema = fetch_table_schema("prod_source", "salesline")

# Transform data
df = transform_dataframe_to_schema(df, schema)
df = df.head(3)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
print(df['LINENUM'].astype(int))