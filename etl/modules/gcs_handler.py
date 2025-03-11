from google.cloud import storage
import pandas as pd
import yaml
import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

def get_gcs_client():
    return storage.Client.from_service_account_json(f'{parent_path}/config/gcs_service_key.json')

def read_gcs_file(bucket_name, file_path):
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    if file_path.endswith(".csv"):
        return pd.read_csv(blob.open("r"))
    elif file_path.endswith(".parquet"):
        return pd.read_parquet(blob.open("rb"))
    elif file_path.endswith(".json"):
        return pd.read_json(blob.open("r"))
    else:
        raise ValueError("Unsupported file format")