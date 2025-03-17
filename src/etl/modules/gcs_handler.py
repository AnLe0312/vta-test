from google.cloud import storage
from datetime import datetime
import pandas as pd
import pytz
import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(os.path.dirname(os.path.dirname(current_path)))
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
    
def get_file_last_modified_time(bucket_name, file_path):
    """
    Fetches the last modified time of a file in GCS and converts it to the desired timezone and format.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    try:
        # Fetch the metadata and check if the updated property exists
        blob.reload()  
        last_modified = blob.updated
        
        if last_modified:
            # Convert the timestamp to your desired timezone (e.g., UTC to your local timezone)
            local_tz = pytz.timezone("Asia/Ho_Chi_Minh") 
            last_modified_local = last_modified.astimezone(local_tz)
            
            # Format the timestamp
            formatted_time = last_modified_local.strftime('%Y-%m-%d %H:%M:%S')
            return formatted_time
        else:
            print(f"No last modified timestamp found for {file_path}")
            return None
    except Exception as e:
        print(f"Error retrieving file last modified time: {str(e)}")
        return None