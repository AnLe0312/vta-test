# This code use for Ingesting data from ax mssql to google storage /raw

import os
import urllib.parse
import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/wind2808/Downloads/windy-forge-404504-8bb02bfe64e6.json"

# MSSQL Configuration
MSSQL_SERVER = "db03.viettinhanh.com.vn,11433"
MSSQL_DATABASE = "VTA_DAX63_Production"
MSSQL_USERNAME = "ttmkhoi"
MSSQL_PASSWORD = "ttmkhoi@964"
MSSQL_DRIVER = "ODBC Driver 17 for SQL Server"  # Ensure this driver is installed

# Google Cloud Storage Details
BUCKET_NAME = "data-lake-vta-test"
GCS_FOLDER = "raw/"
LOCAL_FILE = "VTAPRODPRODUCTCLASSHISTORY.parquet"

# MSSQL Table Configuration
#TABLE_NAME = "RETAILTRANSACTIONSALESTRANS"

def get_sqlalchemy_engine():
    """Create an SQLAlchemy engine using pyodbc."""
    try:
        password_encoded = urllib.parse.quote_plus(MSSQL_PASSWORD)
        driver_encoded = urllib.parse.quote_plus(MSSQL_DRIVER)

        connection_string = (
            f"mssql+pyodbc://{MSSQL_USERNAME}:{password_encoded}@"
            f"{MSSQL_SERVER}/{MSSQL_DATABASE}"
            f"?driver={driver_encoded}"
            f"&ConnectTimeout=30"
            f"&trustServerCertificate=yes"
        )
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"❌ Error creating SQLAlchemy engine: {e}")
        return None

def fetch_data():
    """Fetch data from MSSQL using SQLAlchemy."""
    try:
        engine = get_sqlalchemy_engine()
        if not engine:
            return None
        
        query = f"SELECT * FROM dbo.VTAPRODPRODUCTCLASSHISTORY WITH (NOLOCK)"
        with engine.connect() as conn:
            df = pd.read_sql(query, con=conn)

        if df.empty:
            print("⚠️ No data fetched from SQL Server.")
            return None

        print(f"✅ Data fetched successfully! Rows: {len(df)}")
        return df
    except SQLAlchemyError as e:
        print(f"❌ SQLAlchemy Error fetching data: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected Error fetching data: {e}")
        return None

def save_to_parquet(df, filename):
    """Save DataFrame to a Parquet file."""
    try:
        df.to_parquet(filename, index=False)
        print(f"✅ Data saved to {filename} ({os.path.getsize(filename) / 1024:.2f} KB)")
    except Exception as e:
        print(f"❌ Error saving to Parquet: {e}")

def upload_to_gcs(local_file, bucket_name, destination_blob):
    """Upload file to Google Cloud Storage (GCS)."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_file)
        print(f"✅ File uploaded to gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        print(f"❌ Error uploading to GCS: {e}")

if __name__ == "__main__":
    df = fetch_data()
    if df is not None:
        save_to_parquet(df, LOCAL_FILE)
        upload_to_gcs(LOCAL_FILE, BUCKET_NAME, f"{GCS_FOLDER}{LOCAL_FILE}")
