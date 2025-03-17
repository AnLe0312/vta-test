import os
import urllib.parse
import pandas as pd
from google.cloud import storage
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging
import pyarrow.parquet as pq
import pyarrow as pa

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load credentials from environment variables
GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/wind2808/Downloads/windy-forge-404504-8bb02bfe64e6.json")
MSSQL_SERVER = os.getenv("MSSQL_SERVER", "db03.viettinhanh.com.vn,11433")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "VTA_DAX63_Production")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME", "ttmkhoi")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "ttmkhoi@964")
MSSQL_DRIVER = "ODBC Driver 17 for SQL Server"  # Ensure this driver is installed

# Set Google Cloud credentials (only if not set in the environment)
if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS

# Google Cloud Storage Details
BUCKET_NAME = "data-lake-vta-test"
GCS_FOLDER = "raw/"
LOCAL_FILE = "RETAILTRANSACTIONSALESTRANS.parquet"

def get_sqlalchemy_engine():
    """Create an SQLAlchemy engine using pyodbc."""
    try:
        password_encoded = urllib.parse.quote_plus(MSSQL_PASSWORD)
        driver_encoded = urllib.parse.quote_plus(MSSQL_DRIVER)

        connection_string = (
            f"mssql+pyodbc://{MSSQL_USERNAME}:{password_encoded}@"
            f"{MSSQL_SERVER}/{MSSQL_DATABASE}"
            f"?driver={driver_encoded}&ConnectTimeout=30&trustServerCertificate=yes"
        )
        return create_engine(connection_string)
    except Exception as e:
        logging.error(f"Error creating SQLAlchemy engine: {e}")
        return None

def fetch_data():
    """Fetch data from MSSQL using SQLAlchemy."""
    try:
        engine = get_sqlalchemy_engine()
        if not engine:
            return None
        
        query = "SELECT  * FROM dbo.RETAILTRANSACTIONSALESTRANS WITH (NOLOCK) WHERE CAST(TRANSDATE AS DATE) BETWEEN '2025-01-01' AND '2025-01-31'"
        with engine.connect() as conn:
            df = pd.read_sql(query, con=conn)

        if df.empty:
            logging.warning("No data fetched from SQL Server.")
            return None

        logging.info(f"Data fetched successfully! Rows: {len(df)}")
        return df
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy Error fetching data: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected Error fetching data: {e}")
        return None

def append_to_parquet(df, filename):
    """Append data to an existing Parquet file or create a new one."""
    try:
        if os.path.exists(filename):
            logging.info("Appending data to existing Parquet file.")
            existing_table = pq.read_table(filename)
            combined_table = pa.concat_tables([existing_table, pa.Table.from_pandas(df)])
            pq.write_table(combined_table, filename)
        else:
            logging.info("Creating new Parquet file.")
            df.to_parquet(filename, index=False, engine="pyarrow")

        file_size = os.path.getsize(filename) / 1024
        logging.info(f"Data written to {filename} ({file_size:.2f} KB)")
    except Exception as e:
        logging.error(f"Error appending to Parquet file: {e}")

def upload_to_gcs(local_file, bucket_name, destination_blob):
    """Upload file to Google Cloud Storage (GCS)."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_file)
        logging.info(f"File uploaded to gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        logging.error(f"Error uploading to GCS: {e}")

if __name__ == "__main__":
    df = fetch_data()
    if df is not None:
        append_to_parquet(df, LOCAL_FILE)  # Append instead of overwriting
        upload_to_gcs(LOCAL_FILE, BUCKET_NAME, f"{GCS_FOLDER}{LOCAL_FILE}")
