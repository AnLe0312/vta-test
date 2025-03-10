import io
import pandas as pd
from extract_from_gcs import extract_from_gcs

# Define parameters
FILE_NAME = 'raw/CUSTINVOICEJOUR.parquet'  # Change this to your file name (CSV or Parquet)

# Extract data from GCS
file_content = extract_from_gcs(FILE_NAME)

# Convert content to DataFrame based on file type
if FILE_NAME.endswith(".csv"):
    df = pd.read_csv(io.BytesIO(file_content))  # Convert CSV bytes to DataFrame
elif FILE_NAME.endswith(".parquet"):
    df = pd.read_parquet(io.BytesIO(file_content))  # Convert Parquet bytes to DataFrame
else:
    raise ValueError("Unsupported file format. Only CSV and Parquet are supported.")

print(df.head())  # Display first few rows
