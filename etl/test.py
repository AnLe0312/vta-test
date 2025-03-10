import requests
import pandas as pd
import io

# Replace with your authenticated (signed) URL
authenticated_url = "https://storage.googleapis.com/data-lake-vta-test/raw/CUSTINVOICEJOUR.parquet?authuser=0"

# Fetch the file content
response = requests.get(authenticated_url)

if response.status_code == 200:
    # Read Parquet directly into a DataFrame
    df = pd.read_parquet(io(response.content))
    print(df.head())  # Display first few rows
else:
    print("Failed to fetch file:", response.status_code, response.text)