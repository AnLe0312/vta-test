import sys
from google.cloud import storage

def extract_from_gcs(file_name):
    storage_client = storage.Client(project='windy-forge-404504')
    bucket = storage_client.bucket('data-lake-vta-test')
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()
    return content

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python extract_from_gcs.py data-lake-vta-test <file_name>")
        sys.exit(1)

    file_name = sys.argv[2]

    data = extract_from_gcs(file_name)
    with open(file_name, "wb") as f:
        f.write(data)

    print(f"Downloaded {file_name} from GCS successfully!")
