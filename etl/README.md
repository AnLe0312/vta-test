# ETL Structure
```
etl/
│── config/                     # Configuration files
│   ├── gcs_service_key.json    # GCS service account key
│   ├── config.yaml             # General config (e.g., GCS bucket name, file paths)
│
│── modules/                    # Reusable modules (database connection, GCS handling)
│   ├── db_connector.py         # ClickHouse connection logic
│   ├── gcs_handler.py          # Functions to read GCS files into dataframes
│
│── scripts/                    # Scripts to process and load data
│   ├── process_data.py         # Main script to transform and load data into ClickHouse
│
│── logs/                       # Log files for monitoring execution
│   ├── process.log             # Log output from scripts
│
│── README.md                   # Project documentation
```
