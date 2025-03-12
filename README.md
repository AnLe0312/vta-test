# Project Structure
```
📦 VTA-TEST
│── 📂 data-lake/                # Raw data stored in GCS
│   ├── 📂 raw/                   # Unprocessed data
│   ├── 📂 processed/             # Processed and cleaned data
│   ├── 📂 archive/               # Old/historical data
│
│── 📂 clickhouse/                # ClickHouse database & schemas
│   ├── 📜 ddl/                   # DDL (schema & table creation)
│   │   ├── create_tables.sql
│   │   ├── create_views.sql
│   │
│   ├── 📜 dml/                   # DML (data operations)
│   │   ├── insert_data.sql
│   │   ├── update_data.sql
│   │   ├── delete_data.sql
│   │
│   ├── 📜 queries/               # ClickHouse analytical queries
│   │   ├── reports.sql
│   │   ├── performance_tests.sql
│   │   ├── ad-hoc_queries.sql
│
│── 📂 etl/                       # ETL (Extract, Transform, Load) scripts
│   │── 📂 config/
│   │── 📂 modules/
│   │── 📂 scripts/
│   │── 📂 logs/
│
│── 📂 prefect/                   # Apache Prefect DAGs for automation
│   ├── gcs_to_clickhouse_dag.py
│   ├── clickhouse_dwh_dag.py
│
│── 📂 scripts/                   # Utility scripts for automation
│   ├── setup_env.sh              # Shell script to set up environment
│   ├── test_queries.py           # Run test queries on ClickHouse
│   ├── backup_data.py            # Backup ClickHouse tables
│
│
│── 📂 docs/                      # Documentation
│   ├── README.md                 # Project overview
│   ├── setup_guide.md            # Installation and setup steps
│   ├── data_pipeline.md          # Explanation of ETL process
│
│── .gitignore                    # Ignore logs, virtual environments, and temp files
│── requirements.txt               # Python dependencies (ClickHouse driver, GCS SDK, etc.)
```
