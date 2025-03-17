# Project Structure
```
vta-crm/
│── src/                  
│   ├── clickhouse/        # ClickHouse scripts (e.g., prod_source, pro_dwh)
│   ├── data-lake/         # Data ingestion scripts for GCS
│   ├── data-quality/      # Data validation and monitoring
│   ├── etl/               # ETL workflows (if different from Prefect)
│── prefect/               # Prefect Cloud workflows
│   ├── flows/             # Prefect workflows (main logic)
│   ├── agents/            # Prefect agents to execute flows
│   ├── storage/           # Prefect storage configurations (GCS, Clisckhouse, etc.)
│   ├── deployments/       # Prefect Cloud deployment configurations
│── config/                # Configuration files (e.g., API keys, credentials)
│── docs/                  # Documentation
│── tests/                 # Tests for workflows
│── .gitignore
│── requirements.txt       # Python dependencies 
│── README.md
│── notebook.ipynb         # Jupyter Notebook (use for ML)
```
