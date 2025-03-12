import os
import sys
import yaml
import clickhouse_connect

# Set up paths
current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

# Ensure no proxy interferes with the connection
os.environ["NO_PROXY"] = "mijcgdzo2n.us-central1.gcp.clickhouse.cloud"

def get_clickhouse_connection(database_name=None):
    try:
        # Load config
        with open(f'{parent_path}/config/clickhouse_config.yaml', "r") as f:
            config = yaml.safe_load(f)
        
        # Use the provided database_name or fallback to config file
        db_name = database_name if database_name else config["clickhouse"].get("database", "default")

        # Connect to ClickHouse
        client = clickhouse_connect.get_client(
            host=config["clickhouse"]["host"],
            port=config["clickhouse"]["port"],
            user=config["clickhouse"]["user"],
            password=config["clickhouse"]["password"],
            database=db_name,
            secure=True
        )

        # Check if the database exists
        result = client.query(f"SHOW DATABASES LIKE '{db_name}'").result_set
        if not result:
            raise ValueError(f"Database '{db_name}' does not exist in ClickHouse!")

        return client

    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        return None  # Return None if the connection fails
