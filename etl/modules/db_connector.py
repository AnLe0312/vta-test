from clickhouse_driver import Client
import yaml
import os
import sys

current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

def get_clickhouse_connection():
    with open(f'{parent_path}/config/config.yaml', "r") as f:
        config = yaml.safe_load(f)
    client = Client(host=config["clickhouse"]["host"],
                    user=config["clickhouse"]["user"],
                    password=config["clickhouse"]["password"],
                    database=config["clickhouse"]["database"])
    return client