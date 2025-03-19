import os
import sys

# Set paths
current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(os.path.dirname(current_path))
sql_folder_path = os.path.join(parent_path, "clickhouse\prod_source")
sys.path.append(parent_path)

import re
from etl.modules.db_connector import get_clickhouse_connection

def clean_sql(sql_content):
    """Cleans SQL content: removes comments and trims whitespace."""
    lines = sql_content.split("\n")
    clean_lines = [
        line for line in lines if line.strip() and not line.strip().startswith("--")
    ]
    return " ".join(clean_lines).strip()


def create_tables_from_sql_folder(sql_folder_path):
    """Reads all .sql files from a folder and creates tables in ClickHouse."""
    client = get_clickhouse_connection("prod_source")
    if not client:
        print("Failed to connect to ClickHouse.")
        return

    try:
        # Loop through all SQL files in the folder
        for file_name in os.listdir(sql_folder_path):
            if file_name.endswith(".sql"):
                sql_file_path = os.path.join(sql_folder_path, file_name)
                print(f"🚀 Processing file: {file_name}")

                # Read and clean the SQL file content
                with open(sql_file_path, "r") as f:
                    raw_sql = f.read()
                    create_table_sql = clean_sql(raw_sql)

                # Check if the cleaned SQL contains a CREATE TABLE statement
                if "create table" not in create_table_sql.lower():
                    print(f"⚠️ Skipping {file_name}: Not a valid CREATE TABLE statement.")
                    continue

                try:
                    # Execute the SQL command
                    client.command(create_table_sql)
                    print(f"✅ Table from {file_name} created successfully!")

                except Exception as e:
                    print(f"❌ Error creating table from {file_name}: {e}")
                    print("🔍 SQL Content:\n", create_table_sql)

    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")

    finally:
        client.close()


if __name__ == "__main__":
    create_tables_from_sql_folder(sql_folder_path)