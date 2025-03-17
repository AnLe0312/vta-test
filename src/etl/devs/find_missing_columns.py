import pandas as pd

def find_missing_columns_with_neighbors(df, schema_columns):
    """
    Find missing columns in schema and identify the previous and next columns for each missing column.

    Args:
        df (pd.DataFrame): Input DataFrame.
        schema_columns (list): List of columns in the schema.

    Returns:
        list: A list of missing columns with their previous and next columns.
    """
    # Find columns that are in the DataFrame but not in the schema
    missing_columns = [col for col in df.columns if col not in schema_columns]

    # Store results as a list of dictionaries
    results = []

    for missing_col in missing_columns:
        # Get the index of the missing column in the DataFrame
        idx = list(df.columns).index(missing_col)

        # Determine the previous and next columns (if any)
        prev_col = df.columns[idx - 1] if idx > 0 else None
        next_col = df.columns[idx + 1] if idx < len(df.columns) - 1 else None

        # Save the results as a dictionary
        results.append({
            'missing_column': missing_col,
            'prev_column': prev_col,
            'next_column': next_col
        })

    return results


# ============================
# 🎯 Test the function with an example
# ============================

# Simulate a DataFrame
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'Country': ['USA', 'USA', 'USA'],  # Extra column
    'City': ['New York', 'Los Angeles', 'Chicago'],
    'Email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']  # Another extra column
}
df = pd.DataFrame(data)

# Simulate the current schema
schema_columns = ['Name', 'Age', 'City']

# Run the function and get the results
missing_info = find_missing_columns_with_neighbors(df, schema_columns)

# Display the results
for info in missing_info:
    print(f"Missing column: {info['missing_column']}")
    print(f"  ➜ Previous column: {info['prev_column']}")
    print(f"  ➜ Next column: {info['next_column']}")
    print("-" * 30)
