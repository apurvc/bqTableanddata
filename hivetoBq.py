import re
import sys
from faker import Faker
import random
import csv
import traceback

from pyhive import hive  # You might need to install this package

# Mapping from Hive data types to BigQuery data types
HIVE_TO_BIGQUERY_TYPES = {
    "TINYINT": "INT64",
    "SMALLINT": "INT64",
    "INT": "INT64",
    "BIGINT": "INT64",
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "DECIMAL": "NUMERIC",
    "STRING": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "BOOLEAN": "BOOL",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "BINARY": "BYTES",
    "ARRAY": "ARRAY",
    "MAP": "STRUCT",  # Note: This is a simplification
    "STRUCT": "STRUCT",
}

# Define a dictionary to map BigQuery data types to Faker providers
BIGQUERY_TO_FAKER = {
    "INT64": lambda: fake.random_int(),
    "FLOAT64": lambda: fake.random_number(),
    "NUMERIC": lambda: fake.random_number(digits=10),  # Adjust digits as needed
    "STRING": lambda: fake.word(),
    "BOOL": lambda: fake.boolean(),
    "TIMESTAMP": lambda: fake.date_time_this_year().isoformat(),
    "DATE": lambda: fake.date_this_year(),
    "BYTES": lambda: fake.binary(length=16),  # Adjust length as needed
}

fake = Faker()

def hive_to_bigquery_ddl(hive_ddl: str) -> str:
    """Converts a Hive DDL statement to a BigQuery DDL statement.

    Args:
        hive_ddl: The Hive DDL statement.

    Returns:
        The BigQuery DDL statement.
    """

    # Extract table name and columns from Hive DDL
    match = re.search(
        r"CREATE(?:\s*EXTERNAL)?\s*TABLE(?:\s*IF\s*NOT\s*EXISTS)?\s*(\w+(?:\.\w+)?)\s*\((.*)\)",
        hive_ddl,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError("Invalid Hive DDL format.")

    table_name = match.group(1)
    columns_str = match.group(2)

    # Process columns
    columns = []
    for column_str in columns_str.split(","):
        column_str = column_str.strip()
        column_parts = re.split(r"\s+", column_str)
        column_name = column_parts[0]
        hive_type = column_parts[1]

        # Map Hive type to BigQuery type
        bigquery_type = HIVE_TO_BIGQUERY_TYPES.get(hive_type.upper())
        if not bigquery_type:
            raise ValueError(f"Unsupported Hive type: {hive_type}")

        columns.append((column_name, bigquery_type))  # Change here

    # Construct BigQuery DDL
    bigquery_ddl = f"""
    CREATE OR REPLACE TABLE `{table_name}` (
        {',\n        '.join([f"{column_name} {bigquery_type}" for column_name, bigquery_type in columns])}
    )
    """

    return bigquery_ddl, columns  # Return columns as well

def generate_sample_data(columns: list, num_records: int = 10) -> list:
    """Generates sample data for a BigQuery table schema.

    Args:
        columns: A list of tuples, where each tuple represents a column name and data type.
        num_records: The number of sample records to generate.

    Returns:
        A list of lists, where each inner list represents a row of data.
    """

    # Generate sample data
    data = []
    for _ in range(num_records):
        row = []
        for column_name, bigquery_type in columns:
            faker_function = BIGQUERY_TO_FAKER.get(bigquery_type.upper())
            if faker_function:
                row.append(faker_function())
            else:
                row.append(None)  # Handle unsupported types with None
        data.append(row)

    return data

def write_to_csv(data: list, columns: list, filename: str = "sample_data.csv"):
    """Writes the generated data to a CSV file."""
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        if data: #check if data is not empty
            # Write header row
            writer.writerow([column[0] for column in columns])
            writer.writerows(data)

if __name__ == "__main__":
    if len(sys.argv) <= 2:
        print("I need a sql file and how many record to generate")
        print("Example Usage: python hivetoBq.py file.sql 100")
        sys.exit(1)

    hive_ddl_file = sys.argv[1]

    if len(sys.argv) == 3:
        num_records = int(sys.argv[2])
    else:
        num_records = 100

    try:
        with open(hive_ddl_file, 'r') as file:
            hive_ddl = file.read()

        bigquery_ddl, columns = hive_to_bigquery_ddl(hive_ddl)
        print(bigquery_ddl)

        # Generate and print sample data
        sample_data = generate_sample_data(columns, num_records)  # Generate 5 sample records
        write_to_csv(sample_data,columns) # Pass 'columns' to write_to_csv
        print("\nSample data generated and saved to 'sample_data.csv'")

    except FileNotFoundError:
        print(f"Error: File not found: {hive_ddl_file}")
        sys.exit(1)
    except Exception as e:
        print(traceback.print_exc())
        print(f"An error occurred: {e}")
        sys.exit(1)
