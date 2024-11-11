# Hive to BigQuery DDL Converter and Sample Data Generator

This Python script converts Hive DDL statements to BigQuery DDL statements and generates sample data for the resulting BigQuery table schema.

## Features

* **Hive to BigQuery DDL Conversion:** Converts Hive CREATE TABLE statements to equivalent BigQuery CREATE OR REPLACE TABLE statements.
* **Sample Data Generation:** Generates sample data for the BigQuery table schema using the Faker library.
* **CSV Output:** Writes the generated sample data to a CSV file.

## Usage

1. **Install Dependencies:**
   ```bash
   pip install pyhive faker thrift_sasl

2. **Run the sample Script:**
   ```bash
   python hivetoBq.py sample.sql

## Output
 
| sample Hive DDL 


```sql
    CREATE EXTERNAL TABLE IF NOT EXISTS my_table (
        id INT,
        name STRING,
        created_at TIMESTAMP
    )
``` 
| Converted to BigQuery DDL:

```sql
    CREATE OR REPLACE TABLE `my_table` (
        id INT64,
        name STRING,
        created_at TIMESTAMP
    )
``` 


##Notes:
The script supports a wide range of Hive data types, including INT, STRING, TIMESTAMP, DATE, FLOAT, DOUBLE, DECIMAL, BOOLEAN, BINARY, ARRAY, MAP, and STRUCT.
The BIGQUERY_TO_FAKER dictionary maps BigQuery data types to Faker providers for generating sample data. You can customize this dictionary to use different Faker providers or generate data in a specific format.
The number of sample records generated can be adjusted using the num_records argument in the generate_sample_data function.
