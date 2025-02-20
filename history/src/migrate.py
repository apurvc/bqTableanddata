from google.cloud import storage, bigquery
import os
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
bq_project = os.getenv("BQ_PROJECT")
gcs_project = os.getenv("GCS_PROJECT")
bucket_name = os.getenv("BUCKET_NAME")
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("TABLE_ID")

def initialize_clients():
    storage_client = storage.Client(project=gcs_project)
    bigquery_client = bigquery.Client(project=bq_project)
    return storage_client, bigquery_client

def read_schema(schema_file):
    with open(schema_file, 'r') as f:
        schema_json = json.load(f)
    return [bigquery.SchemaField(**field) for field in schema_json]

def list_partitions(bucket, table_name_in_gcs):
    """
    Lists the partitions in a given bucket that match the specified partition prefix.

    Args:
        bucket (google.cloud.storage.bucket.Bucket): The Google Cloud Storage bucket object.
        table_name_in_gcs (str): The prefix to filter the blobs in the bucket.

    Returns:
        list: A sorted list of partition strings in the format 'dt=YYYYMMDD'.

    Raises:
        ValueError: If the partition date format is not 'YYYYMMDD'.

    Example:
        partitions = list_partitions(bucket, 'data/')
        print(partitions)  # Output: ['dt=20230101', 'dt=20230102', ...]

    Note:
        This function assumes that the partition format in the blob names is 'dt=YYYYMMDD'.
    """
    blobs = bucket.list_blobs(prefix=table_name_in_gcs)
    partitions = set()
    for blob in blobs:
        parts = blob.name.split('/')
        if len(parts) > 1 and parts[1].startswith('dt='):
            partitions.add(parts[1])
        else:
            logging.warning(f"Skipping blob with unexpected format: {blob.name}")
    return sorted(partitions, key=lambda x: datetime.strptime(x.split('=')[1], '%Y%m%d'))

def load_data_to_bigquery(bigquery_client, uris, dataset_id, table_id, schema):
    """
    Loads data from the specified URIs into a BigQuery table.
    Args:
        bigquery_client (google.cloud.bigquery.Client): The BigQuery client used to perform the load operation.
        uris (Union[str, List[str]]): The URIs of the data files to load. Can be a single URI or a list of URIs.
        dataset_id (str): The ID of the dataset containing the target table.
        table_id (str): The ID of the target table.
    Returns:
        None
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    load_job = bigquery_client.load_table_from_uri(
        uris,
        f"{dataset_id}.{table_id}_intermediate",
        job_config=job_config,
    )
    load_job.result()  # Wait for the job to complete

def rename_columns(rename_file):
    with open(rename_file, 'r') as f:
        rename_columns = json.load(f)
    return {col['old_name']: col['new_name'] for col in rename_columns}

def construct_query(bigquery_project, dataset_id, table_id, schema, rename_dict, formatted_date):
    """
    Constructs an SQL query to insert data into a specified table with optional column renaming and additional fields.
    Args:
        your_project (str): The project ID where the dataset and table are located.
        dataset_id (str): The dataset ID containing the target table.
        table_id (str): The target table ID where data will be inserted.
        schema (list): A list of dictionaries representing the schema of the table. Each dictionary should have a 'name' key.
        rename_dict (dict): A dictionary mapping original column names (in lowercase) to their new names.
        formatted_date (str): A date string in the format 'YYYY-MM-DD' to be added as the 'as_of_date' column.
    Returns:
        str: The constructed SQL query string.
    """
    select_columns = []
    insert_columns = []
    for field in schema:
        col_name = field.name
        if col_name.lower() in rename_dict:
            select_columns.append(f"{col_name} AS {rename_dict[col_name.lower()]}")
            insert_columns.append(rename_dict[col_name.lower()])
        else:
            select_columns.append(col_name)
            insert_columns.append(col_name)
    select_columns.append(f"parse_date('%Y-%m-%d','{formatted_date}') as as_of_date")
    insert_columns.append('as_of_date')
    select_columns.append("CURRENT_TIMESTAMP() AS _event_ts")
    insert_columns.append('_event_ts')

    select_clause = ", ".join(select_columns)
    insert_clause = ", ".join(insert_columns)

    query = f"""
    INSERT INTO `{bigquery_project}.{dataset_id}.{table_id}`
        ({insert_clause})
    SELECT
        {select_clause}
    FROM
        `{bigquery_project}.{dataset_id}.{table_id}_intermediate`
    """
    return query

def execute_query(bigquery_client, query):
    query_job = bigquery_client.query(query)
    query_job.result()  # Wait for the job to complete

def load_parquet_to_bigquery(bucket_name, dataset_id, table_id, table_name_in_gcs):
    storage_client, bigquery_client = initialize_clients()
    schema = read_schema("schema.json")
    bucket = storage_client.bucket(bucket_name)
    partitions = list_partitions(bucket, table_name_in_gcs)

    for partition in partitions:
        partition_date = partition.split('=')[1]
        formatted_date = datetime.strptime(partition_date, '%Y%m%d').strftime('%Y-%m-%d')
        blobs = bucket.list_blobs(prefix=f"{table_name_in_gcs}/{partition}/")
        uris = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]

        load_data_to_bigquery(bigquery_client, uris, dataset_id, table_id)
        rename_dict = rename_columns("rename.json")
        #logging.info(rename_dict)
        query = construct_query(bq_project, dataset_id, table_id, schema, rename_dict, formatted_date)
        #logging.info(query)
        execute_query(bigquery_client, query)
        logging.info(f"Loaded data from {partition} into {dataset_id}.{table_id} partitioned by {formatted_date}")

# Start script
if __name__ == "__main__":
    try:
        load_parquet_to_bigquery(
            bucket_name=bucket_name,
            dataset_id=dataset_id,
            table_id=table_id,
            table_name_in_gcs=table_id

    except Exception as err:
        message = f"Task failed: {str(err)}"
        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process
