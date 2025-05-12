from google.cloud import storage, bigquery
import os
import json
from datetime import datetime
import logging
import pandas as pd
import sys
 

# Configure logging
logging.basicConfig(level=logging.INFO)

# Retrieve User-defined env vars
bq_project = os.getenv("BQ_PROJECT")
gcs_project = os.getenv("GCS_PROJECT")
bucket_name = os.getenv("BUCKET_NAME")
dataset_id = os.getenv("DATASET_ID")
trunc_flg = os.getenv("TRUNC_FLG")
mode_config = os.getenv("MODE_CONFIG")

table_ids = json.loads(os.getenv("TABLE_IDS"))
config_file = "tables_config.json"
ren_fld = os.getenv("REN_FLD", "renaming")
start_date = os.getenv("START_DATE", "2000-01-01")
validation_date = os.getenv("VALIDATION_DATE", "2024-11-01") 
 
 
def initialize_clients():
    try:
        storage_client = storage.Client(project=gcs_project)
        bigquery_client = bigquery.Client(project=bq_project)
        return storage_client, bigquery_client
    except Exception as e:
        logging.error(f"Error initializing clients: {e}", exc_info=True)
        raise


def read_schema(schema_file):
    with open(schema_file, "r") as f:
        schema_json = json.load(f)
    return [bigquery.SchemaField(**field) for field in schema_json]


def read_json(json_file):
    with open(json_file, "r") as f:
        json_file = json.load(f)
    return json_file


def list_partitions(bucket, partition_prefix):
    try:
        blobs = bucket.list_blobs(prefix=partition_prefix)
        partitions = set()
        for blob in blobs:
            parts = blob.name.split("/")
            if len(parts) > 1 and parts[1].startswith("dt="):
                partitions.add(parts[1])
            else:
                logging.warning(f"Skipping blob with unexpected format: {blob.name}")
        return sorted(
            partitions, key=lambda x: datetime.strptime(x.split("=")[1], "%Y%m%d")
        )
    except Exception as e:
        logging.error(f"Error listing partitions: {e}", exc_info=True)
        raise


def validation_check(
    bigquery_client, storage_client, dataset_id, bucket_name, partition_prefix, table_id, validation_date, start_date
):
    try:
        json_uri = f"{partition_prefix}/_validation.json"
        json_local = f"{table_id}_validation.json"
        # Get the bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(json_uri)
        # Download the file to your local machine
        blob.download_to_filename(json_local)
        
        #
        query = f"""SELECT cast(FORMAT_DATE('%Y%m%d',as_of_date) as int64) as as_of_date ,count(*) as record_count FROM `{dataset_id}.{table_id}`where 
            as_of_date>= '{start_date}'  
            and as_of_date<='{validation_date}'  group by as_of_date order by as_of_date"""   

        logging.info(f"BQ Dataframe : {query}")
        bq_df = bigquery_client.query(query).to_dataframe()
        #logging.info(f"BQ Dataframe")
        #logging.info(bq_df)####
        json_df = pd.DataFrame(read_json(json_local)["partition_statistics"])
        #logging.info(f"validation Dataframe")
        #logging.info(json_df)#####
        if bq_df.empty:
            logging.error(f"FAIL: {table_id} - BigQuery dataframe is empty.")
            raise
        else:
            merged_df = pd.merge(
                json_df,
                bq_df,
                left_on="partition_value",
                right_on="as_of_date",
                how="inner",
            )

            # Find differences between expected_count and record_count
            differences = merged_df[
                merged_df["expected_count"] != merged_df["record_count"]
            ]

            # Print the differences if any
            if not differences.empty:
                logging.error(f"FAIL: {table_id} - Differences found:")
                logging.error(differences)
            else:
                logging.info(f"SUCCESS: {table_id} - No differences found.")
 
        # Check if the file exists
        if os.path.exists(json_local):
            # Delete the file
            os.remove(json_local)
            logging.info(f"File {json_local} has been deleted.")
        else:
            logging.info(f"File {json_local} does not exist.")

    except Exception as e:
        logging.error(f"FAIL: {table_id} -Error in validation check: {e}", exc_info=True)
        raise


def create_ext_bq_table(bigquery_client, uris, dataset_id, table_id, schema):
    try:
        # Define the external table configuration with custom partitioning
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = uris
        tab_ext = f"{table_id}_ext"
        # Define the table reference
        table_ref = bigquery_client.dataset(dataset_id).table(tab_ext)

        # Create the table object with the external configuration
        table = bigquery.Table(table_ref, schema=schema)
        table.external_data_configuration = external_config

        # Create the table in BigQuery
        table = bigquery_client.create_table(table)

        logging.info(f"SUCCESS: {table_id}_ext created successfully.")

    except Exception as e:
        logging.error(f"FAIL: {table_id}_ext -Error loading data to BigQuery: {e}", exc_info=True)
        raise


def rename_columns(rename_file):
    with open(rename_file, "r") as f:
        rename_columns = pd.read_json(f)
    return rename_columns

 


def construct_query(bq_project, dataset_id, table_id, schema, rename_dict):
    select_columns = []
    insert_columns = []
    has_src_info = False
    for field in schema:
        col_name = field.name  # Use field.name instead of field['name']
        if col_name.lower() == "inserted_at":
            has_src_info = True
        if col_name.lower() == "group":
            col_name = "`group`"
        filtered_rows = rename_dict[rename_dict['old_name'] == col_name.lower()]

            # Set the value of Column in a variable
        if not filtered_rows.empty:
            #cast_op = filtered_rows.iloc[0]['cast_op']
            cast_op = filtered_rows.iloc[0].get('cast_op', None)

            new_name = filtered_rows.iloc[0]['new_name']
            if pd.isna(cast_op) or cast_op is None:
                #print("No rows found with cast_op")
                select_columns.append(f"{col_name} AS {new_name.lower()}")
                insert_columns.append(new_name.lower())
            elif cast_op == 'DROP':
                print(f"Skipping column: {new_name.lower()}")
            elif cast_op == 'ASOFDATE':
                print(f"Casting column to : {cast_op}")
                if mode_config == 'tables_config_delta.json':
                    date_cast="cast(FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d',replace( REGEXP_EXTRACT(_FILE_NAME,    r'[^/]+/[^/]+/[^/]+/[^/]+/([^/]+)') ,'dt=','') )) as date)"
                elif mode_config == 'tables_config_murex.json':
                    date_cast="cast(FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d',replace( REGEXP_EXTRACT(_FILE_NAME,    r'[^/]+/[^/]+/[^/]+/([^/]+)') ,'dt=','') )) as date)"
                else:
                    date_cast="cast(FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d',replace( REGEXP_EXTRACT(_FILE_NAME,   r'[^/]+/[^/]+/([^/]+)' ) ,'dt=','') )) as date)"
 
                select_columns.append(f"{date_cast} AS {new_name.lower()}")
                insert_columns.append(new_name.lower())
            else:
                print(f"Casting column to : {cast_op}")
                select_columns.append(f"{cast_op} AS {new_name.lower()}")
                insert_columns.append(new_name.lower())

        else:
            select_columns.append(col_name)
            insert_columns.append(col_name)

    if not has_src_info:
        select_columns.append("CURRENT_TIMESTAMP() AS _ingestion_ts")
        insert_columns.append("_ingestion_ts")
        select_columns.append("_FILE_NAME AS _src_filename")
        insert_columns.append("_src_filename")
    if mode_config == 'tables_config_delta.json':
         select_columns.append("cast(FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d',replace( REGEXP_EXTRACT(_FILE_NAME,    r'[^/]+/[^/]+/[^/]+/[^/]+/([^/]+)') ,'dt=','') )) as date) as as_of_date")
    elif mode_config == 'tables_config_murex.json':
         select_columns.append("cast(FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d',replace( REGEXP_EXTRACT(_FILE_NAME,    r'[^/]+/[^/]+/[^/]+/([^/]+)') ,'dt=','') )) as date) as as_of_date")
    else:
        select_columns.append("cast(FORMAT_TIMESTAMP('%Y-%m-%d', PARSE_TIMESTAMP('%Y%m%d',replace( REGEXP_EXTRACT(_FILE_NAME,   r'[^/]+/[^/]+/([^/]+)' ) ,'dt=','') )) as date) as as_of_date")
    insert_columns.append("as_of_date")
    select_columns.append("CURRENT_TIMESTAMP() AS _event_ts")
    insert_columns.append("_event_ts")

    select_clause = ", ".join(select_columns)
    insert_clause = ", ".join(insert_columns)
    
    
    union_all_queries = f"""
    SELECT * FROM (SELECT
        {select_clause}
    FROM
        `{bq_project}.{dataset_id}.{table_id}_ext` where _FILE_NAME not like '%dt=0914%'  ) as STG
            WHERE as_of_date>= '{start_date}'  
            and as_of_date<='{validation_date}'
    """
    query = f"""
    INSERT INTO `{bq_project}.{dataset_id}.{table_id}`
        ({insert_clause})
    {union_all_queries}
    """
    return query


def execute_query(bigquery_client, query):
    query_job = bigquery_client.query(query)
    query_job.result()  # Wait for the job to complete


def load_parquet_to_bigquery(bucket_name, dataset_id, table_id, partition_prefix, validation_date,trunc_flg):
    try:
        storage_client, bigquery_client = initialize_clients()
        schema = read_schema(f"parquet/{table_id}_schema.json")

        uris = f"gs://{bucket_name}/{partition_prefix}/*.parquet"
        query = f"drop table if exists {dataset_id}.{table_id}_ext"
        execute_query(bigquery_client, query)
        create_ext_bq_table(bigquery_client, uris, dataset_id, table_id, schema)
        rename_dict = rename_columns(f"{ren_fld}/{table_id}_rename.json")
         
        if trunc_flg == 'Y':
            trunc_sql=f"""delete from `{bq_project}.{dataset_id}.{table_id}` where as_of_date>= '{start_date}' and as_of_date<='{validation_date}'  """
            logging.info(trunc_sql)
            execute_query(bigquery_client, trunc_sql)
        query = construct_query(bq_project, dataset_id, table_id, schema, rename_dict)
        logging.info(query)
        execute_query(bigquery_client, query)
        logging.info(f"SUCCESS: {table_id} -Loaded data from  into {dataset_id}.{table_id}")
        validation_check(
            bigquery_client,
            storage_client,
            dataset_id,
            bucket_name,
            partition_prefix,
            table_id,
            validation_date,
            start_date
        )
        # Drop the external table after validation check
        query = f"DROP TABLE `{dataset_id}.{table_id}_ext`"
        execute_query(bigquery_client, query)
        logging.info(f"External table {dataset_id}.{table_id}_ext dropped successfully.")        
    except Exception as e:
        logging.error(f"FAIL: {table_id} -Error in load_parquet_to_bigquery: {e}")
        raise

with open(f"{mode_config}", 'r') as config_file:
    config = json.load(config_file)

# Start script
if __name__ == "__main__":
    try:
        for table_id in table_ids:
            partition_prefix = config['prefixes'].get(table_id, 'Prefix not found')
            load_parquet_to_bigquery(
                bucket_name=bucket_name,
                dataset_id=dataset_id,
                table_id=table_id,
                partition_prefix=partition_prefix,
                validation_date=validation_date,
                trunc_flg=trunc_flg
            )
    except Exception as err:
        message = f"Task failed: {str(err)}"

        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process
