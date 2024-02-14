from google.cloud import storage
from google.cloud import bigquery

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name} in the bucket {bucket_name}.")

def load_json_files_to_bigquery(bucket_name, dataset_id, type_list):
    bq_client = bigquery.Client()

    for file_type in type_list:
        file_name = f'{file_type}.ndjson'
        gcs_uri = f'gs://{bucket_name}/{file_name}'

        table_id = file_type

        table_ref = bq_client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

        load_job.result()

        print(f'Loaded {load_job.output_rows} rows from {gcs_uri} into BigQuery table {dataset_id}.{table_id}')

