from google.cloud import bigquery

PROJECT_ID = 'cypherkin'
DATASET = 'staging_data'


def load_json_to_bq(event, context):
    """
    :param event: (dict): Event payload
    :param context: (google.cloud.functions.Context): Metadata for the event.
    """
    client = bigquery.Client(PROJECT_ID)
    dataset_ref = bigquery.dataset.DatasetReference(PROJECT_ID, DATASET)

    table_name = event['name'].split('/')[0].replace('.', '_')
    full_name = PROJECT_ID + '.' + DATASET + '.' + table_name
    table_ref = client.create_table(full_name, exists_ok=True)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    uri = 'gs://' + event['bucket'] + '/' + event['name']
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )
    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")