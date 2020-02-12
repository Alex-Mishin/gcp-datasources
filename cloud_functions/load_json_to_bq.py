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

    table_name = '_'.join(event['name'].split('/')[:-1]).replace('.', '_')
    full_name = PROJECT_ID + '.' + DATASET + '.' + table_name
    client.create_table(full_name, exists_ok=True)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    uri = 'gs://' + event['bucket'] + '/' + event['name']
    load_job = client.load_table_from_uri(
        uri, dataset_ref.table(table_name), job_config=job_config
    )
    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("table_name"))
    print("Loaded {} rows.".format(destination_table.num_rows))