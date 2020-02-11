import base64
import urllib3
from google.cloud import storage


def extract_url_store_json(event, context):
    """
    Triggered from a message on a Cloud Pub/Sub topic:
    Message should be a url with an API call returning a json object,
    This object will be saved in a GCS bucket

    :param event: (dict): Event payload.
    :param context: (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    http = urllib3.PoolManager()
    req = http.request('GET', pubsub_message)
    out_str = req.data.decode('utf-8')

    client = storage.Client()
    url_parts = pubsub_message.split('/')
    bucket_name = url_parts[2]
    if not client.lookup_bucket(bucket_name):
        client.create_bucket(bucket_name)
    bucket = client.bucket(bucket_name)

    blob_name = '/'.join(url_parts[3:]) + '/' + context.timestamp
    blob = bucket.blob(blob_name)
    blob.upload_from_string(out_str)
