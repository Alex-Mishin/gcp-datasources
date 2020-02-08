import base64
import urllib3
from google.cloud import storage


def extract_and_store(event, context):
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

    storage_client = storage.Client()
    bucket = storage_client.bucket('api_data_extracted')
    blob_name = pubsub_message.split('/')[2] + '/' + context.timestamp + '.json'
    blob = bucket.blob(blob_name)
    blob.upload_from_string(out_str)
