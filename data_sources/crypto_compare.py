import base64
import urllib3
import json
from google.cloud import storage
from google.cloud import firestore

BUCKET = 'crypto_compare'


def extract_cryptocompare(event, context):
    """
    Triggered from a Cloud Pub/Sub topic "ds_cryptocompare":
    The message is an id from data_sources collection (Firebase),
    with the configurations and state of the data source instance.

    :param event: (dict): Event payload.
    :param context: (google.cloud.functions.Context): Metadata for the event.
    """
    message = base64.b64decode(event['data']).decode('utf-8')
    db = firestore.Client()
    doc_ref = db.collection('data_sources').document(message)
    ds_dict = doc_ref.get().to_dict()

    http = urllib3.PoolManager()
    url = ds_dict['domain'] + ds_dict['endpoint'] + ds_dict['params']
    req = http.request('GET', url)
    data_dict = json.loads(req.data.decode('utf-8'))['data']

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob_name = f"{ds_dict['endpoint']}/{data_dict['TimeFrom']}_{data_dict['TimeTo']}.json"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data_dict)
