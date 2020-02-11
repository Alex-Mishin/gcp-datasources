import json
from google.cloud import storage


def prefix_keys_remove_nulls(data, prefix):
    if type(data) is dict:
        out = {}
        for key, value in data.items():
            if type(value) in (dict, list):
                out[prefix + key] = prefix_keys_remove_nulls(value, prefix)
            elif value:
                out[prefix + key] = value
        return out
    elif type(data) is list:
        out = []
        for item in data:
            if type(item) in (dict, list):
                out.append(prefix_keys_remove_nulls(item, prefix))
            elif item:
                out.append(item)
        return out
    return data


def normalize_json(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    client = storage.Client()
    bucket = client.get_bucket(file['bucket'])
    blob = storage.Blob(file['name'], bucket)

    with open('file.json', 'wb') as file_ojb:
        client.download_blob_to_file(blob, file_ojb)

    with open('file.json', 'r') as file:
        data = json.load(file)

    norm_data = prefix_keys_remove_nulls(data, prefix='c_')
    out = json.dumps(norm_data)

    out_bucket = client.get_bucket('normalized_data')
    out_blob = storage.Blob(file['name'], out_bucket)
    out_blob.upload_from_string(out)
