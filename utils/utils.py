from google.cloud import storage
import requests as req
import os

def get_bearer_token(secret_key, refresh_token):
    auth_headers = {
        "secretkey": secret_key,
        "refreshToken": refresh_token
    }

    response = req.get(
        "https://platform.ironsrc.com/partners/publisher/auth", headers=auth_headers).json()

    return response

def remove_file(path):
    if os.path.exists(path):
        os.remove(path)

def explode(dict_list):
    unnested = []
    for _dict in dict_list:
        print(_dict, type(_dict))
        for key in _dict['data']:
            _iter = _dict.copy()
            _iter.update(key)
            del _iter['data']
            unnested.append(
                {key: value for key, value in _iter.items() if key != 'offerCompletions'})
    return unnested

def insert_file_to_bucket(
    _file,
    blob_path,
    project_id,
    bucket_name
):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path) 
    blob.upload_from_file(_file)

