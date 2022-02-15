from datetime import datetime, timedelta
from google.cloud import bigquery
from flask import Flask
import requests as req
import gzip
import json
import csv
import os

app = Flask(__name__)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

@app.route("/", methods=['GET'])
def hello_word():
    return 'Hello World'

@app.route("/is_user_reports", methods=['POST'])
def main():
    request_json = request.get_json()
    get_insert_is_data(request_json)


def get_insert_is_data(params):
    path = os.path.join(BASE_PATH, 'user_reports.json')

    token = get_bearer_token(params['secret_key'], params['refresh_token'])

    requests_file = get_json(path)

    get_is_data(params['ds'], params['project_id'], requests_file, token)


def get_bearer_token(secret_key, refresh_token):
    auth_headers = {
        "secretkey": secret_key,
        "refreshToken": refresh_token
    }
    response = req.get(
        "https://platform.ironsrc.com/partners/publisher/auth", headers=auth_headers).json()
    return response


def get_json(path):
    with open(path) as _file:
        requests_file = json.load(_file)
    return requests_file


def get_is_data(ds, project, requests_file, token):

    header = {'Authorization': f"Bearer {token}"}
    for config in requests_file:
        for i in range(3, 0, -1):  # Checking last 3 days in ascending order
            _dict = {
                'date': (datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d'),
            }

            config['parameters'].update(_dict)

            url = req.get(config['baseURL'], headers=header,
                          params=config['parameters']).json()
            # Hit API and get link to file
            r = req.get(url['urls'][0], stream=True)

            csv_path = os.join(BASE_PATH, 'test.csv.gz')

            # Download the content of the request into a file
            with open(csv_path, 'wb') as f:
                f.write(r.raw.read())

            with gzip.open(csv_path, "rt", newline="") as file:
                reader = csv.DictReader(file, delimiter=',', quotechar="'")

                rows = list(reader)
                for batch in batch_rows(rows, 10000):
                    pass
                    #insert_is_data(project_id=project, dataset_id='stg_tb', table_id='', rows=batch)


def batch_rows(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def insert_is_data(
    project_id,
    dataset_id,
    table_id,
    rows


):
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_id)
    errors = client.insert_rows(table, rows)  # Make an API request.
    if errors:
        raise Exception('Fail inserting rows!')


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))