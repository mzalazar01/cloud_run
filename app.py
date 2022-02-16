from datetime import datetime, timedelta
from flask import Flask, request
import requests as req
from google.cloud import storage
from utils.model import RowModel
import logging
import shutil
import gzip
import json
import csv
import os

app = Flask(__name__)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Logging definition
logging.getLogger("urllib3").propagate = False

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # or any other level
logger.addHandler(ch)


@app.route("/", methods=['GET'])
def hello_word():
    return 'Hello World'

@app.route("/is_user_reports", methods=['POST'])
def main():
    request_json = request.get_json()
    get_insert_is_data(request_json)
    return 'User reports loaded!'


def get_insert_is_data(params):
    path = os.path.join(BASE_PATH, 'utils/user_reports.json')

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


def remove_file(path):
    if os.path.exists(path):
        os.remove(path)


def get_is_data(ds, project_id, requests_file, token):

    header = {'Authorization': f"Bearer {token}"}
    
    for config in requests_file:
        for i in range(3, 0, -1):  # Checking last 3 days in ascending order
            _dict = {
                'date': (datetime.strptime(ds, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d'),
            }

            config['parameters'].update(_dict)
            
            appkey = config['parameters']['appKey']

            process_date = config['parameters']['date']

            logger.info(
                f"Reading data from appkey {appkey} and date {process_date}")


            logger.info(
                f'Getting data from IR Api: {datetime.now().strftime("%H:%M:%S")}')

            url = req.get(config['baseURL'], headers=header,
                          params=config['parameters']).json()

            #Remove current csv file

            
            # Hit API and get link to file

            logger.info(
                f'Getting reports file: {datetime.now().strftime("%H:%M:%S")}')

            r = req.get(url['urls'][0], stream=True)


            #Making paths to temp files
            csv_zipped = os.path.join(BASE_PATH, 'temp/temp.csv.gz')
            csv_unzip = os.path.join(BASE_PATH, f"temp/user_reports_{appkey}_{process_date}.csv")


            remove_file(csv_zipped) 

            # Download the content of the request into a file
            with open(csv_zipped, 'wb') as f:
                f.write(r.raw.read())

            logger.info(
                f'Unzipping and parsing data from reports file: {datetime.now().strftime("%H:%M:%S")}')
            
            #Unzipping and parsing data from reports file
            
            with gzip.open(csv_zipped, "rt", newline="") as f_in:
                rows = list(csv.DictReader(f_in, delimiter=',', quotechar="'")) #Getting list of dicts from zipped file

                #Validate incoming data
                for row in rows:
                    _ = RowModel(**row)
                
                #Creating final uncompressed and validated csv

                remove_file(csv_unzip)

                with open(csv_unzip, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

                logger.info(
                f'Uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')
                
                #Inserting final csv into bucket
                #insert_is_data(csv_unzip, project_id)

                logger.info(
                f'Finished uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')





def insert_is_data(
    csv_file,
    project_id
):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket('sybogames-analytics-dev')
    blob = bucket.blob(csv_file)  
    blob.upload_from_filename(csv_file)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))