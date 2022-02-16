from datetime import datetime, timedelta
from fastapi import FastAPI, Body
from google.cloud import storage
from utils.model import RequestModel
import requests as req
import logging
import shutil
import gzip
import json
import os

app = FastAPI()

#Setting path to create files in the correct directory
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Logging definition
logging.getLogger("urllib3").propagate = False

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # or any other level
logger.addHandler(ch)


@app.get("/",status_code=200)
async def hello_word():
    return {'message':'Hello World'}

@app.post("/is_user_reports/", status_code=201)
async def main(request: RequestModel):
    path = os.path.join(BASE_PATH, 'utils/user_reports.json')

    params = request.dict()
    
    token = get_bearer_token(params['secret_key'], params['refresh_token'])

    requests_file = get_json(path)

    get_is_data(params['ds'], params['project_id'], params['bucket_name'], requests_file, token)

    return {'message': f"Reports loaded successfully on bucket {params['bucket_name']} from project {params['project_id']}"}

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


def remove_file(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def get_is_data(ds, project_id, bucket_name, requests_file, token):

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
            
            # Hit API and get link to file

            logger.info(
                f'Getting reports file: {datetime.now().strftime("%H:%M:%S")}')

            r = req.get(url['urls'][0], stream=True)


            #Making paths to temp files
            csv_zipped = os.path.join(BASE_PATH, 'temp/temp.csv.gz')
            csv_unzipped = os.path.join(BASE_PATH, f"temp/user_reports_{appkey}_{process_date}.csv")

            remove_file([csv_zipped, csv_unzipped]) 

            # Download the content of the request into a file
            with open(csv_zipped, 'wb') as f:
                f.write(r.raw.read())

            logger.info(
                f'Unzipping and parsing data from reports file: {datetime.now().strftime("%H:%M:%S")}')
            
            #Unzipping and parsing data from reports file
            
            with gzip.open(csv_zipped, "rb") as f_in:
                insert_is_data(f_in, csv_unzipped, project_id, bucket_name)
                

            logger.info(
            f'Uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')
            
            #Inserting final csv into bucket
            

            logger.info(
            f'Finished uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')

            remove_file([csv_zipped, csv_unzipped]) 


def insert_is_data(
    csv_file,
    path,
    project_id,
    bucket_name
):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"user_reports/{path.split('/')[-1]}") 
    blob.upload_from_file(csv_file)


def get(self):
  bucket_name = os.environ.get('BUCKET_NAME',
                               'sybogames-analytics-dev')

  self.response.headers['Content-Type'] = 'text/plain'
  self.response.write('Demo GCS Application running from Version: '
                      + os.environ['CURRENT_VERSION_ID'] + '\n')
  self.response.write('Using bucket name: ' + bucket_name + '\n\n')