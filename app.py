from datetime import datetime, timedelta
from fastapi import FastAPI, Body
from utils.models import BaseRequestModel
from utils.utils import get_bearer_token, remove_file, insert_file_to_bucket, explode
import requests as req
import logging
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

@app.post("/ironsource/user_reports/", status_code=201)
async def user_reports(request: BaseRequestModel):

    params = request.dict()
    
    token = get_bearer_token(params['secretKey'], params['refreshToken'])

    get_user_reports(params, token)

    return {'message': f"Reports loaded successfully on bucket {params['bucketName']} from project {params['projectId']}"}


def get_user_reports(params, token):

    header = {'Authorization': f"Bearer {token}"}
    
    for config in params['requests']:
        for i in range(config['startDate'], config['endDate'], -1):  # Checking since since_date until ds
            _conf = config.copy()

            _dict = {
                'date': (datetime.strptime(params['ds'], '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d'),
                'appKey': config['appKey'],
                'reportType': config['parameters']['reportType']
            }
            
            _conf['parameters'].update(_dict)
            
            appkey = _conf['parameters']['appKey']

            process_date = _conf['parameters']['date']

            logger.info(
                f"Reading data from appkey {appkey} and date {process_date}")


            logger.info(
                f'Getting data from IR Api: {datetime.now().strftime("%H:%M:%S")}')


            url = req.get(_conf['baseURL'], headers=header,
                          params=_conf['parameters']).json()
            
            # Hit API and get link to file

            logger.info(
                f'Getting reports file: {datetime.now().strftime("%H:%M:%S")}')

            r = req.get(url['urls'][0], stream=True)

            #Making paths to temp files
            csv_zipped = os.path.join(BASE_PATH, 'temp/temp.csv.gz')
            blob_path = f"user_reports/user_reports_{appkey}_{process_date}.csv"

            remove_file(csv_zipped) 

            # Download the content of the request into a file
            with open(csv_zipped, 'wb') as f:
                f.write(r.raw.read())

            logger.info(
            f'Uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')
            
            #Inserting csv into bucket

            with gzip.open(csv_zipped, "rb") as f_in:
                insert_file_to_bucket(f_in, blob_path, params['projectId'], params['bucketName'], params['credentials'])
            
            logger.info(
            f'Finished uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')


@app.post("/ironsource/ad_revenue_reports/", status_code=201)
async def ad_revenue_reports(request: BaseRequestModel):

    params = request.dict()
    
    token = get_bearer_token(params['secretKey'], params['refreshToken'])

    get_revenue_reports(params, token)

    return {'message': f"Reports loaded successfully on bucket {params['bucketName']} from project {params['projectId']}"}



def get_revenue_reports(params, token):
    header = {'Authorization': f"Bearer {token}"}
    for config in params['requests']:

        _dict = {
            'startDate': (datetime.strptime(params['ds'], '%Y-%m-%d') - timedelta(days=(config['startDate']))).strftime('%Y-%m-%d'),
            'endDate': (datetime.strptime(params['ds'], '%Y-%m-%d') - timedelta(days=config['endDate'])).strftime('%Y-%m-%d'),
            'appKey': config['appKey']
        }

        config['parameters'].update(_dict)

        logger.info(
                f'Getting data from IR Api: {datetime.now().strftime("%H:%M:%S")}')

        is_data = req.get(config['baseURL'], headers=header,
                          params=config['parameters']).json()

        logger.info(
                f'Unpacking nested data: {datetime.now().strftime("%H:%M:%S")}')

        rows = explode(is_data)

        appkey = config['appKey']
        process_date = params['ds']

        json_path = os.path.join(BASE_PATH, f"temp/ad_revenue_reports_{appkey}_{process_date}.json")
        blob_path = f"ad_revenue_reports/ad_revenue_reports_{appkey}_{process_date}.json"

        logger.info(
                f'Creating temp json file from unnested rows: {datetime.now().strftime("%H:%M:%S")}')

        with open(json_path, 'w') as _file:
            _file.write(json.dumps(rows))

        with open(json_path, 'rb') as _file:
            insert_file_to_bucket(_file, blob_path, params['projectId'], params['bucketName'], params['credentials'])
        
        logger.info(
            f'Uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')


        logger.info(
            f'Finished uploading reports file to bucket: {datetime.now().strftime("%H:%M:%S")}')

        remove_file(json_path) 

        
