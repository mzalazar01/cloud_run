from datetime import datetime
from pydantic import BaseModel,validator, StrictStr, Field, conint
from typing import Dict, Union, List

class RequestModel(BaseModel):
    baseURL: StrictStr
    appKey: StrictStr
    startDate: conint(ge=0)
    endDate: conint(ge=0)
    parameters: Dict[str, Union[str, int]]

    @validator('endDate')
    def end_date_validation(cls, v, values):
        diff = values['startDate'] - v
        if diff < 0:
            raise ValueError('Range is not acceptable')
        return v

    class Config:
        schema_extra = {
            "example": {
                "baseURL": "https://platform.ironsrc.com/partners/userAdRevenue/v3",
                "appKey": "a35SfkE34",
                "startDate": 7,
                "endDate": 1,
                "parameters": {'reportType':1}
            }
        }

class BaseRequestModel(BaseModel):
    ds: StrictStr
    refreshToken: StrictStr
    secretKey: StrictStr
    bucketName: StrictStr
    projectId: StrictStr
    credentials: Dict[str, str]
    requests: List[RequestModel]

    @validator('ds')
    def ds_validation(cls, v):
        try:
            _ = datetime.strptime(v, '%Y-%m-%d')
        except:
            raise ValueError('Value for DS must be %Y-%m-%d')
        return v

    class Config:
        schema_extra = {
            "example": {
                        'ds':'2022-02-10',
                        'refreshToken':'c045d2d022320f9a1cd3a458405f1969',
                        'secretKey':'e573afb8afa54298f82b2de1d8b073ed',
                        'bucketName':'sybogames-analytics-dev', 
                        'projectId':'sybogames-analytics-dev',
                        'credentials': {
                            "type": "service_account",
                            "project_id": "sybogames-analytics-test",
                            "private_key_id": "........",
                            "private_key": "-----BEGIN PRIVATE KEY----- ... \n-----END PRIVATE KEY-----\n",
                            "client_email": "sybogames-analytics-test@appspot.gserviceaccount.com",
                            "client_id": "...",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sybogames-analytics-test%40appspot.gserviceaccount.com"
                        },
                        'requests': [
                            {
                            'baseURL': "https://platform.ironsrc.com/partners/userAdRevenue/v3",
                            'appKey': 'e4895939',
                            'startDate': 2,
                            'endDate':0,
                            'parameters': {
                                        "reportType":1
                                    }
                            },
                        ]
                    }
        }