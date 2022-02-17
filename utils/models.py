from datetime import datetime, timedelta, date
from pydantic import BaseModel,validator, StrictStr, Field, conint, StrictInt
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
    ds: StrictStr = Field(..., description="ds must have %Y-%m-%d format. DS is the reference date, do not submit the process date you want from the IR API")
    refreshToken: StrictStr = Field(..., description="refreshToken needs to be obtained from ...")
    secretKey: StrictStr = Field(..., description="secretKey needs to be obtained from ...")
    bucketName: StrictStr = Field('sybogames-analytics-dev', description="bucketName is the bucket to use, NOT the path of the blob")
    projectId: StrictStr = Field('sybogames-analytics-dev', description="projectId refers to the name of the project inside GCP")
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
                "ds": "2022-02-07",
                "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
                "secretKey": "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.bQTnz6AuMJvmXXQsVPrxeQNvzDkimo7VNXxHeSBfClLufmCVZRUuyTwJF311JHuh",
                "bucketName": "sybogames-analytics-test",
                "projectId": "sybogames-analytics-test",
                "requests": [
                                {
                                'baseURL': "https://platform.ironsrc.com/partners/publisher/mediation/applications/v6/stats",
                                'appKey': 'e4895939',
                                'startDate': 2,
                                'endDate':0,
                                'parameters': {
                                            "metrics": "activeUsers,engagedUsers,appRequests,appFillRate,clicks,impressions,completions,revenue,eCPM",
                                            "breakdown": "date,app,platform,country"
                                        }
                                }
                            ]
            }
        }