from datetime import datetime
from pydantic import BaseModel,validator, StrictStr, Field

class RequestModel(BaseModel):
    ds: StrictStr = Field(..., description="ds must have %Y-%m-%d format. DS is the reference date, do not submit the process date you want from the IR API")
    refresh_token: StrictStr = Field(..., description="refesh_token needs to be obtained from ...")
    secret_key: StrictStr = Field(..., description="secret_key needs to be obtained from ...")
    bucket_name: StrictStr = Field('sybogames-analytics-dev', description="bucket_name is the bucket to use, NOT the path of the blob")
    project_id: StrictStr = Field('sybogames-analytics-dev', description="project_id refers to the name of the project inside GCP")

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
                "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
                "secret_key": "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.bQTnz6AuMJvmXXQsVPrxeQNvzDkimo7VNXxHeSBfClLufmCVZRUuyTwJF311JHuh",
                "bucket_name": "sybogames-analytics-test",
                "project_id": "sybogames-analytics-test"
            }
        }

    