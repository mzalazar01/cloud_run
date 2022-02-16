from typing import Optional
from pydantic import BaseModel, confloat, conint, validator, StrictStr

class RowModel(BaseModel):
    ad_unit: Optional[StrictStr] = None
    advertising_id: Optional[StrictStr] = None
    advertising_id_type: Optional[StrictStr] = None
    user_id: Optional[StrictStr] = None
    segment: Optional[StrictStr] = None
    placement: Optional[StrictStr] = None
    ad_network: Optional[StrictStr] = None
    AB_Testing: Optional[StrictStr] = None
    impressions: conint(ge=0)
    revenue: confloat(ge=0)

    @validator('AB_Testing')
    def ab_testing_validation(cls, v):
        if not v in ('A', 'B'):
            raise ValueError('Value for AB Testing distinct than A or B is not allowed')
        return v