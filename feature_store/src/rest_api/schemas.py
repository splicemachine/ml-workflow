from typing import List, Optional, Dict
from datetime import datetime
from pydantic import BaseModel

class FeatureSetBase(BaseModel):
    schema_name: str
    table_name: str
    description: Optional[str] = None

class FeatureSetCreate(FeatureSetBase):
    primary_keys: Dict[str, str]

class FeatureSet(FeatureSetBase):
    feature_set_id: int
    deployed: bool