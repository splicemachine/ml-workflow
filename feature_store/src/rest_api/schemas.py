from typing import List, Optional, Dict
from datetime import datetime
from pydantic import BaseModel

class FeatureSetBase(BaseModel):
    schema_name: str
    table_name: str
    description: Optional[str] = None
    primary_keys: Dict[str, str]

class FeatureSetCreate(FeatureSetBase):
    pass

class FeatureSet(FeatureSetBase):
    feature_set_id: int
    deployed: Optional[bool] = False

    class Config:
        orm_mode = True

class FeatureBase(BaseModel):
    name: str
    feature_data_type: str
    feature_type: str
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

class FeatureCreate(FeatureBase):
    pass

class Feature(FeatureBase):
    feature_set_id: int

    class Config:
        orm_mode = True

class FeatureDescription(Feature):
    feature_set_name: Optional[str] = None
    compliance_level: Optional[int] = None
    last_update_ts: datetime
    last_update_username: str

class TrainingViewBase(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    pk_columns: List[str]
    ts_column: str
    label_column: Optional[str] = None
    join_columns: Optional[List[str]] = None

class TrainingViewCreate(TrainingViewBase):
    sql_text: str

class TrainingView(TrainingViewBase):
    view_id: int
    view_sql: str

    class Config:
        orm_mode = True