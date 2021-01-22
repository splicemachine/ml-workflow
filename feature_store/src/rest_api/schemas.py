from typing import List, Optional, Dict
from datetime import datetime
from pydantic import BaseModel

class FeatureSetBase(BaseModel):
    schema_name: str
    table_name: str
    description: Optional[str] = None
    primary_keys: Dict[str, str]

class FeatureSetCreate(FeatureSetBase):
    # primary_keys: Dict[str, str]
    pass

class FeatureSet(FeatureSetBase):
    feature_set_id: int
    # deployed: bool

    class Config:
        orm_mode = True

# class FeatureType:
#     """
#     Class containing names for
#     valid feature types
#     """
#     categorical: str = "N"
#     ordinal: str = "O"
#     continuous: str = "C"

class FeatureBase(BaseModel):
    name: str
    feature_data_type: str
    feature_type: str
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

class FeatureCreate(FeatureBase):
    # Fill this in later
    pass

class Feature(FeatureBase):
    # Fill this in later
    feature_set_id: int

    class Config:
        orm_mode = True

class FeatureDescription(Feature):
    feature_set_name: str

class TrainingViewBase(BaseModel):
    name: str
    description: Optional[str] = None
    pk_columns: List[str]
    ts_column: str
    label_column: Optional[str] = None
    join_keys: List[str]

class TrainingViewCreate(TrainingViewBase):
    sql_text: str

class TrainingView(TrainingViewBase):
    view_sql: str

    class Config:
        orm_mode = True