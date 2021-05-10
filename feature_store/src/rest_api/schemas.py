from typing import List, Optional, Dict, Union, Any
from datetime import datetime
from pydantic import BaseModel, validator, Field
from shared.services.database import Converters

class DataType(BaseModel):
    """
    A class for representing a SQL data type as an object. Data types can have length, precision,
    and recall values depending on their type (VARCHAR(50), DECIMAL(15,2) for example.
    This class enables the breaking up of those data types into objects
    """
    data_type: str
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

class FeatureMetadata(BaseModel):
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    attributes: Optional[Dict[str, str]] = None

class FeatureBase(FeatureMetadata):
    feature_set_id: Optional[int] = None
    name: str
    feature_data_type: DataType
    feature_type: str

class FeatureCreate(FeatureBase):
    pass

class Feature(FeatureBase):
    feature_id: int
    compliance_level: Optional[int] = None
    last_update_ts: Optional[datetime] = None
    last_update_username: Optional[str] = None
    
    class Config:
        orm_mode = True

class FeatureDetail(Feature):
    feature_set_name: Optional[str] = None
    deployed: Optional[bool] = None

class FeatureSearch(BaseModel):
    name: Optional[Dict[str, str]] = Field(None, example={'is': 'total_spending'})
    tags: Optional[List[str]] = Field(None, example=['TAG1', 'CUSTOMER', 'ANOTHER_TAG'])
    attributes: Optional[Dict[str, str]] = Field(None, example={'QUALITY': 'GOOD', 'FEAT_TYPE': 'RFM'})
    feature_data_type: Optional[str] = Field(None, example='INTEGER')
    feature_type: Optional[str] = Field(None, example='C')
    schema_name: Optional[Dict[str, str]] = Field(None, example={'like': 'MY_SCHEMA'})
    table_name: Optional[Dict[str, str]] = Field(None, example={'like': 'spending'})
    deployed: Optional[bool] = Field(None, example=False)
    last_update_username: Optional[Dict[str, str]] = Field(None, example={'is': 'jack'})
    last_update_ts: Optional[Dict[str, datetime]] = Field(None, example={'lte': str(datetime.today()),
                                                                         'gte':'2010-04-23 19:57:03.243103'})

    @validator('feature_data_type')
    def convert(cls, v):
        if v.upper() not in Converters.SQL_TYPES:
            raise ValueError(f'Available feature datatypes are {Converters.SQL_TYPES}')
        return 'INTEGER' if v.upper() == 'INT' else v.upper()  # Users may want INT

    @validator('feature_type')
    def check_values(cls, v):
        if v not in ('C', 'N', 'O'):
            raise ValueError("Feature type must be one of ('C','N','O')")
        return v

    @validator('name', 'schema_name', 'table_name', 'last_update_username')
    def validate_dict(cls, v):
        if v and len(v) != 1:
            raise ValueError("Can only provide 1 filter per field!")
        if v and list(v.keys())[0] not in ('like', 'is'):
            raise ValueError("Available keys for this field are 'like' and 'is'")
        return v

    @validator('last_update_ts')
    def key_must_be_comparitor(cls, v):
        if not v:
            return
        for key, val in v.items():
            if key not in ('lt', 'lte', 'eq', 'gt', 'gte'):
                raise ValueError("last_update_ts key must be one of ('lt', 'lte', 'eq', 'gt', 'gte') ")
            # Datetime has a larger max precision than the database, so we need to trim
            # m = str(val.microsecond)
            # v[key] = val.replace(microsecond=int(m[:4]))
        return v


class FeatureSetBase(BaseModel):
    schema_name: str
    table_name: str
    description: Optional[str] = None
    primary_keys: Dict[str, DataType]

class FeatureSetCreate(FeatureSetBase):
    features: Optional[List[FeatureCreate]] = None
    pass

class FeatureSet(FeatureSetBase):
    feature_set_id: int
    deployed: Optional[bool] = False
    deploy_ts: Optional[datetime] = None
    last_update_username: Optional[str] = None
    last_update_ts: Optional[datetime] = None

    class Config:
        orm_mode = True

class FeatureSetDetail(FeatureSet):
    features: Optional[List[Feature]] = None
    num_features: Optional[int] = None

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
    view_id: Optional[int] = None
    view_sql: str

    class Config:
        orm_mode = True

class TrainingViewDetail(TrainingView):
    features: List[FeatureDetail]

class Source(BaseModel):
    name: str
    sql_text: str
    event_ts_column: str
    update_ts_column: str
    source_id: Optional[int] = None
    pk_columns: List[str]

class Pipeline(BaseModel):
    feature_set_id: int
    source_id: int
    pipeline_start_ts: datetime
    pipeline_interval: str
    backfill_start_ts: datetime
    backfill_interval: str
    pipeline_url: str
    last_update_ts: datetime
    last_update_username: str
    class Config:
        orm_mode = True

class FeatureAggregation(BaseModel):
    column_name: str
    agg_functions: List[str]
    agg_windows: List[str]
    feature_name_prefix: Optional[str] = None
    agg_default_value: Optional[float] = None

class SourceFeatureSetAgg(BaseModel):
    source_name: str
    schema_name: str
    table_name: str
    start_time: datetime
    schedule_interval: str
    aggregations: List[FeatureAggregation]
    backfill_start_time: Optional[datetime] = None
    backfill_interval: Optional[str] = None
    description: Optional[str] = None

# Basically just for neat documentation
class FeatureTimeframe(BaseModel):
    features: Union[List[Feature], List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

class FeatureJoinKeys(BaseModel):
    features: List[Union[str, FeatureDetail]]
    join_key_values: Dict[str, Union[str, int]]

class Deployment(BaseModel):
    model_schema_name: str
    model_table_name: str
    training_set_id: Optional[int] = None
    training_set_version: Optional[int] = None
    training_set_start_ts: Optional[datetime] = None
    training_set_end_ts: Optional[datetime] = None
    training_set_create_ts: Optional[datetime] = None
    run_id: Optional[str] = None
    last_update_ts: datetime
    last_update_username: str

    class Config:
        orm_mode = True

class DeploymentDetail(Deployment):
    training_set_name: Optional[str] = None

class DeploymentFeatures(DeploymentDetail):
    features: List[Feature]

class TrainingSetMetadata(BaseModel):
    name: Optional[str] = None
    training_set_start_ts: Optional[datetime] = None
    training_set_end_ts: Optional[datetime] = None
    training_set_create_ts: Optional[datetime] = None
    training_set_version: Optional[int] = None
    training_set_id: Optional[int] = None
    features: Optional[str] = None
    label: Optional[str] = None
    view_id: Optional[int] = None
    last_update_username: Optional[str] = None
    last_update_ts: Optional[datetime] = None

    class Config:
        orm_mode = True

class TrainingSet(BaseModel):
    sql: str
    training_view: Optional[TrainingView] = None
    features: List[Feature]
    metadata: Optional[TrainingSetMetadata] = None
    data: Optional[Any] = None # For storing the result of the query


class FeatureStoreSummary(BaseModel):
    num_feature_sets: int
    num_deployed_feature_sets: int
    num_features: int
    num_deployed_features: int
    num_training_sets: int
    num_training_views: int
    num_models: int
    num_deployed_models: int
    num_pending_feature_set_deployments: int
    recent_features: List[str]
    most_used_features: List[str]
