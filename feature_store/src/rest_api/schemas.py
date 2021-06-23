from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator

from shared.db.converters import Converters


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
    feature_set_id: Optional[int] = None
    feature_set_version: Optional[int] = None
    deployed: Optional[bool] = None

class FeatureSearch(BaseModel):
    name: Optional[Dict[str, str]] = Field(None, example={'accuracy': 'is', 'value': 'total_spending'})
    tags: Optional[List[str]] = Field(None, example=['TAG1', 'CUSTOMER', 'ANOTHER_TAG'])
    attributes: Optional[Dict[str, str]] = Field(None, example={'QUALITY': 'GOOD', 'FEAT_TYPE': 'RFM'})
    feature_data_type: Optional[str] = Field(None, example='INTEGER')
    feature_type: Optional[str] = Field(None, example='C')
    schema_name: Optional[Dict[str, str]] = Field(None, example={'accuracy': 'like', 'value':'MY_SCHEMA'})
    table_name: Optional[Dict[str, str]] = Field(None, example={'accuracy': 'like', 'value':'spending'})
    deployed: Optional[bool] = Field(None, example=False)
    last_update_username: Optional[Dict[str, str]] = Field(None, example={'accuracy':'is', 'value': 'jack'})
    last_update_ts: Optional[List[Dict[str, Union[str,datetime]]]] = Field(None, example=[
                                                                        {'accuracy':'lte', 'value': str(datetime.today())},
                                                                        {'accuracy':'gte', 'value': '2010-04-23 19:57:03.243103'}
                                                                    ]
                                                                )

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
        if v:
            err = "Must provide an accuracy and a value. Example: {'accuracy': 'is', 'value': 'total_spending'}"
            if len(v) != 2:
                raise ValueError(err)
            if 'accuracy' not in v or 'value' not in v:
                raise ValueError(err)
        # Convert to a single element dictionary: {'accuracy': 'is', 'value': 'total_spending'} -> {'is': 'total_spending'}
            return {v['accuracy']: v['value']}
        return v

    @validator('last_update_ts')
    def key_must_be_comparitor(cls, v):
        # Condense into simpler dictionary
        #[{'accuracy': 'lte', 'value': str(datetime.today())}, {'accuracy': 'gte', 'value':'2010-04-23 19:57:03.243103'}]
        # to
        # {'lte': str(datetime.today())}, 'gte': '2010-04-23 19:57:03.243103'}
        out = {}
        if not v:
            return
        for vt in v:
            if 'accuracy' not in vt or 'value' not in vt:
                err =  "Must provide an accuracy and a value. Example: " \
                       "[\n\t{'accuracy': 'lte', 'value': str(datetime.today())},\n\t" \
                       "{'accuracy': 'gte', 'value':'2010-04-23 19:57:03.243103'}\n]"
                raise ValueError(err)
            if vt['accuracy'] not in ('lt', 'lte', 'eq', 'gt', 'gte'):
                raise ValueError("last_update_ts accuracy must be one of ('lt', 'lte', 'eq', 'gt', 'gte') ")
            out[vt['accuracy']] = vt['value']
        return out


class FeatureSetBase(BaseModel):
    schema_name: str
    table_name: str
    description: Optional[str] = None
    primary_keys: Dict[str, DataType]

class FeatureSetCreate(FeatureSetBase):
    features: Optional[List[FeatureCreate]] = None

class FeatureSet(FeatureSetBase):
    feature_set_id: int
    last_update_username: Optional[str] = None
    last_update_ts: Optional[datetime] = None

    class Config:
        orm_mode = True

class FeatureSetAlter(BaseModel):
    feature_set_id: Optional[int] = None
    description: Optional[str] = None
    primary_keys: Optional[Dict[str, DataType]] = None

class FeatureSetUpdate(FeatureSetAlter):
    primary_keys: Dict[str, DataType]
    features: Optional[List[FeatureCreate]] = None

class FeatureSetVersion(BaseModel):
    feature_set_id: int
    feature_set_version: int
    deployed: bool = False
    deploy_ts: Optional[datetime] = None
    create_ts: datetime
    create_username: Optional[str] = None

class FeatureSetDetail(FeatureSet, FeatureSetVersion):
    features: Optional[List[Feature]] = None
    num_features: Optional[int] = None
    has_training_sets: Optional[bool] = None
    has_deployments: Optional[bool] = None

class TrainingViewBase(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class TrainingViewCreate(TrainingViewBase):
    pk_columns: List[str]
    sql_text: str
    ts_column: str
    label_column: Optional[str] = None
    join_columns: Optional[List[str]] = None

class TrainingViewUpdate(BaseModel):
    description: Optional[str] = None
    pk_columns: List[str]
    sql_text: str
    ts_column: str
    label_column: Optional[str] = None
    join_columns: Optional[List[str]] = None

class TrainingViewAlter(BaseModel):
    description: Optional[str] = None
    pk_columns: Optional[List[str]]
    sql_text: Optional[str]
    ts_column: Optional[str]
    label_column: Optional[str] = None
    join_columns: Optional[List[str]] = None

class TrainingView(TrainingViewBase):
    view_id: Optional[int] = None
    # view_sql: str

    class Config:
        orm_mode = True

class TrainingViewVersion(BaseModel):
    view_id: Optional[int] = None
    view_version: Optional[int] = None
    sql_text: str
    ts_column: str
    label_column: Optional[str] = None
    last_update_username: Optional[str] = None
    last_update_ts: Optional[datetime] = None

class TrainingViewDetail(TrainingView, TrainingViewVersion):
    pk_columns: List[str]
    join_columns: Optional[List[str]] = None
    features: Optional[List[FeatureDetail]] = None

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
    view_version: Optional[int] = None
    last_update_username: Optional[str] = None
    last_update_ts: Optional[datetime] = None

    class Config:
        orm_mode = True

class TrainingSet(BaseModel):
    sql: str
    training_view: Optional[TrainingViewDetail] = None
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

class PipeAlter(BaseModel):
    description: Optional[str] = None
    func: Optional[str] = None
    code: Optional[str] = None

class PipeUpdate(PipeAlter):
    func: str

class PipeCreate(PipeUpdate):
    name: str
    ptype: str
    lang: str

    @validator('ptype')
    def check_ptype(cls, t):
        if t not in ('S', 'B', 'O', 'R'):
            raise ValueError("ptype must be one of ('S','B','O','R')")
        return t

    @validator('lang')
    def check_lang(cls, l):
        if l not in ('python', 'pyspark', 'sql'):
            raise ValueError("lang must be one of the currently supported languages: ('python', 'pyspark', 'sql')")
        return l

class PipeVersion(BaseModel):
    pipe_id: int
    pipe_version: int
    func: str
    code: str
    last_update_ts: Optional[datetime] = None
    last_update_username: Optional[str] = None

class Pipe(PipeCreate):
    pipe_id: int

class PipeDetail(Pipe, PipeVersion):
    pass
