from sqlalchemy.orm import Session, aliased
from typing import List, Dict, Union, Any, Tuple, Set
from . import schemas
from .constants import SQL
from sqlalchemy.sql.elements import TextClause
from shared.models import feature_store_models as models
from shared.services.database import SQLAlchemyClient, DatabaseFunctions, Converters, DatabaseSQL
from shared.logger.logging_config import logger
from fastapi import status
import re
import json
from datetime import datetime
from collections import Counter
from sqlalchemy import desc, update, String, func, distinct, cast, and_, Column, literal_column, text, case
from .utils.utils import (__get_pk_columns, get_pk_column_str, datatype_to_sql,
                          sql_to_datatype, _sql_to_sqlalchemy_columns,
                          __validate_feature_data_type, __validate_primary_keys)
from .utils.feature_utils import model_to_schema_feature
from mlflow.store.tracking.dbmodels.models import SqlRun, SqlTag, SqlParam
from sqlalchemy.schema import MetaData, Table, PrimaryKeyConstraint, DDL
from sqlalchemy.types import TIMESTAMP
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from decimal import Decimal
from uuid import uuid1


def get_db():
    """
    Provides SqlAlchemy Session object to path operations
    """
    db = SQLAlchemyClient.SessionMaker()
    try:
        yield db
    finally:
        logger.info("Closing session")
        db.close()


def validate_feature_set(db: Session, fset: schemas.FeatureSetCreate) -> None:
    """
    Asserts a feature set doesn't already exist in the database
    :param db: SqlAlchemy Session
    :param fset: the feature set
    :return: None
    """
    logger.info("Validating Schema")
    str = f'Feature Set {fset.schema_name}.{fset.table_name} already exists. Use a different schema and/or table name.'
    # Validate Table
    if DatabaseFunctions.table_exists(fset.schema_name, fset.table_name, db.get_bind()):
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS,
                                     message=str)
    # Validate metadata
    if len(get_feature_sets(db, _filter={'table_name': fset.table_name, 'schema_name': fset.schema_name})) > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS,
                                     message=str)
    # Validate names exist
    if not fset.schema_name:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message='You must specify a schema name')
    if not fset.table_name:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message='You must specify a table name')

    if fset.schema_name.upper() in ('MLMANAGER', 'SYS', 'SYSVW', 'FEATURESTORE'):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f'You cannot create feature sets in the schema {fset.schema_name}')
    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', fset.schema_name, re.IGNORECASE):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
                                     message=f'Schema {fset.schema_name} does not conform. Must start with an alphabetic character, '
                                             'and can only contains letters, numbers and underscores')
    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', fset.table_name, re.IGNORECASE):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
                                     message=f'Table {fset.table_name} does not conform. Must start with an alphabetic character, '
                                             'and can only contains letters, numbers and underscores')
    __validate_primary_keys(fset.primary_keys)


def validate_schema_table(names: List[str]) -> None:
    """
    Asserts a list names each conforms to {schema_name.table_name}
    :param names: the list of names
    :return: None
    """
    if not all([len(name.split('.')) == 2 for name in names]):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message="It seems you've passed in an invalid name. " \
                                             "Names must conform to '[schema_name].[table_name]'")


def validate_feature(db: Session, name: str, data_type: schemas.DataType) -> None:
    """
    Ensures that the feature doesn't exist as all features have unique names
    :param db: SqlAlchemy Session
    :param name: the Feature name
    :param data_type: (str) the Feature data type
    :return: None
    """
    # TODO: Capitalization of feature name column
    # TODO: Make more informative, add which feature set contains existing feature

    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', name, re.IGNORECASE):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
                                     message='Feature name does not conform. Must start with an alphabetic character, '
                                             'and can only contains letters, numbers and underscores')

    l = db.query(models.Feature.name).filter(func.upper(models.Feature.name) == name.upper()).count()
    if l > 0:
        err_str = f"Cannot add feature {name}, feature already exists in Feature Store. Try a new feature name."
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS,
                                     message=err_str)
    __validate_feature_data_type(data_type)
    # Last resort if all else passes. Create a temporary table with the column name and data type. If this fails,
    # you won't be able to deploy this feature
    try:
        sql_type = datatype_to_sql(data_type)
        tmp = str(uuid1()).replace('-', '_')
        db.execute(
            f'CREATE LOCAL TEMPORARY TABLE COLUMN_TEST_{tmp}({name} {sql_type})')  # Will be deleted when session ends
    except Exception as err:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f'The feature {name} of type {data_type} is invalid. The data type could '
                                             f'not be parsed, and threw the following error: {str(err)}')


def validate_feature_vector_keys(join_key_values, feature_sets) -> None:
    """
    Validates that all necessary primary keys are provided when requesting a feature vector

    :param join_key_values: dict The primary (join) key columns and values provided by the user
    :param feature_sets: List[FeatureSet] the list of Feature Sets derived from the requested Features
    :return: None. Raise Exception on bad validation
    """

    feature_set_key_columns = {fkey.lower() for fset in feature_sets for fkey in fset.primary_keys.keys()}
    missing_keys = feature_set_key_columns - join_key_values.keys()
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.MISSING_ARGUMENTS,
                                     message=f"The following keys were not provided and must be: {missing_keys}")


def get_feature_vector(db: Session, feats: List[schemas.Feature], join_keys: Dict[str, Union[str, int]],
                       feature_sets: List[schemas.FeatureSet],
                       return_pks: bool, return_sql: bool) -> Union[Dict[str, Any], str]:
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets

    :param db: SqlAlchemy Session
    :param features: List of Features
    :param join_key_values: (dict) join key values to get the proper Feature values formatted as {join_key_column_name: join_key_value}
    :param feature_sets: List of Feature Sets
    :param return_pks: Whether to return the Feature Set primary keys in the vector. Default True
    :param return_sql: Whether to return the SQL needed to get the vector or the values themselves. Default False
    :return: Dict or str (SQL statement)
    """
    metadata = MetaData(db.get_bind())

    tables = [Table(fset.table_name.lower(), metadata, PrimaryKeyConstraint(*[pk.lower() for pk in fset.primary_keys]),
                    schema=fset.schema_name.lower(), autoload=True). \
                  alias(f'fset{fset.feature_set_id}') for fset in feature_sets]

    columns = []
    if return_pks:
        seen = set()
        pks = [seen.add(pk_col.name) or getattr(table.c, pk_col.name) for table in tables for pk_col in
               table.primary_key if pk_col.name not in seen]
        columns.extend(pks)

    columns.extend([getattr(table.c, f.name.lower()) for f in feats for table in tables if f.name.lower() in table.c])

    # For each Feature Set, for each primary key in the given feature set, get primary key value from the user provided dictionary
    filters = [getattr(table.c, pk_col.name) == join_keys[pk_col.name.lower()]
               for table in tables for pk_col in table.primary_key]

    q = db.query(*columns).filter(and_(*filters))
    sql = str(q.statement.compile(db.get_bind(), compile_kwargs={"literal_binds": True}))

    if return_sql:
        return sql

    vector = db.execute(sql).first()
    return dict(vector) if vector else {}


def get_training_view_features(db: Session, training_view: str) -> List[schemas.Feature]:
    """
    Returns the available features for the given a training view name

    :param db: SqlAlchemy Session
    :param training_view: The name of the training view
    :return: A list of available Feature objects
    """
    fsk = db.query(
        models.FeatureSetKey.feature_set_id,
        models.FeatureSetKey.key_column_name,
        func.count().over(partition_by=models.FeatureSetKey.feature_set_id). \
            label('KeyCount')). \
        subquery('fsk')

    tc = aliased(models.TrainingView, name='tc')
    c = aliased(models.TrainingViewKey, name='c')
    f = aliased(models.Feature, name='f')

    match_keys = db.query(
        f.feature_id,
        fsk.c.KeyCount,
        func.count(distinct(fsk.c.key_column_name)). \
            label('JoinKeyMatchCount')). \
        select_from(tc). \
        join(c, (c.view_id == tc.view_id) & (c.key_type == 'J')). \
        join(fsk, c.key_column_name == fsk.c.key_column_name). \
        join(f, f.feature_set_id == fsk.c.feature_set_id). \
        filter(tc.name == training_view). \
        group_by(
        f.feature_id,
        fsk.c.KeyCount). \
        subquery('match_keys')

    fl = db.query(match_keys.c.feature_id). \
        filter(match_keys.c.JoinKeyMatchCount == match_keys.c.KeyCount). \
        subquery('fl')

    q = db.query(f).filter(f.feature_id.in_(fl))

    features = [model_to_schema_feature(f) for f in q.all()]
    return features


def feature_set_is_deployed(db: Session, fset_id: int) -> bool:
    """
    Returns if this feature set is deployed or not

    :param db:  SqlAlchemy Session
    :param feature_set_id: The Feature Set ID in question
    :return: True if the feature set is deployed
    """
    return db.query(models.FeatureSet.deployed). \
        filter(models.FeatureSet.feature_set_id == fset_id). \
        all()[0]


def delete_features_from_feature_set(db: Session, feature_set_id: int):
    """
    Deletes features for a particular feature set

    :param db: Database Session
    :param features: feature IDs to delete
    """
    # Delete features
    logger.info("Removing features")
    db.query(models.Feature).filter(models.Feature.feature_set_id == feature_set_id).delete(synchronize_session='fetch')


def delete_features_set_keys(db: Session, feature_set_id: int):
    """
    Deletes feature set keys for a particular feature set

    :param db: Database Session
    :param features: feature IDs to delete
    """
    # Delete features
    logger.info("Removing features")
    db.query(models.FeatureSetKey).filter(models.FeatureSetKey.feature_set_id == feature_set_id). \
        delete(synchronize_session='fetch')


def delete_feature_set(db: Session, feature_set_id: int):
    """
    Deletes a feature set with a given ID

    :param db: Database Session
    :param feature_set_id: feature set ID to delete
    """
    db.query(models.FeatureSet).filter(models.FeatureSet.feature_set_id == feature_set_id).delete(
        synchronize_session='fetch')


def delete_pipeline(db: Session, feature_set_id: int):
    """
    Deletes pipeline and dependencies from feature store with a given feature set id

    :param db: Database Session
    :param feature_set_id: feature set ID to delete
    """
    # Pipeline Operations
    db.query(models.PipelineOps).filter(models.PipelineOps.feature_set_id == feature_set_id) \
        .delete(synchronize_session='fetch')
    # Pipeline aggregations
    db.query(models.PipelineAgg).filter(models.PipelineAgg.feature_set_id == feature_set_id) \
        .delete(synchronize_session='fetch')
    # Pipeline
    db.query(models.Pipeline).filter(models.Pipeline.feature_set_id == feature_set_id) \
        .delete(synchronize_session='fetch')


def delete_training_set_features(db: Session, training_sets: Set[int]):
    """
    Deletes training set features from training sets with the given IDs
    
    :param db: Database Session
    :param training_sets: training set IDs
    """
    db.query(models.TrainingSetFeature).filter(models.TrainingSetFeature.training_set_id.in_(training_sets)). \
        delete(synchronize_session='fetch')


def delete_training_set_stats(db: Session, training_sets: Set[int]):
    """
    Deletes statistics about features for particular training sets

    :param db: Session
    :param training_sets: Training Set IDs
    """
    db.query(models.TrainingSetLabelStats).filter(models.TrainingSetLabelStats.training_set_id.in_(training_sets)). \
        delete(synchronize_session='fetch')
    db.query(models.TrainingSetFeatureStats).filter(models.TrainingSetFeatureStats.training_set_id.in_(training_sets)). \
        delete(synchronize_session='fetch')


def delete_training_sets(db: Session, training_sets: Set[int]):
    """
    Deletes training sets with the given IDs
    
    :param db: Database Session
    :param training_sets: training set IDs to delete
    """
    db.query(models.TrainingSet).filter(models.TrainingSet.training_set_id.in_(training_sets)). \
        delete(synchronize_session='fetch')


def delete_training_set_instances(db: Session, training_sets: Set[int]):
    """
    Deletes training set instances with the given IDs.

    :param db: Database Session
    :param training_sets: training set IDs to delete
    """
    db.query(models.TrainingSetInstance).filter(models.TrainingSetInstance.training_set_id.in_(training_sets)). \
        delete(synchronize_session='fetch')


def delete_training_view_keys(db: Session, view_id: int):
    """
    Deletes training view keys for a particular training view

    :param db: Database Session
    :param view_id: training view ID
    """
    # Delete features
    logger.info("Removing Training View Keys")
    db.query(models.TrainingViewKey).filter(models.TrainingViewKey.view_id == view_id). \
        delete(synchronize_session='fetch')


def delete_training_view(db: Session, view_id: int):
    """
    Deletes a training view

    :param db: Database Session
    :param view_id: training view ID
    """
    db.query(models.TrainingView).filter(models.TrainingView.view_id == view_id). \
        delete(synchronize_session='fetch')


def register_training_set_instance(db: Session, tsm: schemas.TrainingSetMetadata):
    """
    Registers a new Training Set Instance. A training set with a start ts, end ts, create ts, name, ID, and a version

    :param db: Session
    :param tsm: TrainingSetMetadata
    """
    instance = tsm.__dict__
    instance['last_update_username'] = 'CURRENT_USER'  # FIXME: We need to set this to the feature store user's username
    db.execute(DatabaseSQL.training_set_instance.format(**instance))


def create_training_set(db: Session, ts: schemas.TrainingSet, name: str) -> int:
    """
    Creates a new record in the Training_Set table and returns the Training Set ID

    :param db: Session
    :param ts: The Training Set
    :param name: Training Set name
    :return: (int) the Training Set ID
    """
    ts = models.TrainingSet(name=name, view_id=ts.metadata.view_id)
    db.add(ts)
    db.flush()
    return ts.training_set_id


def register_training_set_features(db: Session, tset_id: int, features: List[schemas.Feature], label: str = None):
    """
    Registers features for a training set

    :param db: Session
    :param features: A list of tuples indicating the Feature's ID and if that Feature is a label
    for this Training Set
    """
    feats = [
        models.TrainingSetFeature(
            training_set_id=tset_id,
            feature_id=f.feature_id,
            is_label=(f.name.lower() == label.lower()) if label else False,
        ) for f in features
    ]
    db.bulk_save_objects(feats)


def get_feature_set_dependencies(db: Session, feature_set_id: int) -> Dict[str, Set[Any]]:
    """
    Returns the model deployments and training sets that rely on the given feature set

    :param db:  SqlAlchemy Session
    :param feature_set_id: The Feature Set ID in question
    """
    # return db.query(models.FeatureSet.deployed).filter(models.FeatureSet.feature_set_id==feature_set_id).all()[0]
    f = aliased(models.Feature, name='f')
    tset = aliased(models.TrainingSet, name='tset')
    tset_feat = aliased(models.TrainingSetFeature, name='tset_feat')
    pipe = aliased(models.Pipeline, name='pipeline')
    d = aliased(models.Deployment, name='d')

    p = db.query(f.feature_id).filter(f.feature_set_id == feature_set_id).subquery('p')
    p1 = db.query(tset_feat.training_set_id).filter(tset_feat.feature_id.in_(p)).subquery('p1')
    r = db.query(tset.training_set_id, d.model_schema_name, d.model_table_name). \
        select_from(tset). \
        join(d, d.training_set_id == tset.training_set_id, isouter=True). \
        filter(tset.training_set_id.in_(p1)).all()
    deps = dict(
        model=set([f'{schema}.{table}' for _, schema, table in r if schema and table]),
        training_set=set([tid for tid, _, _ in r])
    )
    return deps


def get_training_view_dependencies(db: Session, vid: int) -> List[Dict[str, str]]:
    """
    Returns the mlflow run ID and model deployment name that rely on the given training view

    :param db:  SqlAlchemy Session
    :param feature_set_id: The Feature Set ID in question
    """
    tset = aliased(models.TrainingSet, name='tset')
    d = aliased(models.Deployment, name='d')

    p = db.query(tset.training_set_id).filter(tset.view_id == vid).subquery('p')
    res = db.query(d.model_schema_name, d.model_table_name, d.run_id).filter(d.training_set_id.in_(p)).all()

    deps = [{
        'run_id': run_id,
        'deployment': f'{schema}.{table}'
    } for schema, table, run_id in res]

    return deps


def get_training_sets_from_view(db: Session, vid: int) -> List[int]:
    """
    Returns a list of training set IDs that were created from the given training view ID
    
    :param db: SqlAlchemy Session
    :param vid: The training view ID
    """
    res = db.query(models.TrainingSet.training_set_id).filter(models.TrainingSet.view_id == vid).all()
    return [i for (i,) in res]


def get_training_set_instance_by_name(db: Session, name: str, version: int = None) -> schemas.TrainingSetMetadata:
    """
    Gets a training set instance by its name, if it exists. If it does exist, it will get the training set instance
    with the largest version number.

    :param db: Session
    :param name: Training Set Instance name
    :param version: The training set version. If not set will get the newest version
    :return: TrainingSetMetadata
    """
    ts = aliased(models.TrainingSet, name='ts')
    tsi = aliased(models.TrainingSetInstance, name='tsi')
    tsf = aliased(models.TrainingSetFeature, name='tsf')

    # Get the max version for the Training Set Name provided
    p = db.query(
        tsi.training_set_id,
        func.max(tsi.training_set_version).label('training_set_version'),
    ). \
        select_from(ts). \
        join(tsi, ts.training_set_id == tsi.training_set_id). \
        filter(ts.name == name)
    if version:
        p = p.filter(tsi.training_set_version == version)

    p = p.group_by(tsi.training_set_id). \
        subquery('p')

    feature_ids = db.query(
        tsf.training_set_id, func.string_agg(tsf.feature_id, literal_column("','"), type_=String).label('features')
    ).\
        group_by(tsf.training_set_id).subquery('feature_ids')

    # Get the training set start_ts, end_ts, name, create_ts
    tsm = db.query(
        ts.name,
        ts.training_set_id,
        ts.view_id,
        tsi.training_set_version,
        tsi.training_set_start_ts,
        tsi.training_set_end_ts,
        tsi.training_set_create_ts,
        feature_ids.c.features
    ). \
        select_from(p). \
        join(ts, p.c.training_set_id == ts.training_set_id). \
        join(tsi, and_(tsi.training_set_version == p.c.training_set_version, tsi.training_set_id == p.c.training_set_id)). \
        join(feature_ids, feature_ids.c.training_set_id == tsi.training_set_id). \
        first()
    return schemas.TrainingSetMetadata(**tsm._asdict()) if tsm else None

def list_training_sets(db: Session) -> List[schemas.TrainingSetMetadata]:
    """
    Gets a list of training sets (Name, View ID, Training Set ID, Last_update_ts, Last_update_username, label)

    :return: List of Training Set Metadata (Name, View ID, Training Set ID, Last_update_ts, Last_update_username, label)
    """
    tv = aliased(models.TrainingView, name='tv')
    ts = aliased(models.TrainingSet, name='ts')
    tsf = aliased(models.TrainingSetFeature, name='tsf')
    f = aliased(models.Feature, name='f')

    q = db.query(
        ts.name, ts.training_set_id, ts.view_id, ts.last_update_username, ts.last_update_ts,
        # If the training set comes from a training view, the label will be TrainingView.label_column
        # Otherwise, it may be one of the features in TrainingSetFeatures
        # Or, it may be null (a label isn't required)
        case(
            [(f.name != None, f.name)],
            else_= tv.label_column
        ).label('label')).\
        select_from(ts).\
        join(tsf, and_(ts.training_set_id==tsf.training_set_id,tsf.is_label==True),isouter=True).\
        join(f, f.feature_id==tsf.feature_id, isouter=True).\
        join(tv, ts.view_id==tv.view_id, isouter=True)

    tsms: List[schemas.TrainingSetMetadata] = []
    for name, tsid, view_id, user, ts, label in q.all():
        tsms.append(
            schemas.TrainingSetMetadata(
                name=name, training_set_id=tsid, view_id=view_id,
                last_update_username=user,last_update_ts=ts, label=label
            )
        )
    return tsms



def get_feature_sets(db: Session, feature_set_ids: List[int] = None, feature_set_names: List[str] = None,
                     _filter: Dict[str, str] = None) -> List[schemas.FeatureSetDetail]:
    """
    Returns a list of available feature sets

    :param db: SqlAlchemy Session
    :param feature_set_ids: A list of feature set IDs. If none will return all FeatureSets
    :param detail: Whether or not to include extra details of the Feature Set (number of features)
    :param _filter: Dictionary of filters to apply to the query. This filter can be on any attribute of FeatureSets.
        If None, will return all FeatureSets
    :return: List[FeatureSet] the list of Feature Sets
    """
    feature_sets = []
    feature_set_ids = feature_set_ids or []
    _filter = _filter or {}

    fset = aliased(models.FeatureSet, name='fset')

    queries = []
    if feature_set_ids:
        queries.append(fset.feature_set_id.in_(tuple(set(feature_set_ids))))
    if feature_set_names:
        queries.extend([and_(func.upper(fset.schema_name) == name.split('.')[0].upper(),
                             func.upper(fset.table_name) == name.split('.')[1].upper()) for name in feature_set_names])
    if _filter:
        queries.extend([getattr(fset, name) == value for name, value in _filter.items()])

    p = db.query(
        models.FeatureSetKey.feature_set_id,
        func.string_agg(models.FeatureSetKey.key_column_name, literal_column("'|'"), type_=String). \
            label('pk_columns'),
        func.string_agg(models.FeatureSetKey.key_column_data_type, literal_column("'|'"), type_=String). \
            label('pk_types')
    ). \
        group_by(models.FeatureSetKey.feature_set_id). \
        subquery('p')

    num_feats = db.query(
        models.Feature.feature_set_id,
        func.count(models.Feature.feature_id).label('num_features')
    ). \
        group_by(models.Feature.feature_set_id). \
        subquery('nf')

    q = db.query(
        fset,
        num_feats.c.num_features,
        p.c.pk_columns,
        p.c.pk_types). \
        join(p, fset.feature_set_id == p.c.feature_set_id). \
        outerjoin(num_feats, fset.feature_set_id == num_feats.c.feature_set_id). \
        filter(and_(*queries))

    for fs, nf, pk_columns, pk_types in q.all():
        pkcols = pk_columns.split('|')
        pktypes = pk_types.split('|')
        primary_keys = {c: sql_to_datatype(k) for c, k in zip(pkcols, pktypes)}
        feature_sets.append(schemas.FeatureSetDetail(**fs.__dict__, primary_keys=primary_keys, num_features=nf))
    return feature_sets


def get_training_views(db: Session, _filter: Dict[str, Union[int, str]] = None) -> List[schemas.TrainingView]:
    """
    Returns a list of all available training views with an optional filter

    :param db: SqlAlchemy Session
    :param _filter: Dictionary container the filter keyword (label, description etc) and the value to filter on
        If None, will return all TrainingViews
    :return: List[TrainingView]
    """
    training_views = []

    p = db.query(
        models.TrainingViewKey.view_id,
        func.string_agg(models.TrainingViewKey.key_column_name, literal_column("','"), type_=String). \
            label('pk_columns')
    ). \
        filter(models.TrainingViewKey.key_type == 'P'). \
        group_by(models.TrainingViewKey.view_id). \
        subquery('p')

    c = db.query(
        models.TrainingViewKey.view_id,
        func.string_agg(models.TrainingViewKey.key_column_name, literal_column("','"), type_=String). \
            label('join_columns')
    ). \
        filter(models.TrainingViewKey.key_type == 'J'). \
        group_by(models.TrainingViewKey.view_id). \
        subquery('c')

    tv = aliased(models.TrainingView, name='tc')

    q = db.query(
        tv.view_id,
        tv.name,
        tv.description,
        cast(tv.sql_text, String(1000)).label('view_sql'),
        p.c.pk_columns,
        tv.ts_column,
        tv.label_column,
        c.c.join_columns). \
        join(p, tv.view_id == p.c.view_id). \
        join(c, tv.view_id == c.c.view_id)

    if _filter:
        q = q.filter(and_(*[getattr(tv, name) == value for name, value in _filter.items()]))

    for tv in q.all():
        t = tv._asdict()
        # DB doesn't support lists so it stores , separated vals in a string
        t['pk_columns'] = t.pop('pk_columns').split(',')
        t['join_columns'] = t.pop('join_columns').split(',')
        training_views.append(schemas.TrainingView(**t))
    return training_views


def get_training_view_id(db: Session, name: str) -> int:
    """
    Returns the view_id for the given training view

    :param db: SqlAlchemy Session
    :param name: The name of the training view
    :return: in
    """
    view = db.query(models.TrainingView.view_id). \
        filter(func.upper(models.TrainingView.name) == name.upper()). \
        first()
    return view[0] if view else None


def get_feature_descriptions_by_name(db: Session, names: List[str], sort: bool = True) -> List[schemas.FeatureDetail]:
    """
    Returns a dataframe or list of features whose names are provided

    :param db: SqlAlchemy Session
    :param names: The list of feature names
    :return: List[Feature] The list of Feature objects and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    f = aliased(models.Feature, name='f')
    fset = aliased(models.FeatureSet, name='fset')

    df = db.query(fset.schema_name, fset.table_name, fset.deployed, f). \
        select_from(f). \
        join(fset, f.feature_set_id == fset.feature_set_id)

    # If they don't pass in feature names, get all features 
    if names:
        df = df.filter(func.upper(f.name).in_([name.upper() for name in names]))

    features = []
    for schema, table, deployed, feat in df.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = model_to_schema_feature(feat)
        features.append(schemas.FeatureDetail(**f.__dict__, feature_set_name=f'{schema}.{table}', deployed=deployed))

    if sort and names:
        indices = {v.upper(): i for i, v in enumerate(names)}
        features = sorted(features, key=lambda f: indices[f.name.upper()])
    return features


def feature_search(db: Session, fs: schemas.FeatureSearch) -> List[schemas.FeatureDetail]:
    """
    Returns a list of FeatureDetails based on the provided search criteria

    :param fs:
    :return:
    """
    f = aliased(models.Feature, name='f')
    fset = aliased(models.FeatureSet, name='fset')

    q = db.query(fset.schema_name, fset.table_name, fset.deployed, f). \
        select_from(f). \
        join(fset, f.feature_set_id == fset.feature_set_id)

    # If there's a better way to do this we should fix it
    for col, comp in fs:
        table = fset if col in ('schema_name', 'table_name', 'deployed') else f  # These columns come from feature set
        if not comp:
            continue
        if type(comp) in (bool, str):  # No dictionary, simple comparison
            q = q.filter(getattr(table, col) == comp)
        elif isinstance(comp, list):
            for tag in comp:
                q = q.filter(table.tags.like(f"'{tag}'"))
        elif col == 'attributes':  # Special comparison for attributes
            for k, v in comp.items():
                q = q.filter(f.attributes.like(f"%'{k}':'{v}'%"))
        elif col == 'last_update_ts':  # Timestamp Comparisons
            for k, val in comp.items():  # I wonder if there's a better way to do this
                if k == 'gt':  # cast(tc.sql_text, String(1000))
                    q = q.filter(f.last_update_ts > TextClause(f"'{str(val)}'"))
                elif k == 'gte':
                    q = q.filter(f.last_update_ts >= TextClause(f"'{str(val)}'"))
                elif k == 'eq':
                    q = q.filter(f.last_update_ts == TextClause(f"'{str(val)}'"))
                elif k == 'lte':
                    q = q.filter(f.last_update_ts <= TextClause(f"'{str(val)}'"))
                elif k == 'lt':
                    q = q.filter(f.last_update_ts < TextClause(f"'{str(val)}'"))
        else:  # Rest are just 1 element dictionaries
            c, val = list(comp.items())[0]
            if c == 'is':
                q = q.filter(getattr(table, col) == val)
            elif c == 'like':
                q = q.filter(func.upper(getattr(table, col)).like(f'%{val.upper()}%'))

    features = []
    for schema, table, deployed, feat in q.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = model_to_schema_feature(feat)
        features.append(schemas.FeatureDetail(**f.__dict__, feature_set_name=f'{schema}.{table}', deployed=deployed))

    return features


def _get_feature_set_counts(db) -> List[Tuple[bool, int]]:
    """
    Returns the counts of undeployed and deployed feature set as a list of tuples
    """

    fset = aliased(models.FeatureSet, name='fset')
    return db.query(fset.deployed, func.count(fset.feature_set_id)).group_by(fset.deployed).all()


def _get_feature_counts(db) -> List[Tuple[bool, int]]:
    """
    Returns the counts of undeployed and deployed features as a list of tuples
    """

    fset = aliased(models.FeatureSet, name='fset')
    f = aliased(models.Feature, name='f')
    return db.query(fset.deployed, func.count(fset.feature_set_id)). \
        join(f, f.feature_set_id == fset.feature_set_id). \
        group_by(fset.deployed).all()


def _get_num_training_sets(db) -> int:
    """
    Returns the number of training sets
    """
    return db.query(models.TrainingSet).count()


def _get_num_training_views(db) -> int:
    """
    Returns the number of training views
    """
    return db.query(models.TrainingView).count()


def _get_num_deployments(db) -> int:
    """
    Returns the number of actively deployed models that were trained using training sets from the feature store
    """
    return db.query(models.Deployment).count()


def _get_num_pending_feature_sets(db) -> int:
    """
    Returns the number of feature sets pending deployment
    """
    pend = aliased(models.PendingFeatureSetDeployment, name='pend')
    return db.query(pend). \
        filter(pend.status == 'PENDING'). \
        count()


def _get_num_created_models(db) -> int:
    """
    Returns the number of models that have been created and trained with a feature store training set.
    This corresponds to the number of mlflow runs with the tag splice.model_name set and the parameter
    splice.feature_store.training_set set.
    """
    return db.query(SqlRun). \
        join(SqlTag, SqlRun.run_uuid == SqlTag.run_uuid). \
        join(SqlParam, SqlRun.run_uuid == SqlParam.run_uuid). \
        filter(SqlParam.key == 'splice.feature_store.training_set'). \
        filter(SqlTag.key == 'splice.model_name'). \
        count()


def get_recent_features(db: Session, n: int = 5) -> List[str]:
    """
    Gets the top n most recently added features to the feature store

    :param db: Session
    :param n: How many features to get. Default 5
    :return: List[str] Feature names
    """
    res = db.query(models.Feature.name).order_by(desc(models.Feature.last_update_ts)).limit(n).all()
    return [i for (i,) in res]


def get_most_used_features(db: Session, n=5) -> List[str]:
    """
    Gets the top n most used features (where most used means in the most number of deployments)

    :param db: Session
    :param n: How many to return. Default 5
    :return: List[str] Feature Names
    """
    p = db.query(models.Deployment.training_set_id).subquery('p')
    p1 = db.query(models.TrainingSetFeature.feature_id).filter(models.TrainingSetFeature.training_set_id.in_(p))
    res = db.query(models.Feature.name, func.count().label('feat_count')). \
        filter(models.Feature.feature_id.in_(p1)). \
        group_by(models.Feature.name). \
        subquery('feature_count')
    res = db.query(res.c.name).order_by(res.c.feat_count).limit(n).all()
    return [i for (i,) in res]


def get_fs_summary(db: Session) -> schemas.FeatureStoreSummary:
    """
    This function returns a summary of the feature store including:
        * Number of feature sets
        * Number of deployed feature sets
        * Number of features
        * Number of deployed features
        * Number of training sets
        * Number of training views
        * Number of associated models - this is a count of the MLManager.RUNS table where the `splice.model_name` tag is set and the `splice.feature_store.training_set` parameter is set
        * Number of active (deployed) models (that have used the feature store for training)
        * Number of pending feature sets - this will will require a new table `featurestore.pending_feature_set_deployments` and it will be a count of that
    """
    feature_set_counts = _get_feature_set_counts(db)
    num_fsets = sum(i[1] for i in feature_set_counts)
    num_deployed_fsets = sum(i[1] for i in feature_set_counts if i[0])  # Only sum the ones that have Deployed=True

    feature_counts = _get_feature_counts(db)
    num_feats = sum(i[1] for i in feature_counts)
    num_deployed_feats = sum(i[1] for i in feature_counts if i[0])  # Only sum the ones that have Deployed=True

    num_training_sets = _get_num_training_sets(db)
    num_training_views = _get_num_training_views(db)
    num_created_models = _get_num_created_models(db)
    num_deployemnts = _get_num_deployments(db)
    num_pending_feature_set_deployments = _get_num_pending_feature_sets(db)

    recent_features = get_recent_features(db, 5)
    most_used_features = get_most_used_features(db, 5)

    return schemas.FeatureStoreSummary(
        num_feature_sets=num_fsets,
        num_deployed_feature_sets=num_deployed_fsets,
        num_features=num_feats,
        num_deployed_features=num_deployed_feats,
        num_training_sets=num_training_sets,
        num_training_views=num_training_views,
        num_models=num_created_models,
        num_deployed_models=num_deployemnts,
        num_pending_feature_set_deployments=num_pending_feature_set_deployments,
        recent_features=recent_features,
        most_used_features=most_used_features
    )


def update_feature_metadata(db: Session, name: str, desc: str = None,
                            tags: List[str] = None, attributes: Dict[str, str] = None) -> schemas.Feature:
    """
    Updates the metadata of a feature
    :param db: Session
    :param desc: New description of the feature
    :param tags: New tags of the feature
    :param attributes: New attributes of the feature
    """
    updates = {}
    if desc:
        updates['description'] = desc
    if tags:
        updates['tags'] = ','.join(tags)
    if attributes:
        updates['attributes'] = json.dumps(attributes)
    feat = db.query(models.Feature).filter(models.Feature.name == name)
    feat.update(updates)
    db.flush()
    return model_to_schema_feature(feat.first())


def get_features_by_name(db: Session, names: List[str]) -> List[schemas.FeatureDetail]:
    """
    Returns a dataframe or list of features whose names are provided

    :param db: SqlAlchemy Session
    :param names: The list of feature names
    :return: List[Feature] The list of Feature objects and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    f = aliased(models.Feature, name='f')
    fset = aliased(models.FeatureSet, name='fset')

    df = db.query(f, fset.schema_name, fset.table_name, fset.deployed). \
        select_from(f). \
        join(fset, f.feature_set_id == fset.feature_set_id)

    # If they don't pass in feature names, get all features 
    if names:
        df = df.filter(func.upper(f.name).in_([name.upper() for name in names]))

    return df.all()


def get_features_by_id(db: Session, ids: List[int]) -> List[schemas.Feature]:
    """
    Returns a dataframe or list of features whose IDs are provided

    :param db: SqlAlchemy Session
    :param names: The list of feature names
    :return: List[Feature] The list of Feature objects and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    f = aliased(models.Feature, name='f')
    df = db.query(f).filter(f.feature_id.in_(ids))
    return [model_to_schema_feature(f) for f in df.all()]


def get_feature_vector_sql(db: Session, features: List[schemas.Feature], tctx: schemas.TrainingView) -> str:
    """
    Returns the parameterized feature retrieval SQL used for online model serving.

    :param db: SqlAlchemy Session
    :param features: (List[Feature]) the list of features from the feature store to be included in the training
    :param training_view: (str) The registered training view

        :NOTE:
            .. code-block:: text

                This function will error if the view SQL is missing a view key required to retrieve the\
                desired features

    :return: (str) the parameterized feature vector SQL
    """

    sql = 'SELECT '

    # SELECT expressions
    for pkcol in tctx.pk_columns:  # Select primary key column(s)
        sql += f'\n\t{{p_{pkcol}}} {pkcol},'

    for feature in features:
        sql += f'\n\tfset{feature.feature_set_id}.{feature.name}, '  # Collect all features over time
    sql = sql.rstrip(', ')

    # FROM clause
    sql += f'\nFROM '

    # JOIN clause
    feature_set_ids = list({f.feature_set_id for f in features})  # Distinct set of IDs
    feature_sets = get_feature_sets(db, feature_set_ids)
    where = '\nWHERE '
    for fset in feature_sets:
        # Join Feature Set
        sql += f'\n\t{fset.schema_name}.{fset.table_name} fset{fset.feature_set_id}, '
        for pkcol in __get_pk_columns(fset):
            where += f'\n\tfset{fset.feature_set_id}.{pkcol}={{p_{pkcol}}} AND '

    sql = sql.rstrip(', ')
    where = where.rstrip('AND ')
    sql += where

    return sql


def register_feature_set_metadata(db: Session, fset: schemas.FeatureSetCreate) -> schemas.FeatureSet:
    fset_metadata = models.FeatureSet(schema_name=fset.schema_name, table_name=fset.table_name,
                                      description=fset.description)
    db.add(fset_metadata)
    db.flush()

    fsid = fset_metadata.feature_set_id

    for pk in __get_pk_columns(fset):
        pk_metadata = models.FeatureSetKey(feature_set_id=fsid,
                                           key_column_name=pk.upper(),
                                           key_column_data_type=datatype_to_sql(fset.primary_keys[pk]))
        db.add(pk_metadata)
    return schemas.FeatureSet(**fset.__dict__, feature_set_id=fsid)


def register_feature_metadata(db: Session, f: schemas.FeatureCreate) -> schemas.Feature:
    """
    Registers the feature's existence in the feature store
    :param db: SqlAlchemy Session
    :param f: (Feature) the feature to register
    :return: The feature metadata
    """
    feature = models.Feature(
        feature_set_id=f.feature_set_id, name=f.name, description=f.description,
        feature_data_type=datatype_to_sql(f.feature_data_type),
        feature_type=f.feature_type, tags=','.join(f.tags) if f.tags else None,
        attributes=json.dumps(f.attributes) if f.attributes else None
    )
    db.add(feature)
    db.flush()
    return model_to_schema_feature(feature)


def bulk_register_feature_metadata(db: Session, feats: List[schemas.FeatureCreate]) -> None:
    """
    Registers many features' existences in the feature store
    :param db: SqlAlchemy Session
    :param feats: (List[Feature]) the features to register
    :return: None
    """

    features: List[models.Feature] = [
        models.Feature(
            feature_set_id=f.feature_set_id,
            name=f.name,
            description=f.description,
            feature_data_type=datatype_to_sql(f.feature_data_type),
            feature_type=f.feature_type,
            tags=','.join(f.tags) if f.tags else None,
            attributes=json.dumps(f.attributes) if f.attributes else None
        )
        for f in feats
    ]
    db.bulk_save_objects(features)


def process_features(db: Session, features: List[Union[schemas.Feature, str]]) -> List[schemas.Feature]:
    """
    Process a list of Features parameter. If the list is strings, it converts them to Features, else returns itself

    :param db: SqlAlchemy Session
    :param features: The list of Feature names or Feature objects
    :return: List[Feature]
    """
    feat_str = [f for f in features if isinstance(f, str)]
    str_to_feat = get_feature_descriptions_by_name(db, names=feat_str) if feat_str else []
    all_features = str_to_feat + [f for f in features if not isinstance(f, str)]
    if not all([isinstance(i, schemas.Feature) for i in all_features]):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message="It seems you've passed in Features that are neither" \
                                             " a feature name (string) or a Feature object")
    if len(all_features) != len(features):
        old_names = set([(f if isinstance(f, str) else f.name).upper() for f in features])
        new_names = set([f.name.upper() for f in all_features])
        missing = ', '.join(old_names - new_names)
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f'Could not find the following features: {missing}')
    return all_features


def deploy_feature_set(db: Session, fset: schemas.FeatureSet) -> schemas.FeatureSet:
    """
    Deploys the current feature set. Equivalent to calling fs.deploy(schema_name, table_name)
    :param db: SqlAlchemy Session
    :param fset: The feature set
    :return: List[Feature]
    """
    new_pk_cols = ','.join(f'NEWW.{p}' for p in __get_pk_columns(fset))
    new_feature_cols = ','.join(f'NEWW.{f.name}' for f in get_features(db, fset))

    metadata = MetaData(db.get_bind())

    feature_list = get_feature_column_str(db, fset)
    insert_trigger_sql = SQL.feature_set_trigger.format(
        schema=fset.schema_name, table=fset.table_name, action='INSERT',
        pk_list=get_pk_column_str(fset), feature_list=feature_list,
        new_pk_cols=new_pk_cols, new_feature_cols=new_feature_cols)
    update_trigger_sql = SQL.feature_set_trigger.format(
        schema=fset.schema_name, table=fset.table_name, action='UPDATE',
        pk_list=get_pk_column_str(fset), feature_list=feature_list,
        new_pk_cols=new_pk_cols, new_feature_cols=new_feature_cols)

    logger.info('Creating Feature Set...')
    pk_columns = _sql_to_sqlalchemy_columns(fset.primary_keys, True)
    ts_columns = [Column('last_update_ts', TIMESTAMP, nullable=False)]
    feature_columns = _sql_to_sqlalchemy_columns({f.name.lower(): f.feature_data_type
                                                  for f in get_features(db, fset)}, False)
    columns = pk_columns + ts_columns + feature_columns
    feature_set = Table(fset.table_name.lower(), metadata, *columns, schema=fset.schema_name.lower())
    feature_set.create(db.connection())
    logger.info('Done.')

    logger.info('Creating Feature Set History...')

    pk_columns = _sql_to_sqlalchemy_columns(fset.primary_keys, True)
    ts_columns = [
        Column('asof_ts', TIMESTAMP, primary_key=True),
        Column('ingest_ts', TIMESTAMP, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    ]
    feature_columns = _sql_to_sqlalchemy_columns({f.name.lower(): f.feature_data_type
                                                  for f in get_features(db, fset)}, False)
    columns = pk_columns + ts_columns + feature_columns

    history = Table(f'{fset.table_name.lower()}_history', metadata, *columns, schema=fset.schema_name.lower())
    history.create(db.connection())
    logger.info('Done.')

    logger.info('Creating Historian Triggers...')
    # TODO: Add third trigger to ensure online table has newest value
    insert_trigger = DDL(insert_trigger_sql)
    db.execute(insert_trigger)
    update_trigger = DDL(update_trigger_sql)
    db.execute(update_trigger)
    logger.info('Done.')

    logger.info('Updating Metadata...')
    # Due to an issue with our sqlalchemy driver, we cannot update in the standard way with timestamps. TODO
    # So we create the update, compile it, and execute it directly
    # db.query(models.FeatureSet).filter(models.FeatureSet.feature_set_id==fset.feature_set_id).\
    #     update({models.FeatureSet.deployed: True, models.FeatureSet.deploy_ts: datetime.now()})
    updt = update(models.FeatureSet).where(models.FeatureSet.feature_set_id == fset.feature_set_id). \
        values(deployed=True, deploy_ts=text('CURRENT_TIMESTAMP'))
    stmt = updt.compile(dialect=db.get_bind().dialect, compile_kwargs={"literal_binds": True})
    db.execute(str(stmt))
    fset.deployed = True
    logger.info('Done.')
    return fset


def validate_training_view(db: Session, tv: schemas.TrainingViewCreate) -> None:
    """
    Validates that the training view doesn't already exist, that the provided sql is valid, and that the pk_cols and
    label_col provided are valid

    :param db: SqlAlchemy Session
    :param tv: TrainingView
    :return: None
    """
    # Validate name doesn't exist
    if len(get_training_views(db, _filter={'name': tv.name})) > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS,
                                     message=f"Training View {tv.name} already exists!")

    # Column comparison
    # Lazily evaluate sql resultset, ensure that the result contains all columns matching pks, join_keys, tscol and label_col
    from sqlalchemy.exc import ProgrammingError
    try:
        sql = f'select * from ({tv.sql_text}) x where 1=0'  # So we get column names but don't actually execute their sql
        valid_df = db.execute(sql)
    except ProgrammingError as e:
        if '[Splice Machine][Splice]' in str(e):
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                         message=f'The provided SQL is incorrect. The following error was raised during '
                                                 f'validation:\n\n{str(e)}') from None
        raise e

    # Ensure the label column specified is in the output of the SQL
    if tv.label_column and not tv.label_column.upper() in valid_df.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided label column {tv.label_column} "
                                             f"is not available in the provided SQL")

    # Ensure timestamp column is in the SQL
    if tv.ts_column.upper() not in valid_df.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided timestamp column {tv.ts_column} "
                                             f"is not available in the provided SQL")

    # Ensure the primary key columns are in the output of the SQL
    all_pks = [key.upper() for key in tv.pk_columns]
    pks = set(all_pks)
    missing_keys = pks - set(valid_df.keys())
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided primary key(s) {missing_keys} are not available in the provided SQL")

    # Check for duplicate primary keys
    dups = ([key.lower() for key, count in Counter(all_pks).items() if count > 1])
    if dups:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"You cannot provide multiple of the same primary key: remove the duplicates"
                                             f" {dups}")

    # Confirm that all join_keys provided correspond to primary keys of created feature sets
    jks = set(i[0].upper() for i in db.query(distinct(models.FeatureSetKey.key_column_name)).all())
    missing_keys = set(i.upper() for i in tv.join_columns) - jks
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"Not all provided join keys exist. Remove {missing_keys} or " \
                                             f"create a feature set that uses the missing keys")

    # Ensure the join key columns are in the output of the SQL
    all_jks = [key.upper() for key in tv.join_columns]
    jks = set(all_jks)
    missing_keys = jks - set(valid_df.keys())
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided join key(s) {missing_keys} are not available in the provided SQL")

    # Check for duplicate join keys
    dups = ([key.lower() for key, count in Counter(all_jks).items() if count > 1])
    if dups:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"You cannot provide multiple of the same join key: remove the duplicates"
                                             f" {dups}")


def create_training_view(db: Session, tv: schemas.TrainingViewCreate) -> None:
    """
    Registers a training view for use in generating training SQL

    :param db: SqlAlchemy Session
    :param tv: The training view to register
    :return: None
    """
    logger.info('Building training sql...')
    train = models.TrainingView(name=tv.name, description=tv.description or 'None Provided', sql_text=tv.sql_text,
                                ts_column=tv.ts_column,
                                label_column=tv.label_column)
    db.add(train)
    db.flush()
    logger.info('Done.')

    # Get generated view ID
    vid = train.view_id

    logger.info('Creating Join Keys')
    for i in tv.join_columns:
        logger.info(f'\tCreating Join Key {i}...')
        key = models.TrainingViewKey(view_id=vid, key_column_name=i.upper(), key_type='J')
        db.add(key)
    logger.info('Done.')
    logger.info('Creating Training View Primary Keys')
    for i in tv.pk_columns:
        logger.info(f'\tCreating Primary Key {i}...')
        key = models.TrainingViewKey(view_id=vid, key_column_name=i.upper(), key_type='P')
        db.add(key)
    logger.info('Done.')


def get_source(db: Session, name: str = None, pipe_id: int = None) -> schemas.Source:
    """
    Gets a Source by name

    :param db: Session
    :param name: Source name
    :return: Source
    """

    s = db.query(models.Source)
    if name:
        s = s.filter(func.upper(models.Source.name) == name.upper())
    if pipe_id:
        s = s.filter(models.Source.source_id == pipe_id)
    s = s.first()
    if s:
        sk = db.query(models.SourceKey.key_column_name).filter(models.SourceKey.source_id == s.source_id).all()
        sch = s.__dict__
        sch['pk_columns'] = [i for (i,) in sk]
        return schemas.Source(**sch)


def get_pipeline_source(db: Session, id) -> schemas.Source:
    """
    Gets the source of a particular Pipeline (since a Pipeline has only 1 source)

    :param db: Session
    :param id: Pipeline's source_id
    :return: Source
    """
    return get_source(db, pipe_id=id)


def get_source_dependencies(db: Session, id: int) -> List[str]:
    """
    Returns the dependencies of a Source (typically a Pipeline/Feature Set)

    :param db: SQLAlchemy Session
    :param id: The Source ID
    :return: The name of the Feature Set being fed by a Pipeline reading from this Source
    """
    p = db.query(models.Pipeline.feature_set_id). \
        filter(models.Pipeline.source_id == id).subquery('p')
    res = db.query(models.FeatureSet.schema_name, models.FeatureSet.table_name). \
        filter(models.FeatureSet.feature_set_id.in_(p)).all()
    fset_names = [f'{s}.{t}' for s, t in res]
    return fset_names


def get_source_pk_types(db: Session, source: schemas.Source) -> Dict[str, schemas.DataType]:
    """
    Gets the primary key column names and SQL data types for a source

    :param db: Session
    :param source: The Source in question
    :return: Primary Keys with their types
    """
    lazy_sql = f'select * from ({source.sql_text}) x where 1=0'
    valid_df = db.execute(lazy_sql)
    col_descriptions = valid_df.cursor.description
    pk_types = {}
    pks = [d.lower() for d in source.pk_columns]
    for d in col_descriptions:
        if d[0].lower() in pks:
            col, py_type = d[0], d[1]
            dtype = Converters.PY_DB_CONVERSIONS[py_type]
            if py_type == Decimal:  # handle precision and recall
                prec, scale = d[3], d[5]
                data_type = schemas.DataType(data_type=dtype, prec=prec, scale=scale)
            elif py_type == str:  # handle varchar length
                length = d[3]
                data_type = schemas.DataType(data_type=dtype, length=length)
            else:
                data_type = schemas.DataType(data_type=dtype)
            pk_types[col] = data_type
    return pk_types


def validate_source(db: Session, name, sql_text, pk_columns, event_ts_column, update_ts_column) -> None:
    """
    Validates that the Source doesn't already exist, that the provided sql is valid, and that the pk_cols and
    event_ts_column/update_ts_column provided are valid (meaning they exist from the SQL)

    :param db: SqlAlchemy Session
    :param name: The source name
    :param sql: The source provided SQL
    :param pk_columns: The primary keys of the source
    :param event_ts_column: The event_ts_col name
    :param event_ts_column: The update_ts_col name
    :return: None
    """
    # Validate name doesn't exist
    if get_source(db, name):
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS,
                                     message=f"Source {name} already exists!")

    # Column comparison
    # Lazily evaluate sql resultset, ensure that the result contains all columns matching pks, join_keys, tscol and label_col
    from sqlalchemy.exc import ProgrammingError
    try:
        sql = f'select * from ({sql_text}) x where 1=0'  # So we get column names but don't actually execute their sql
        valid_df = db.execute(sql)
    except ProgrammingError as e:
        if '[Splice Machine][Splice]' in str(e):
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                         message=f'The provided SQL is incorrect. The following error was raised during '
                                                 f'validation:\n\n{str(e)}') from None
        raise e

    if valid_df.is_insert:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided SQL seems to be an insert statement. Source SQL must be a "
                                             f"SELECT")
    if not valid_df.returns_rows:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided does not seem to be a SELECT statement that should return "
                                             f"rows. The provided source SQL must be able to return rows.")

    # Ensure the event_ts_column specified is in the output of the SQL
    if event_ts_column.upper() not in valid_df.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided event_ts_column column {event_ts_column} is not available in the provided SQL")

    # Ensure the event_ts_column specified is in the output of the SQL
    if update_ts_column.upper() not in valid_df.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided update_ts_column column {update_ts_column} is not available in the provided SQL")

    # Ensure the primary key columns are in the output of the SQL
    pks = set(key.upper() for key in pk_columns)
    missing_keys = pks - set(valid_df.keys())
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                     message=f"Provided primary key(s) {missing_keys} are not available in the provided SQL")


def create_source(db: Session, name, sql_text, pk_columns, event_ts_column, update_ts_column) -> None:
    """
    Creates a Source and stores the metadata in the Feature Store

    :param db: SqlAlchemy Session
    :param name: The source name
    :param sql: The source provided SQL
    :param pk_columns: The primary keys of the source
    :param event_ts_column: The event_ts_col name
    :param event_ts_column: The update_ts_col name
    :return: None
    """
    s = models.Source(
        name=name,
        sql_text=sql_text,
        event_ts_column=event_ts_column,
        update_ts_column=update_ts_column,

    )
    db.add(s)
    db.flush()  # Get source ID
    source_keys: List[models.SourceKey] = []
    for k in pk_columns:
        sk = models.SourceKey(
            source_id=s.source_id,
            key_column_name=k
        )
        source_keys.append(sk)
    db.bulk_save_objects(source_keys)


def delete_source(db: Session, id: int):
    """
    Deletes a Source and its Keys from the Feature Store

    :param db: SQLAlchemy Session
    :param id: Source ID
    """
    logger.info(f"Delete source keys for source {id}")
    db.query(models.SourceKey).filter(models.SourceKey.source_id == id).delete(synchronize_session='fetch')
    logger.info(f"Delete source {id}")
    db.query(models.Source).filter(models.Source.source_id == id).delete(synchronize_session='fetch')


def create_pipeline(db: Session, sf: schemas.SourceFeatureSetAgg, fset_id: int,
                    source_id: int, pipeline_url: str) -> None:
    """
    Registers a pipeline in the Feature Store that will be scheduled and executed by Airflow. This
    metadata is for tracking purposes only

    :param db: SqlAlchemy Session
    :param sf: SourceFeatureSetAgg
    :param fset_id: Feature Set ID for this Pipeline
    :param source_id: The ID of the Source SQL
    :param pipeline_url: The Airflow (or other ETL tool) URL
    """
    p = dict(
        feature_set_id=fset_id,
        source_id=source_id,
        pipeline_start_ts=str(sf.start_time),
        pipeline_interval=sf.schedule_interval,
        backfill_start_ts=str(sf.backfill_start_time),
        backfill_interval=sf.backfill_interval,
        pipeline_url=pipeline_url
    )
    logger.info("Adding pipeline")
    db.execute(SQL.pipeline.format(**p))
    db.flush()


def create_pipeline_aggregations(db: Session, pipeline_aggs: List[models.PipelineAgg]):
    """
    Creates the pipeline aggregation functions and registers them in the Feature Store

    bulk_register_feature_metadata
    :param pipeline_aggs: The pipeline aggregation functions to add
    """
    db.bulk_save_objects(pipeline_aggs)


def validate_feature_aggregations(db: Session, source: schemas.Source, fns: List[schemas.FeatureAggregation]) -> None:
    """
    Validates that the provided feature aggregations have column names that are in the provided SQL and that there
    aren't duplicates. It also validates that the source SQL still works.

    :param db: SqlAlchemy Session
    :param source: The source
    :param fns: The list of feature aggreagtions
    :return: None
    """

    # Column comparison
    # Even though this source has already been validated, something in the backend may have broken it (a table may have
    #  been dropped or modified etc)
    from sqlalchemy.exc import ProgrammingError
    try:
        sql = f'select * from ({source.sql_text}) x where 1=0'  # So we get column names but don't actually execute their sql
        valid_df = db.execute(sql)
    except ProgrammingError as e:
        if '[Splice Machine][Splice]' in str(e):
            raise SpliceMachineException(
                status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                message=f'The provided Source {source.name} has SQL that is no longer valid. You should work with '
                        f'an administrator or data engineer to address the issue with your Source SQL. This feature set'
                        f' cannot currently be created. The following error was raised during validation:\n\n{str(e)}'
            ) from None
        raise e

    # Ensure the columns chosen for feature aggregations are in the output of the SQL
    cols = set(f.column_name for f in fns)
    missing_cols = cols - set(valid_df.keys())
    if missing_cols:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"Provided columns {missing_cols} are not available in the provided SQL."
                                             f" Remove those feature aggregations or use a different Source")

    # Check for duplicate column prefixes
    prefixes = [f.feature_name_prefix for f in fns]
    dups = ([key for key, count in Counter(prefixes).items() if count > 1])
    if dups:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"You cannot provide multiple of the same feature prefix: remove the duplicates"
                                             f" {dups}")


def get_feature_aggregations(db: Session, fset_id: int) -> List[schemas.FeatureAggregation]:
    """
    Returns a list of feature aggregations for a particular feature set pipeline

    :param db: Session
    :param fset_id: The ID of the feature set
    :return: List[schemas.FeatureAggregations] the feature aggregations for that feature set pipeline
    """
    p = aliased(models.PipelineAgg, name='p')
    feat_aggs = db.query(
        p.feature_name_prefix, p.column_name, p.agg_functions, p.agg_windows, p.agg_default_value
    ).filter(p.feature_set_id == fset_id).all()
    feat_aggs: List[schemas.FeatureAggregation] = [
        schemas.FeatureAggregation(
            feature_name_prefix=fnp,
            column_name=column_name,
            agg_functions=json.loads(agg_functions),
            agg_windows=json.loads(agg_windows),
            agg_default_value=float(agg_default_val)
        )
        for fnp, column_name, agg_functions, agg_windows, agg_default_val in feat_aggs
    ]
    return feat_aggs


def get_pipeline(db: Session, fset_id: int) -> schemas.Pipeline:
    """
    Gets a pipeline from a feature set ID

    :param db: Session
    :param fset_id: Feature Set ID
    :return: the pipeline
    """
    return db.query(models.Pipeline).filter(models.Pipeline.feature_set_id == fset_id).first()


def get_last_pipeline_run(db: Session, fset_id: int) -> datetime:
    """
    Gets the timestamp of the last "extract" (run) of a Feature Set Pipeline. Since they run on a schedule
    :param db: SQLAlchemy Session
    :param fset_id: Feature Set ID that the Pipeline is feeding
    :return: datetime
    """
    res = db.query(func.max(models.PipelineOps.extract_up_to_ts)).filter(
        models.PipelineOps.feature_set_id == fset_id).first()
    return res[0] if res else None


def retrieve_training_set_metadata_from_deployment(db: Session, schema_name: str,
                                                   table_name: str) -> schemas.TrainingSetMetadata:
    """
    Reads Feature Store metadata to retrieve definition of training set used to train the specified model.
    :param schema_name: model schema name
    :param table_name: model table name
    :return:
    """
    d = aliased(models.Deployment, name='d')
    ts = aliased(models.TrainingSet, name='ts')
    tsi = aliased(models.TrainingSetInstance, name='tsi')
    tsf = aliased(models.TrainingSetFeature, name='tsf')
    tv = aliased(models.TrainingView, name='tv')
    f = aliased(models.Feature, name='f')

    deploy = db.query(
        tv.name,
        tsi.training_set_start_ts,
        tsi.training_set_end_ts,
        tsi.training_set_create_ts,
        tsi.training_set_version,
        func.string_agg(f.name, literal_column("','"), type_=String). \
            label('features'),
        tv.label_column.label('label')
    ). \
        select_from(d). \
        join(ts, d.training_set_id == ts.training_set_id). \
        join(tsi, d.training_set_version == tsi.training_set_version and tsi.training_set_id == d.training_set_id). \
        join(tsf, tsf.training_set_id == d.training_set_id). \
        outerjoin(tv, tv.view_id == ts.view_id). \
        join(f, tsf.feature_id == f.feature_id). \
        filter(and_(
        func.upper(d.model_schema_name) == schema_name.upper(),
        func.upper(d.model_table_name) == table_name.upper()
    )). \
        group_by(tv.name,
                 tsi.training_set_start_ts,
                 tsi.training_set_end_ts,
                 tsi.training_set_create_ts,
                 tv.label_column,
                 tsi.training_set_version
                 ).first()

    if not deploy:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f"No deployment found for {schema_name}.{table_name}")
    return schemas.TrainingSetMetadata(**deploy._asdict())


def delete_feature(db: Session, feature: models.Feature) -> None:
    db.delete(feature)


def get_deployments(db: Session, _filter: Dict[str, str] = None, feature: schemas.FeatureDetail = None,
                    feature_set: schemas.FeatureSet = None) -> List[schemas.DeploymentDetail]:
    d = aliased(models.Deployment, name='d')
    ts = aliased(models.TrainingSet, name='ts')
    tsi = aliased(models.TrainingSetInstance, name='tsi')
    f = aliased(models.Feature, name='f')
    tsf = aliased(models.TrainingSetFeature, name='tsf')

    q = db.query(ts.name, d, tsi.training_set_start_ts, tsi.training_set_end_ts, tsi.training_set_create_ts). \
        join(ts, ts.training_set_id == d.training_set_id). \
        join(tsi, tsi.training_set_version == d.training_set_version)

    if _filter:
        # if filter key is a column in Training_Set, get compare Training_Set column, else compare to Deployment column
        q = q.filter(and_(*[(getattr(ts, name) if hasattr(ts, name) else getattr(d, name)) == value
                            for name, value in _filter.items()]))
    elif feature:
        q = q.join(tsf, tsf.training_set_id == ts.training_set_id). \
            filter(tsf.feature_id == feature.feature_id)
    elif feature_set:
        p = db.query(f.feature_id).filter(f.feature_set_id == feature_set.feature_set_id)
        q = q.join(tsf, tsf.training_set_id == ts.training_set_id). \
            filter(tsf.feature_id.in_(p))

    deployments = []
    for name, deployment in q.all():
        deployments.append(schemas.DeploymentDetail(**deployment.__dict__, training_set_name=name))
    return deployments


def get_features_from_deployment(db: Session, tsid: int) -> List[schemas.Feature]:
    ids = db.query(models.TrainingSetFeature.feature_id). \
        filter(models.TrainingSetFeature.training_set_id == tsid). \
        subquery('ids')

    q = db.query(models.Feature).filter(models.Feature.feature_id.in_(ids))

    features = [model_to_schema_feature(f) for f in q.all()]
    return features


# Feature/FeatureSet specific

def get_features(db: Session, fset: schemas.FeatureSet) -> List[schemas.Feature]:
    """
    Get's all of the features from this featureset as a list of splicemachine.features.Feature

    :param db: SqlAlchemy Session
    :param fset: The feature set from which to get features
    :return: List[Feature]
    """
    features = []
    if fset.feature_set_id:
        features_rows = db.query(models.Feature). \
            filter(models.Feature.feature_set_id == fset.feature_set_id).all()
        features = [model_to_schema_feature(f) for f in features_rows]
    return features


def get_feature_column_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f.name for f in get_features(db, fset)])


def get_current_time(db: Session) -> datetime:
    return db.execute('VALUES(CURRENT_TIMESTAMP)').first()[0]
