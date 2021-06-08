from sqlalchemy.orm import Session, aliased
from typing import List, Dict, Union, Any, Tuple, Set
from . import schemas
from .constants import SQL
from sqlalchemy import desc, String, func, distinct, and_, or_, Column, literal_column, case, select
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.schema import MetaData, Table, PrimaryKeyConstraint, DDL
from sqlalchemy.types import TIMESTAMP
from shared.models import feature_store_models as models
from shared.services.database import SQLAlchemyClient, DatabaseFunctions, Converters
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.logger.logging_config import logger
from fastapi import status
import re
import json
from datetime import datetime
from collections import Counter
from .utils.utils import (__get_pk_columns, get_pk_column_str, datatype_to_sql,
                          sql_to_datatype, _sql_to_sqlalchemy_columns,
                          __validate_feature_data_type, __validate_primary_keys,
                          __get_table_name)
from .utils.feature_utils import model_to_schema_feature
from mlflow.store.tracking.dbmodels.models import SqlRun, SqlTag, SqlParam
from splicemachinesa.constants import RESERVED_WORDS

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

    if fset.schema_name.lower() in RESERVED_WORDS:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f'Feature set schema {fset.schema_name} is in the list of reserved words. '
                                             f'Feature set must not use a reserved schema/table name. For the full list see '
                                             f'https://github.com/splicemachine/splice_sqlalchemy/blob/master/splicemachinesa/constants.py')
    if fset.table_name.lower() in RESERVED_WORDS:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f'Feature set table {fset.table_name} is in the list of reserved words. '
                                             f'Feature set must not use a reserved schema/table name. For the full list see '
                                             f'https://github.com/splicemachine/splice_sqlalchemy/blob/master/splicemachinesa/constants.py')
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

    if name.lower() in RESERVED_WORDS:
        raise SpliceMachineException(
            status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
            message=f'Feature name {name} is in the list of reserved words. Feature name must not use a reserved column name. '
                    'For the full list see '
                    'https://github.com/splicemachine/splice_sqlalchemy/blob/master/splicemachinesa/constants.py')

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
            f'CREATE LOCAL TEMPORARY TABLE COLUMN_TEST_{tmp}({name.upper()} {sql_type})')  # Will be deleted when session ends
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


def get_training_view_features(db: Session, view: schemas.TrainingViewDetail) -> List[schemas.Feature]:
    """
    Returns the available features for the given a training view name

    :param db: SqlAlchemy Session
    :param view: The training view
    :return: A list of available Feature objects
    """
    m = db.query(models.FeatureSetVersion.feature_set_id, 
            func.max(models.FeatureSetVersion.feature_set_version).label('feature_set_version')). \
        group_by(models.FeatureSetVersion.feature_set_id). \
        subquery('m')
    fsk = db.query(
        models.FeatureSetKey.feature_set_id,
        models.FeatureSetKey.feature_set_version,
        models.FeatureSetKey.key_column_name,
        func.count().over(partition_by=[models.FeatureSetKey.feature_set_id, models.FeatureSetKey.feature_set_version]). \
            label('KeyCount')). \
        join(m, (m.c.feature_set_id == models.FeatureSetKey.feature_set_id) & 
            (m.c.feature_set_version == models.FeatureSetKey.feature_set_version)). \
        subquery('fsk')

    tc = aliased(models.TrainingViewVersion, name='tc')
    c = aliased(models.TrainingViewKey, name='c')
    f = aliased(models.Feature, name='f')
    fv = aliased(models.FeatureVersion, name='fv')

    match_keys = db.query(
        f.feature_id,
        fsk.c.feature_set_id,
        fsk.c.feature_set_version,
        fsk.c.KeyCount,
        func.count(distinct(fsk.c.key_column_name)). \
            label('JoinKeyMatchCount')). \
        select_from(tc). \
        join(c, (c.view_id == tc.view_id) & (c.view_version == tc.view_version) & (c.key_type == 'J')). \
        join(fsk, fsk.c.key_column_name == c.key_column_name). \
        join(fv, (fv.feature_set_id == fsk.c.feature_set_id) & (fv.feature_set_version == fsk.c.feature_set_version)). \
        join(f, f.feature_id == fv.feature_id). \
        filter((tc.view_id == view.view_id) & (tc.view_version == view.view_version)). \
        group_by(
        f.feature_id,
        fsk.c.feature_set_id,
        fsk.c.feature_set_version,
        fsk.c.KeyCount). \
        subquery('match_keys')

    fl = db.query(match_keys.c.feature_id, match_keys.c.feature_set_id, match_keys.c.feature_set_version). \
        filter(match_keys.c.JoinKeyMatchCount == match_keys.c.KeyCount). \
        subquery('fl')

    q = db.query(f, fl.c.feature_set_id, fl.c.feature_set_version). \
        join(fl, fl.c.feature_id == f.feature_id)

    features = []
    for f, fsid, fsv in q.all():
        feature = model_to_schema_feature(f)
        feature.feature_set_id = fsid
        feature.feature_set_version = fsv
        features.append(feature)
    return features


def feature_set_is_deployed(db: Session, fset_id: int) -> bool:
    """
    Returns if this feature set is deployed or not

    :param db:  SqlAlchemy Session
    :param feature_set_id: The Feature Set ID in question
    :return: True if the feature set is deployed
    """
    d = db.query(models.FeatureSetVersion). \
        filter((models.FeatureSetVersion.feature_set_id == fset_id) & 
            (models.FeatureSetVersion.deployed == True)). \
        count()
    return bool(d)


def delete_features_from_feature_set(db: Session, feature_set_id: int, version: int = None):
    """
    Deletes features for a particular feature set

    :param db: Database Session
    :param fset: Feature Set and Version tied to the features
    """
    # Delete feature stats
    logger.info("Removing feature stats")
    s = db.query(models.FeatureStats). \
        filter(models.FeatureStats.feature_set_id == feature_set_id)
    if version:
        s = s.filter(models.FeatureStats.feature_set_version == version)
    s.delete(synchronize_session='fetch')

    # Delete feature versions
    logger.info("Removing feature versions")
    d = db.query(models.FeatureVersion). \
        filter(models.FeatureVersion.feature_set_id == feature_set_id)
    if version:
        d = d.filter(models.FeatureVersion.feature_set_version == version)
    d.delete(synchronize_session='fetch')

    # Delete features
    logger.info("Removing features")
    f = db.query(models.FeatureVersion.feature_id).subquery('f')
    db.query(models.Feature). \
        filter(models.Feature.feature_id.notin_(f)). \
        delete(synchronize_session='fetch')


def delete_feature_set_keys(db: Session, feature_set_id: int, version: int):
    """
    Deletes feature set keys for a particular feature set

    :param db: Database Session
    :param fset: Feature Set and Version tied to the keys
    """
    # Delete feature set keys
    logger.info("Removing keys")
    d = db.query(models.FeatureSetKey). \
        filter(models.FeatureSetKey.feature_set_id == feature_set_id)
    if version:
        d = d.filter(models.FeatureSetKey.feature_set_version == version)
    d.delete(synchronize_session='fetch')

def delete_feature_set_version(db: Session, feature_set_id: int, version: int):
    """
    Deletes particular feature set version for a feature set

    :param db: Database Session
    :param fset: Feature Set Version to be deleted
    """
    # Delete feature set version
    logger.info("Removing version")
    d = db.query(models.FeatureSetVersion). \
        filter(models.FeatureSetVersion.feature_set_id == feature_set_id)
    if version:
        d = d.filter(models.FeatureSetVersion.feature_set_version == version)
    d.delete(synchronize_session='fetch')

    if not version or db.query(models.FeatureSetVersion). \
        filter(models.FeatureSetVersion.feature_set_id == feature_set_id). \
        count() == 0:
            delete_feature_set(db, feature_set_id)


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


def delete_training_view_keys(db: Session, tv: schemas.TrainingViewDetail):
    """
    Deletes training view keys for a particular training view

    :param db: Database Session
    :param tv: training view
    """
    # Delete keys
    logger.info("Removing Training View Keys")
    db.query(models.TrainingViewKey). \
        filter((models.TrainingViewKey.view_id == tv.view_id) &
            (models.TrainingViewKey.view_version == tv.view_version)). \
        delete(synchronize_session='fetch')

def delete_training_view_version(db: Session, tv: schemas.TrainingViewDetail):
    """
    Deletes particular training view version for a training view

    :param db: Database Session
    :param tv: training view
    """
    # Delete training view version
    logger.info("Removing version")
    db.query(models.TrainingViewVersion). \
        filter((models.TrainingViewVersion.view_id == tv.view_id) & 
            (models.TrainingViewVersion.view_version == tv.view_version)). \
        delete(synchronize_session='fetch')

    if db.query(models.TrainingViewVersion). \
        filter(models.TrainingViewVersion.view_id == tv.view_id). \
        count() == 0:
            delete_training_view(db, tv.view_id)

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
    # FIXME: We need to set this to the feature store user's username
    ts = models.TrainingSetInstance(training_set_id=tsm.training_set_id, training_set_version=tsm.training_set_version,
                                    training_set_start_ts=tsm.training_set_start_ts, training_set_end_ts=tsm.training_set_end_ts,
                                    training_set_create_ts=tsm.training_set_create_ts, last_update_username='CURRENT_USER') # FIXME: We need to set this to the feature store user's username
    db.add(ts)


def create_training_set(db: Session, ts: schemas.TrainingSet, name: str) -> int:
    """
    Creates a new record in the Training_Set table and returns the Training Set ID

    :param db: Session
    :param ts: The Training Set
    :param name: Training Set name
    :return: (int) the Training Set ID
    """
    ts = models.TrainingSet(name=name, view_id=ts.metadata.view_id, view_version=ts.metadata.view_version)
    db.add(ts)
    db.flush()
    return ts.training_set_id


def register_training_set_features(db: Session, tset_id: int, features: List[schemas.FeatureDetail], label: str = None):
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
            feature_set_id=f.feature_set_id,
            feature_set_version=f.feature_set_version,
            is_label=(f.name.lower() == label.lower()) if label else False,
        ) for f in features
    ]
    db.bulk_save_objects(feats)


def get_feature_set_dependencies(db: Session, fset: schemas.FeatureSetDetail) -> Dict[str, Set[Any]]:
    """
    Returns the model deployments and training sets that rely on the given feature set

    :param db:  SqlAlchemy Session
    :param feature_set_id: The Feature Set ID in question
    """
    # return db.query(models.FeatureSet.deployed).filter(models.FeatureSet.feature_set_id==feature_set_id).all()[0]
    fv = aliased(models.FeatureVersion, name='fv')
    tset = aliased(models.TrainingSet, name='tset')
    tset_feat = aliased(models.TrainingSetFeature, name='tset_feat')
    pipe = aliased(models.Pipeline, name='pipeline')
    d = aliased(models.Deployment, name='d')

    p = db.query(fv). \
        filter((fv.feature_set_id == fset.feature_set_id) & (fv.feature_set_version == fset.feature_set_version)). \
        subquery('p')
    p1 = db.query(tset_feat.training_set_id). \
        join(p, (tset_feat.feature_id == p.c.feature_id) & 
            (tset_feat.feature_set_id == p.c.feature_set_id) & 
            (tset_feat.feature_set_version == p.c.feature_set_version)). \
        subquery('p1')

    r = db.query(tset.training_set_id, d.model_schema_name, d.model_table_name). \
        select_from(tset). \
        join(d, d.training_set_id == tset.training_set_id, isouter=True). \
        filter(tset.training_set_id.in_(p1)).all()
    deps = dict(
        model=set([f'{schema}.{table}' for _, schema, table in r if schema and table]),
        training_set=set([tid for tid, _, _ in r])
    )
    return deps


def get_training_view_dependencies(db: Session, view: schemas.TrainingViewDetail) -> List[Dict[str, str]]:
    """
    Returns the mlflow run ID and model deployment name that rely on the given training view

    :param db:  SqlAlchemy Session
    :param feature_set_id: The Feature Set ID in question
    """
    tset = aliased(models.TrainingSet, name='tset')
    d = aliased(models.Deployment, name='d')

    p = db.query(tset.training_set_id). \
        filter((tset.view_id == view.view_id) & (tset.view_version == view.view_version)). \
        subquery('p')
    res = db.query(d.model_schema_name, d.model_table_name, d.run_id).filter(d.training_set_id.in_(p)).all()

    deps = [{
        'run_id': run_id,
        'deployment': f'{schema}.{table}'
    } for schema, table, run_id in res]

    return deps


def get_training_sets_from_view(db: Session, view: schemas.TrainingViewDetail) -> List[int]:
    """
    Returns a list of training set IDs that were created from the given training view ID
    
    :param db: SqlAlchemy Session
    :param vid: The training view ID
    """
    res = db.query(models.TrainingSet.training_set_id, models.TrainingSet.name). \
        filter((models.TrainingSet.view_id == view.view_id) & 
            (models.TrainingSet.view_version == view.view_version)). \
        all()
    return [(i, name) for i, name in res]


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
        ts.view_version,
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

def list_training_sets(db: Session, tvw_id: int = None) -> List[schemas.TrainingSetMetadata]:
    """
    Gets a list of training sets (Name, View ID, Training Set ID, Last_update_ts, Last_update_username, label)
    :param tvw_id: A training view ID. If provided, will return only training sets created from that view

    :return: List of Training Set Metadata (Name, View ID, Training Set ID, Last_update_ts, Last_update_username, label)
    """
    tv = aliased(models.TrainingView, name='tv')
    ts = aliased(models.TrainingSet, name='ts')
    tsf = aliased(models.TrainingSetFeature, name='tsf')
    f = aliased(models.Feature, name='f')

    q = db.query(
        ts.name, ts.training_set_id, ts.view_id, ts.view_version, ts.last_update_username, ts.last_update_ts,
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
        join(tv, (ts.view_id==tv.view_id) & (ts.view_version==tv.view_version), isouter=True)
    if tvw_id:
        q = q.filter(ts.view_id==tvw_id)

    tsms: List[schemas.TrainingSetMetadata] = []
    for name, tsid, view_id, view_version, user, ts, label in q.all():
        tsms.append(
            schemas.TrainingSetMetadata(
                name=name, training_set_id=tsid, view_id=view_id, view_version=view_version,
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
    fsv = aliased(models.FeatureSetVersion, name='fsv')

    p = db.query(
        models.FeatureSetKey.feature_set_id,
        models.FeatureSetKey.feature_set_version,
        func.string_agg(models.FeatureSetKey.key_column_name, literal_column("'|'"), type_=String). \
            label('pk_columns'),
        func.string_agg(models.FeatureSetKey.key_column_data_type, literal_column("'|'"), type_=String). \
            label('pk_types')
    ). \
        group_by(models.FeatureSetKey.feature_set_id,
            models.FeatureSetKey.feature_set_version). \
        subquery('p')

    num_feats = db.query(
        models.FeatureVersion.feature_set_id,
        models.FeatureVersion.feature_set_version,
        func.count(models.FeatureVersion.feature_id).label('num_features')
    ). \
        group_by(
            models.FeatureVersion.feature_set_id,
            models.FeatureVersion.feature_set_version). \
        subquery('nf')

    q = db.query(
        fset,
        fsv,
        num_feats.c.num_features,
        p.c.pk_columns,
        p.c.pk_types). \
        join(fsv, fset.feature_set_id == fsv.feature_set_id). \
        join(p, (fsv.feature_set_id == p.c.feature_set_id) & 
            (fsv.feature_set_version == p.c.feature_set_version)). \
        outerjoin(num_feats, (fsv.feature_set_id == num_feats.c.feature_set_id) & 
            (fsv.feature_set_version == num_feats.c.feature_set_version))

    queries = []
    if feature_set_ids:
        queries.append(fset.feature_set_id.in_(tuple(set(feature_set_ids))))
    if feature_set_names:
        queries.extend([and_(func.upper(fset.schema_name) == name.split('.')[0].upper(),
                             func.upper(fset.table_name) == name.split('.')[1].upper()) for name in feature_set_names])
    if _filter:
        version = _filter.pop('feature_set_version', None)
        if version:
            if version == 'latest':
                mv = db.query(models.FeatureSetVersion.feature_set_id, 
                            func.max(models.FeatureSetVersion.feature_set_version).label('feature_set_version')).\
                        group_by(fsv.feature_set_id).subquery('mv')
                q = q.join(mv, (fsv.feature_set_id == mv.c.feature_set_id) & (fsv.feature_set_version == mv.c.feature_set_version))
            else:
                queries.append(fsv.feature_set_version == version)
        queries.extend([getattr(fset, name) == value for name, value in _filter.items()])
    else:
        d = db.query(models.FeatureSetVersion.feature_set_id.label('feature_set_id'), 
                    func.max(models.FeatureSetVersion.feature_set_version).label('feature_set_version')). \
                filter(models.FeatureSetVersion.deployed == True). \
                group_by(models.FeatureSetVersion.feature_set_id)
        # Python numbers here will cause an error
        u = db.query(models.FeatureSetVersion.feature_set_id.label('feature_set_id'), 
                    func.max(models.FeatureSetVersion.feature_set_version).label('feature_set_version')).\
                group_by(models.FeatureSetVersion.feature_set_id). \
                having(func.sum(
                    case([(models.FeatureSetVersion.deployed == True, literal_column("1"))],
                    else_=literal_column("0")
                )) == literal_column("0"))
        mv = d.union(u).subquery('mv')
        q = q.join(mv, (fsv.feature_set_id == mv.c.feature_set_id) & (fsv.feature_set_version == mv.c.feature_set_version))

    if queries:
        q = q.filter(and_(*queries))

    for fs, fsv, nf, pk_columns, pk_types in q.all():
        pkcols = pk_columns.split('|')
        pktypes = pk_types.split('|')
        primary_keys = {c: sql_to_datatype(k) for c, k in zip(pkcols, pktypes)}
        detail = fs.__dict__
        detail.update(fsv.__dict__)
        feature_sets.append(schemas.FeatureSetDetail(**detail, primary_keys=primary_keys, num_features=nf))
    return feature_sets


def get_training_views(db: Session, _filter: Dict[str, Union[int, str]] = None) -> List[schemas.TrainingViewDetail]:
    """
    Returns a list of all available training views with an optional filter

    :param db: SqlAlchemy Session
    :param _filter: Dictionary container the filter keyword (label, description etc) and the value to filter on
        If None, will return all TrainingViews
    :return: List[TrainingView]
    """
    training_views = []

    p = db.query(
            models.TrainingViewKey.view_id, models.TrainingViewKey.view_version,
            func.string_agg(models.TrainingViewKey.key_column_name, literal_column("','"), type_=String). \
                label('pk_columns')
        ). \
        filter(models.TrainingViewKey.key_type == 'P'). \
        group_by(models.TrainingViewKey.view_id, models.TrainingViewKey.view_version). \
        subquery('p')

    c = db.query(
            models.TrainingViewKey.view_id, models.TrainingViewKey.view_version,
            func.string_agg(models.TrainingViewKey.key_column_name, literal_column("','"), type_=String). \
                label('join_columns')
        ). \
        filter(models.TrainingViewKey.key_type == 'J'). \
        group_by(models.TrainingViewKey.view_id, models.TrainingViewKey.view_version). \
        subquery('c')

    tv = aliased(models.TrainingView, name='tv')
    tvv = aliased(models.TrainingViewVersion, name='tvv')

    q = db.query(
        tv,
        tvv,
        p.c.pk_columns,
        c.c.join_columns). \
        join(tvv, tv.view_id == tvv.view_id). \
        join(p, (tvv.view_id == p.c.view_id) & (tvv.view_version == p.c.view_version)). \
        join(c, (tvv.view_id == c.c.view_id) & (tvv.view_version == c.c.view_version))

    filters = []
    mv = db.query(models.TrainingViewVersion.view_id, 
                func.max(models.TrainingViewVersion.view_version).label('view_version')).\
            group_by(models.TrainingViewVersion.view_id).\
            subquery('mv')
    if _filter:
        version = _filter.pop('view_version', None)
        if version:
            if version == 'latest':
                q = q.join(mv, (tvv.view_id == mv.c.view_id) & (tvv.view_version == mv.c.view_version))
            else:
                filters.append(tvv.view_version == version)
        else:
            q = q.join(mv, (tvv.view_id == mv.c.view_id) & (tvv.view_version == mv.c.view_version))
        filters.extend([getattr(tv, name) == value for name, value in _filter.items()])
    else:
        q = q.join(mv, (tvv.view_id == mv.c.view_id) & (tvv.view_version == mv.c.view_version))
    
    q = q.filter(and_(*filters))

    for view, view_version, pk_columns, join_columns in q.all():
        v = view.__dict__
        vv = view_version.__dict__
        vv.update(v)
        # DB doesn't support lists so it stores , separated vals in a string
        pk_columns = pk_columns.split(',')
        join_columns = join_columns.split(',')
        training_views.append(schemas.TrainingViewDetail(**vv, pk_columns=pk_columns, join_columns=join_columns))
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


def _construct_feature_detail_query(db: Session, f, fset, fv, fsv):
    d = db.query(fv.feature_id). \
        join(fsv, (fv.feature_set_id == fsv.feature_set_id) & (fv.feature_set_version == fsv.feature_set_version)). \
        filter(fsv.deployed). \
        subquery('d')

    return db.query(fset.schema_name, fset.table_name, f.feature_id.in_(d).label('deployed'), f). \
        select_from(f). \
        join(fv, f.feature_id == fv.feature_id). \
        join(fsv, (fv.feature_set_id == fsv.feature_set_id) & (fv.feature_set_version == fsv.feature_set_version)). \
        join(fset, fsv.feature_set_id == fset.feature_set_id)

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
    fv = aliased(models.FeatureVersion, name='fv')
    fsv = aliased(models.FeatureSetVersion, name='fsv')

    df = _construct_feature_detail_query(db, f, fset, fv, fsv)

    # If they don't pass in feature names, get all features 
    if names:
        df = df.filter(func.upper(f.name).in_([name.upper() for name in names]))

    features = []
    for schema, table, deployed, feat in df.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = model_to_schema_feature(feat)
        f.feature_set_name = f'{schema}.{table}'
        f.deployed = deployed
        features.append(schemas.FeatureDetail(**f.__dict__))

    if sort and names:
        indices = {v.upper(): i for i, v in enumerate(names)}
        features = sorted(features, key=lambda f: indices[f.name.upper()])
    return features

def get_latest_features(db: Session, names: List[str]) -> List[schemas.FeatureDetail]:
    f = aliased(models.Feature, name='f')
    fv = aliased(models.FeatureVersion, name='fv')
    fsv = aliased(models.FeatureSetVersion, name='fsv')

    l = db.query(fsv.feature_set_id, func.max(fsv.feature_set_version).label('feature_set_version')). \
        filter(fsv.deployed == True). \
        group_by(fsv.feature_set_id). \
        subquery('l')

    df = db.query(f, fv.feature_set_id, fv.feature_set_version). \
        join(fv, fv.feature_id == f.feature_id). \
        join(l, (fv.feature_set_id == l.c.feature_set_id) & (fv.feature_set_version == l.c.feature_set_version))

    if names:
        df = df.filter(func.upper(f.name).in_([name.upper() for name in names]))

    features = []
    for feat, feature_set_id, feature_set_version in df.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = model_to_schema_feature(feat)
        f.feature_set_id = feature_set_id
        f.feature_set_version = feature_set_version
        features.append(schemas.FeatureDetail(**f.__dict__))
    return features

def feature_search(db: Session, fs: schemas.FeatureSearch) -> List[schemas.FeatureDetail]:
    """
    Returns a list of FeatureDetails based on the provided search criteria

    :param fs:
    :return:
    """
    f = aliased(models.Feature, name='f')
    fset = aliased(models.FeatureSet, name='fset')
    fv = aliased(models.FeatureVersion, name='fv')
    fsv = aliased(models.FeatureSetVersion, name='fsv')

    q = _construct_feature_detail_query(db, f, fset, fv, fsv)

    # If there's a better way to do this we should fix it
    for col, comp in fs:
        table = fset if col in ('schema_name', 'table_name', 'deployed') else f  # These columns come from feature set
        if comp == None: # Because the comparitor may be "false" (ie deployed=False) but it's not None
            continue
        if type(comp) in (bool, str):  # No dictionary, simple comparison
            q = q.filter(getattr(table, col) == comp)
        elif isinstance(comp, list):
            for tag in comp:
                # Tag can be in the front, middle, or end of the list
                filter = [table.tags.like(f"{tag},%"),   # Tag is first in the list (tag,)
                          table.tags.like(f"%,{tag},%"), # Tag in the middle (,tag,)
                          table.tags.like(f"%,{tag}")]   # Tag is the last in the list (,tag)
                q = q.filter(or_(*filter))
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
    fsv = aliased(models.FeatureSetVersion, name='fsv')

    d = db.query(distinct(fsv.feature_set_id)).filter(fsv.deployed == True).subquery('d')
    a = db.query(fset.feature_set_id, fset.feature_set_id.in_(d).label('deployed')).subquery('a')
    return db.query(a.c.deployed, func.count(a.c.feature_set_id)).group_by(a.c.deployed).all()

def _get_feature_counts(db) -> List[Tuple[bool, int]]:
    """
    Returns the counts of undeployed and deployed features as a list of tuples
    """

    f = aliased(models.Feature, name='f')
    fsv = aliased(models.FeatureSetVersion, name='fsv')
    fv = aliased(models.FeatureVersion, name='fv')
    d = db.query(distinct(fv.feature_id)). \
        join(fsv, (fv.feature_set_id==fsv.feature_set_id) & (fv.feature_set_version==fsv.feature_set_version)). \
        filter(fsv.deployed == True). \
        subquery('d')
    a = db.query(f.feature_id, f.feature_id.in_(d).label('deployed')).subquery('a')
    return db.query(a.c.deployed, func.count(a.c.feature_id)).group_by(a.c.deployed).all()


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

def update_feature_set_deployment_status(db: Session, fset_id: int, deploy_status: bool):
    """
    Updates the deployment status of a feature set (for example, if someone is undeploying an fset).

    :param db: SqlAlchemy Session
    :param fset_id: Feature Set ID
    """
    db.query(models.FeatureSet).filter(models.FeatureSet.feature_set_id==fset_id).update({'deployed':deploy_status})

def update_feature_set_description(db: Session, fset_id: int, description: str):
    """
    Updates the deployment status of a feature set (for example, if someone is undeploying an fset).

    :param db: SqlAlchemy Session
    :param fset_id: Feature Set ID
    """
    db.query(models.FeatureSet).filter(models.FeatureSet.feature_set_id==fset_id).update({'description':description})

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

def get_latest_feature(db: Session, name: str):
    """
    Returns a feature from the latest version of the feature set

    :param db: SqlAlchemy Session
    :param name: The feature name
    :return: Feature The Feature object and its metadata. Note, this is not the Feature
    value, simply the describing metadata about the feature. To create a training dataset with Feature value, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    f = aliased(models.Feature, name='f')
    fset = aliased(models.FeatureSet, name='fset')
    fsv = aliased(models.FeatureSetVersion, name='fsv')
    fv = aliased(models.FeatureVersion, name='fv')

    mv = db.query(models.FeatureSetVersion.feature_set_id, 
                func.max(models.FeatureSetVersion.feature_set_version).label('feature_set_version')).\
            group_by(models.FeatureSetVersion.feature_set_id).\
            subquery('mv')

    df = db.query(f, fset.schema_name, fset.table_name, fsv.feature_set_version, fsv.deployed). \
        select_from(f). \
        join(fv, f.feature_id == fv.feature_id). \
        join(fsv, and_(fv.feature_set_id == fsv.feature_set_id, fv.feature_set_version == fsv.feature_set_version)). \
        join(fset, fsv.feature_set_id == fset.feature_set_id). \
        join(mv, and_(fv.feature_set_id == mv.c.feature_set_id, fv.feature_set_version == mv.c.feature_set_version)). \
        filter(f.name == name)

    return df.all()

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

    fd = fset.__dict__
    fd.update(fset_metadata.__dict__)

    return schemas.FeatureSet(**fd)

def _register_feature_set_keys(db: Session, fset: Union[schemas.FeatureSetBase, schemas.FeatureSetUpdate], version: int) -> None:
    for pk in __get_pk_columns(fset):
        pk_metadata = models.FeatureSetKey(feature_set_id=fset.feature_set_id,
                                           feature_set_version=version,
                                           key_column_name=pk.upper(),
                                           key_column_data_type=datatype_to_sql(fset.primary_keys[pk]))
        db.add(pk_metadata)

def create_feature_set_version(db: Session, fset: schemas.FeatureSet, version: int = 1, use_last_update = False) -> schemas.FeatureSetVersion:
    fset_version = models.FeatureSetVersion(feature_set_id=fset.feature_set_id, feature_set_version=version,
                                            create_ts=fset.last_update_ts if use_last_update else datetime.now(),
                                            create_username=fset.last_update_username)
    db.add(fset_version)
    db.flush()
    _register_feature_set_keys(db, fset, version)
    
    return fset_version

def update_feature_set_keys(db: Session, fset: schemas.FeatureSetUpdate, version: int):
    db.query(models.FeatureSetKey). \
        filter(and_(
            models.FeatureSetKey.feature_set_id == fset.feature_set_id,
            models.FeatureSetKey.feature_set_version == version)). \
        delete(synchronize_session='fetch')

    _register_feature_set_keys(db, fset, version)

def register_feature_metadata(db: Session, f: schemas.FeatureCreate) -> schemas.Feature:
    """
    Registers the feature's existence in the feature store
    :param db: SqlAlchemy Session
    :param f: (Feature) the feature to register
    :return: The feature metadata
    """
    feature = models.Feature(
        name=f.name, description=f.description,
        feature_data_type=datatype_to_sql(f.feature_data_type),
        feature_type=f.feature_type, tags=','.join(f.tags) if f.tags else None,
        attributes=json.dumps(f.attributes) if f.attributes else None
    )
    db.add(feature)
    db.flush()
    return model_to_schema_feature(feature)

def register_feature_version(db: Session, f: schemas.Feature, feature_set_id: int, version: int = 1) -> None:
    """
    Registers the feature's existence in the feature store
    :param db: SqlAlchemy Session
    :param f: (Feature) the feature to register
    :return: The feature metadata
    """
    db.add(
        models.FeatureVersion(
            feature_id = f.feature_id,
            feature_set_id = feature_set_id,
            feature_set_version = version
    ))

def bulk_register_feature_metadata(db: Session, feats: List[schemas.FeatureCreate]) -> None:
    """
    Registers many features' existences in the feature store
    :param db: SqlAlchemy Session
    :param feats: (List[Feature]) the features to register
    :return: None
    """

    features: List[models.Feature] = [
        models.Feature(
            name=f.name,
            description=f.description,
            feature_data_type=datatype_to_sql(f.feature_data_type),
            feature_type=f.feature_type,
            tags=','.join(f.tags) if f.tags else None,
            attributes=json.dumps(f.attributes) if f.attributes else None
        )
        for f in feats
    ]
    # db.bulk_save_objects(features)
    [db.add(f) for f in features]
    db.flush()
    return features

def bulk_register_feature_versions(db: Session, feats: List[models.Feature], feature_set_id: int, version: int = 1) -> None:
    """
    Registers many features' with a specific version of a feature set
    :param db: SqlAlchemy Session
    :param feats: (List[Feature]) the features to register
    :param version: The feature set version to which the features are being added
    :return: None
    """
    feats = db.query(models.Feature).filter(models.Feature.name.in_([f.name for f in feats]))
    [
        db.add(
            models.FeatureVersion(
                feature_id = f.feature_id,
                feature_set_id = feature_set_id,
                feature_set_version = version
        ))
        for f in feats
    ]

def process_features(db: Session, features: List[Union[schemas.Feature, str]]) -> List[schemas.Feature]:
    """
    Process a list of Features parameter. If the list is strings, it converts them to Features, else returns itself

    :param db: SqlAlchemy Session
    :param features: The list of Feature names or Feature objects
    :return: List[Feature]
    """
    try:
        feat_str = [f if isinstance(f, str) else f.name for f in features]
    except:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message="It seems you've passed in Features that are neither" \
                                             " a feature name (string) nor a Feature object")
    all_features = get_latest_features(db, names=feat_str)
    if len(all_features) != len(features):
        old_names = set([(f if isinstance(f, str) else f.name).upper() for f in features])
        new_names = set([f.name.upper() for f in all_features])
        missing = ', '.join(old_names - new_names)
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f'Could not find the following features in the latest Feature Sets: {missing}')
    return all_features

def create_historian_triggers(db: Session, fset: schemas.FeatureSet) -> None:
    # TODO: Add third trigger to ensure online table has newest value
    table_name = __get_table_name(fset)
    new_pk_cols = ','.join(f'NEWW.{p}' for p in __get_pk_columns(fset))
    new_feature_cols = ','.join(f'NEWW.{f.name}' for f in get_features(db, fset))

    feature_list = get_feature_column_str(db, fset)
    insert_trigger_sql = SQL.feature_set_trigger.format(
        schema=fset.schema_name, table=table_name, action='INSERT',
        pk_list=get_pk_column_str(fset), feature_list=feature_list,
        new_pk_cols=new_pk_cols, new_feature_cols=new_feature_cols)
    update_trigger_sql = SQL.feature_set_trigger.format(
        schema=fset.schema_name, table=table_name, action='UPDATE',
        pk_list=get_pk_column_str(fset), feature_list=feature_list,
        new_pk_cols=new_pk_cols, new_feature_cols=new_feature_cols)

    insert_trigger = DDL(insert_trigger_sql)
    db.execute(insert_trigger)
    update_trigger = DDL(update_trigger_sql)
    db.execute(update_trigger)

def deploy_feature_set(db: Session, fset: schemas.FeatureSetDetail) -> schemas.FeatureSetDetail:
    """
    Deploys the current feature set. Equivalent to calling fs.deploy(schema_name, table_name)
    :param db: SqlAlchemy Session
    :param fset: The feature set
    :return: List[Feature]
    """
    metadata = MetaData(db.get_bind())
    table_name = __get_table_name(fset)

    logger.info('Creating Feature Set...')
    pk_columns = _sql_to_sqlalchemy_columns(fset.primary_keys, True)
    ts_columns = [Column('last_update_ts', TIMESTAMP, nullable=False)]
    feature_columns = _sql_to_sqlalchemy_columns({f.name.lower(): f.feature_data_type
                                                  for f in get_features(db, fset)}, False)
    columns = pk_columns + ts_columns + feature_columns
    feature_set = Table(table_name, metadata, *columns, schema=fset.schema_name.lower())
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

    history = Table(f'{table_name}_history', metadata, *columns, schema=fset.schema_name.lower())
    history.create(db.connection())
    logger.info('Done.')

    logger.info('Updating Metadata...')
    # Due to an issue with our sqlalchemy driver, we cannot update in the standard way with timestamps. TODO
    # So we create the update, compile it, and execute it directly
    deploy_ts = datetime.now()
    db.query(models.FeatureSetVersion). \
        filter((models.FeatureSetVersion.feature_set_id==fset.feature_set_id) &
            (models.FeatureSetVersion.feature_set_version == fset.feature_set_version)).\
        update({models.FeatureSetVersion.deployed: True, 
            models.FeatureSetVersion.deploy_ts: datetime.now()})
    fset.deploy_ts = deploy_ts
    fset.deployed = True
    logger.info('Done.')
    return fset

def migrate_feature_set(db: Session, previous: schemas.FeatureSetDetail, live: schemas.FeatureSetDetail):
    metadata = MetaData(db.connection())

    old_name = __get_table_name(previous)
    new_name = __get_table_name(live)
    old_serving = Table(old_name, metadata, PrimaryKeyConstraint(*[pk.lower() for pk in previous.primary_keys]),
                    schema=previous.schema_name.lower(), autoload=True, keep_existing=True)
    old_history = Table(f'{old_name}_history', metadata, PrimaryKeyConstraint(*[pk.lower() for pk in previous.primary_keys]),
                    schema=previous.schema_name.lower(), autoload=True, keep_existing=True)
    
    live_serving = Table(new_name, metadata, PrimaryKeyConstraint(*[pk.lower() for pk in live.primary_keys]),
                    schema=live.schema_name.lower(), autoload=True, keep_existing=True)
    live_history = Table(f'{new_name}_history', metadata, PrimaryKeyConstraint(*[pk.lower() for pk in live.primary_keys]),
                    schema=live.schema_name.lower(), autoload=True, keep_existing=True)
    
    _migrate_data(db, old_serving, live_serving)
    _migrate_data(db, old_history, live_history)

def _migrate_data(db: Session, old: Table, new: Table):
    new_cols = [c.name for c in new.c]
    old_cols = [c for c in old.c if c.name in new_cols]

    ins = new.insert().from_select([c.name for c in old_cols], select(old_cols))
    db.execute(ins)

def validate_training_view(db: Session, tv: schemas.TrainingViewCreate) -> None:
    """
    Validates that the training view doesn't already exist, that the provided sql is valid, and that the pk_cols and
    label_col provided are valid

    :param db: SqlAlchemy Session
    :param tv: TrainingView
    :return: None
    """
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


def create_training_view(db: Session, tv: schemas.TrainingViewCreate) -> schemas.TrainingViewDetail:
    """
    Registers a training view for use in generating training SQL

    :param db: SqlAlchemy Session
    :param tv: The training view to register
    :return: None
    """
    logger.info('Building training sql...')
    train = models.TrainingView(name=tv.name, description=tv.description or 'None Provided')
    db.add(train)
    db.flush()
    logger.info('Done.')

    # Get generated view ID
    return schemas.TrainingViewDetail(**tv.__dict__, view_id=train.view_id)

def create_training_view_version(db: Session, tv: schemas.TrainingViewDetail, version: int = 1) -> None:
    tvv = models.TrainingViewVersion(view_id=tv.view_id, view_version=version, sql_text=tv.sql_text,
                                            label_column=tv.label_column, ts_column=tv.ts_column)
    db.add(tvv)
    db.flush()
    tv.view_version = version

    _register_training_view_keys(db, tv)

    return tv

def _register_training_view_keys(db: Session, tv: schemas.TrainingViewDetail) -> None:
    logger.info('Creating Join Keys')
    for i in tv.join_columns:
        logger.info(f'\tCreating Join Key {i}...')
        key = models.TrainingViewKey(view_id=tv.view_id, view_version=tv.view_version, key_column_name=i.upper(), key_type='J')
        db.add(key)
    logger.info('Done.')
    logger.info('Creating Training View Primary Keys')
    for i in tv.pk_columns:
        logger.info(f'\tCreating Primary Key {i}...')
        key = models.TrainingViewKey(view_id=tv.view_id, view_version=tv.view_version, key_column_name=i.upper(), key_type='P')
        db.add(key)
    logger.info('Done.')

def alter_training_view_version(db: Session, tv: schemas.TrainingViewDetail):
    db.query(models.TrainingView). \
        filter((models.TrainingView.view_id == tv.view_id) &
            (models.TrainingViewVersion.view_version == tv.view_version)). \
        update({
            'sql_text': tv.sql_text,
            'label_column': tv.label_column,
            'ts_column': tv.ts_column,
            'last_update_ts': datetime.now()
        })

def alter_training_view_keys(db: Session, tv: schemas.TrainingViewDetail):
    db.query(models.TrainingViewKey). \
        filter((models.TrainingViewKey.view_id == tv.view_id) &
            (models.TrainingViewVersionKey.view_version == tv.view_version)). \
        delete(synchronize_session='fetch')

    _register_training_view_keys(db, tv)

def update_training_view_description(db: Session, view_id: int, description: str) -> None:
    db.query(models.TrainingView).filter(models.TrainingView.view_id==view_id).update({'description':description})

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
        join(tsi, (d.training_set_version == tsi.training_set_version) & (tsi.training_set_id == d.training_set_id)). \
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

def remove_feature(db: Session, feature: models.Feature, version: int) -> None:
    db.query(models.FeatureVersion). \
        filter(and_(
            models.FeatureVersion.feature_id == feature.feature_id,
            models.FeatureVersion.feature_set_version == version)). \
        delete(synchronize_session='fetch')

    count = db.query(models.FeatureVersion).filter(models.FeatureVersion.feature_id == feature.feature_id).count()

    if count == 0:
        db.delete(feature)

def delete_feature(db: Session, feature: models.Feature) -> None:
    db.delete(feature)


def get_deployments(db: Session, _filter: Dict[str, str] = None, feature: schemas.FeatureDetail = None,
                    feature_set: schemas.FeatureSetDetail = None) -> List[schemas.DeploymentDetail]:
    d = aliased(models.Deployment, name='d')
    ts = aliased(models.TrainingSet, name='ts')
    tsi = aliased(models.TrainingSetInstance, name='tsi')
    f = aliased(models.Feature, name='f')
    fv = aliased(models.FeatureVersion, name='fv')
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
        p = db.query(f.feature_id). \
            join(fv, (fv.feature_id == f.feature_id)). \
            filter((fv.feature_set_id == feature_set.feature_set_id) & 
                (fv.feature_set_version == feature_set.feature_set_version)). \
            subquery('p')
        q = q.join(tsf, tsf.training_set_id == ts.training_set_id). \
            filter(tsf.feature_id.in_(p))

    deployments = []
    for name, deployment, tset_start, tset_end, tset_create in q.all():
        deployments.append(
            schemas.DeploymentDetail(
                **deployment.__dict__,
                training_set_name=name,
                training_set_start_ts=tset_start,
                training_set_end_ts=tset_end,
                training_set_create_ts=tset_create
            )
        )
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
            join(models.FeatureVersion, models.Feature.feature_id == models.FeatureVersion.feature_id). \
            filter((models.FeatureVersion.feature_set_id == fset.feature_set_id) &
                (models.FeatureVersion.feature_set_version == fset.feature_set_version)
            ).all()
        features = [model_to_schema_feature(f) for f in features_rows]
    return features


def get_feature_column_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f.name for f in get_features(db, fset)])


def get_current_time(db: Session) -> datetime:
    return db.execute('VALUES(CURRENT_TIMESTAMP)').first()[0]
