
from sqlalchemy.orm import Session, aliased, load_only
from typing import List, Dict, Union, Optional, Any, Tuple, Set
from . import schemas
from .constants import SQL, SQLALCHEMY_TYPES
from shared.models import feature_store_models as models
from shared.services.database import SQLAlchemyClient, DatabaseFunctions
from shared.logger.logging_config import logger
from fastapi import status
import re
import json
from datetime import datetime
from sqlalchemy import update, Integer, String, func, distinct, cast, and_, Column, event, DateTime, literal_column, text
from .utils.utils import (__get_pk_columns, get_pk_column_str, datatype_to_sql,
                          sql_to_datatype, _sql_to_sqlalchemy_columns, model_to_schema_feature,
                          __validate_feature_data_type, __validate_primary_keys)
from mlflow.store.tracking.dbmodels.models import SqlRun, SqlTag, SqlParam
from sqlalchemy.schema import MetaData, Table, PrimaryKeyConstraint, DDL
from sqlalchemy.types import (CHAR, VARCHAR, DATE, TIME, TIMESTAMP, BLOB, CLOB, TEXT, BIGINT,
                                DECIMAL, FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, BOOLEAN)
from shared.api.exceptions import SpliceMachineException, ExceptionCodes


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
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=str)
    # Validate metadata
    if len(get_feature_sets(db, _filter={'table_name': fset.table_name, 'schema_name': fset.schema_name})) > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=str)
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

def validate_feature(db: Session, name: str) -> None:
    """
    Ensures that the feature doesn't exist as all features have unique names
    :param db: SqlAlchemy Session
    :param name: the Feature name
    :return: None
    """
    # TODO: Capitalization of feature name column
    # TODO: Make more informative, add which feature set contains existing feature
    str = f"Cannot add feature {name}, feature already exists in Feature Store. Try a new feature name."
    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', name, re.IGNORECASE):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
                                     message='Feature name does not conform. Must start with an alphabetic character, '
                                     'and can only contains letters, numbers and underscores')

    l = db.query(models.Feature.name).filter(func.upper(models.Feature.name) == name.upper()).count()
    if l > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=str)

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

def get_feature_vector(db: Session, feats: List[schemas.Feature], join_keys: Dict[str, Union[str, int]], feature_sets: List[schemas.FeatureSet], 
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

    tables = [Table(fset.table_name.lower(), metadata, PrimaryKeyConstraint(*[pk.lower() for pk in fset.primary_keys]), schema=fset.schema_name.lower(), autoload=True).\
        alias(f'fset{fset.feature_set_id}') for fset in feature_sets]

    columns = []
    if return_pks:
        seen = set()
        pks = [seen.add(pk_col.name) or getattr(table.c, pk_col.name) for table in tables for pk_col in table.primary_key if pk_col.name not in seen]
        columns.extend(pks)

    columns.extend([getattr(table.c, f.name.lower()) for f in feats for table in tables if f.name.lower() in table.c])

    # For each Feature Set, for each primary key in the given feature set, get primary key value from the user provided dictionary
    filters = [getattr(table.c, pk_col.name)==join_keys[pk_col.name.lower()] 
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
        func.count().over(partition_by=models.FeatureSetKey.feature_set_id).\
            label('KeyCount')).\
        subquery('fsk')

    tc = aliased(models.TrainingView, name='tc')
    c = aliased(models.TrainingViewKey, name='c')
    f = aliased(models.Feature, name='f')

    match_keys = db.query(
        f.feature_id,
        fsk.c.KeyCount,
        func.count(distinct(fsk.c.key_column_name)).\
            label('JoinKeyMatchCount')).\
        select_from(tc).\
        join(c, (c.view_id==tc.view_id) & (c.key_type=='J')).\
        join(fsk, c.key_column_name==fsk.c.key_column_name).\
        join(f, f.feature_set_id==fsk.c.feature_set_id).\
        filter(tc.name==training_view).\
        group_by(
            f.feature_id,
            fsk.c.KeyCount).\
        subquery('match_keys')

    fl = db.query(match_keys.c.feature_id).\
        filter(match_keys.c.JoinKeyMatchCount==match_keys.c.KeyCount).\
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
    return db.query(models.FeatureSet.deployed).\
        filter(models.FeatureSet.feature_set_id==fset_id).\
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
    db.query(models.FeatureSetKey).filter(models.FeatureSetKey.feature_set_id == feature_set_id).\
        delete(synchronize_session='fetch')

def delete_feature_set(db: Session, feature_set_id: int):
    """
    Deletes a feature set with a given ID

    :param db: Database Session
    :param feature_set_id: feature set ID to delete
    """
    db.query(models.FeatureSet).filter(models.FeatureSet.feature_set_id == feature_set_id).delete(synchronize_session='fetch')

def full_delete_feature_set(db: Session, feature_set: schemas.FeatureSet, cascade: bool = False,
                       training_sets: Set[int] = None):
    """
    Deletes a Feature Set. Drops the table. Removes keys. Potentially removes training sets if there are dependencies

    :param db: Database Session
    :param feature_set: feature set to delete
    :param training_sets: Set[int] training sets 
    :param cascade: whether to delete dependent training sets. If this is True training_sets must be set.
    """
    logger.info("Dropping table")
    DatabaseFunctions.drop_table_if_exists(feature_set.schema_name, feature_set.table_name, db.get_bind())
    logger.info("Dropping history table")
    DatabaseFunctions.drop_table_if_exists(feature_set.schema_name, f'{feature_set.table_name}_history', db.get_bind())
    if cascade and training_sets:
        logger.info(f'linked training sets: {training_sets}')
        # Delete training set features if any
        logger.info("Removing training set features")
        delete_training_set_features(db, training_sets)

        # Delete training sets
        logger.info("Removing training sets")
        delete_training_sets(db, training_sets)

    # Delete features
    logger.info("Removing features")
    delete_features_from_feature_set(db, feature_set.feature_set_id)
    # Delete Feature Set Keys
    logger.info("Removing feature set keys")
    delete_features_set_keys(db, feature_set.feature_set_id)

    # Delete feature set
    logger.info("Removing features set")
    delete_feature_set(db, feature_set.feature_set_id)


def delete_training_set_features(db: Session, training_sets: Set[int]):
    """
    Deletes training set features from training sets with the given IDs
    
    :param db: Database Session
    :param training_sets: training set IDs
    """
    db.query(models.TrainingSetFeature).filter(models.TrainingSetFeature.training_set_id.in_(training_sets)).\
            delete(synchronize_session='fetch')

def delete_training_sets(db: Session, training_sets: Set[int]):
    """
    Deletes training sets with the given IDs
    
    :param db: Database Session
    :param training_sets: training set IDs to delete
    """
    db.query(models.TrainingSet).filter(models.TrainingSet.training_set_id.in_(training_sets)).\
            delete(synchronize_session='fetch')


def delete_training_view_keys(db: Session, view_id: int):
    """
    Deletes training view keys for a particular training view

    :param db: Database Session
    :param view_id: training view ID
    """
    # Delete features
    logger.info("Removing Training View Keys")
    db.query(models.TrainingViewKey).filter(models.TrainingViewKey.view_id == view_id).\
        delete(synchronize_session='fetch')

def delete_training_view(db: Session, view_id: int):
    """
    Deletes a training view

    :param db: Database Session
    :param view_id: training view ID
    """
    db.query(models.TrainingView).filter(models.TrainingView.view_id == view_id).\
        delete(synchronize_session='fetch')


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
    d = aliased(models.Deployment, name='d')

    p = db.query(f.feature_id).filter(f.feature_set_id==feature_set_id).subquery('p')
    p1 = db.query(tset_feat.training_set_id).filter(tset_feat.feature_id.in_(p)).subquery('p1')
    r = db.query(tset.training_set_id, d.model_schema_name, d.model_table_name).\
        select_from(tset).\
        join(d,d.training_set_id==tset.training_set_id, isouter=True).\
        filter(tset.training_set_id.in_(p1)).all()
    deps = dict(
        model = set([f'{schema}.{table}' for _, schema, table in r if schema and table]),
        training_set = set([tid for tid, _, _ in r])
    )
    return deps

def get_training_view_dependencies(db: Session, vid: int) -> List[Dict[str,str]]:
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
    return [i[0] for i in res] # Returns 



def get_feature_sets(db: Session, feature_set_ids: List[int] = None, feature_set_names: List[str] = None, _filter: Dict[str, str] = None) -> List[schemas.FeatureSet]:
    """
    Returns a list of available feature sets

    :param db: SqlAlchemy Session
    :param feature_set_ids: A list of feature set IDs. If none will return all FeatureSets
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
        queries.extend([and_(func.upper(fset.schema_name)==name.split('.')[0].upper(), 
            func.upper(fset.table_name)==name.split('.')[1].upper()) for name in feature_set_names])
    if _filter:
        queries.extend([getattr(fset, name) == value for name, value in _filter.items()])

    p = db.query(
        models.FeatureSetKey.feature_set_id,
        func.string_agg(models.FeatureSetKey.key_column_name, literal_column("'|'"), type_=String).\
            label('pk_columns'),
        func.string_agg(models.FeatureSetKey.key_column_data_type, literal_column("'|'"), type_=String).\
            label('pk_types')
        ).\
        group_by(models.FeatureSetKey.feature_set_id).\
        subquery('p')

    q = db.query(
        fset, 
        p.c.pk_columns, 
        p.c.pk_types).\
        join(p, fset.feature_set_id==p.c.feature_set_id).\
        filter(and_(*queries))

    for fs, pk_columns, pk_types in q.all():
        pkcols = pk_columns.split('|')
        pktypes = pk_types.split('|')
        primary_keys = {c: sql_to_datatype(k) for c, k in zip(pkcols, pktypes)}
        feature_sets.append(schemas.FeatureSet(**fs.__dict__, primary_keys=primary_keys))
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
        func.string_agg(models.TrainingViewKey.key_column_name, literal_column("','"), type_=String).\
            label('pk_columns')
        ).\
        filter(models.TrainingViewKey.key_type=='P').\
        group_by(models.TrainingViewKey.view_id).\
        subquery('p')

    c = db.query(
        models.TrainingViewKey.view_id,
        func.string_agg(models.TrainingViewKey.key_column_name, literal_column("','"), type_=String).\
            label('join_columns')
        ).\
        filter(models.TrainingViewKey.key_type=='J').\
        group_by(models.TrainingViewKey.view_id).\
        subquery('c')

    tc = aliased(models.TrainingView, name='tc')

    q = db.query(
        tc.view_id,
        tc.name,
        tc.description, 
        cast(tc.sql_text, String(1000)).label('view_sql'),
        p.c.pk_columns,
        tc.ts_column,
        tc.label_column,
        c.c.join_columns).\
        join(p, tc.view_id==p.c.view_id).\
        join(c, tc.view_id==c.c.view_id)

    if _filter:
        q = q.filter(and_(*[getattr(tc, name) == value for name, value in _filter.items()]))

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
    return db.query(models.TrainingView.view_id).\
        filter(models.TrainingView.name==name).\
        first()[0]

def get_feature_descriptions_by_name(db: Session, names: List[str], sort: bool = True) -> List[schemas.FeatureDescription]:
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

    df = db.query(fset.schema_name, fset.table_name, f).\
        select_from(f).\
        join(fset, f.feature_set_id==fset.feature_set_id)

    # If they don't pass in feature names, get all features 
    if names:
        df = df.filter(func.upper(f.name).in_([name.upper() for name in names]))

    features = []
    for schema, table, feat in df.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = model_to_schema_feature(feat)
        features.append(schemas.FeatureDescription(**f.__dict__, feature_set_name=f'{schema}.{table}'))

    if sort:
        indices = {v.upper():i for i,v in enumerate(names)}
        features = sorted(features, key=lambda f: indices[f.name.upper()])
    return features

def _get_feature_set_counts(db) -> List[Tuple[bool,int]]:
    """
    Returns the counts of undeployed and deployed feature set as a list of tuples
    """

    fset = aliased(models.FeatureSet, name='fset')
    return db.query(fset.deployed, func.count(fset.feature_set_id)).group_by(fset.deployed).all()

def _get_feature_counts(db) -> List[Tuple[bool,int]]:
    """
    Returns the counts of undeployed and deployed features as a list of tuples
    """

    fset = aliased(models.FeatureSet, name='fset')
    f = aliased(models.Feature, name='f')
    return db.query(fset.deployed, func.count(fset.feature_set_id)).\
        join(f, f.feature_set_id==fset.feature_set_id).\
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
    return db.query(pend).\
        filter(pend.status=='PENDING').\
        count()

def _get_num_created_models(db) -> int:
    """
    Returns the number of models that have been created and trained with a feature store training set.
    This corresponds to the number of mlflow runs with the tag splice.model_name set and the parameter
    splice.feature_store.training_set set.
    """
    return db.query(SqlRun).\
        join(SqlTag, SqlRun.run_uuid==SqlTag.run_uuid).\
        join(SqlParam, SqlRun.run_uuid==SqlParam.run_uuid).\
        filter(SqlParam.key=='splice.feature_store.training_set').\
        filter(SqlTag.key=='splice.model_name').\
        count()

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
    num_deployed_fsets = sum(i[1] for i in feature_set_counts if i[0]) # Only sum the ones that have Deployed=True

    feature_counts = _get_feature_counts(db)
    num_feats = sum(i[1] for i in feature_counts)
    num_deployed_feats = sum(i[1] for i in feature_counts if i[0]) # Only sum the ones that have Deployed=True

    num_training_sets = _get_num_training_sets(db)
    num_training_views = _get_num_training_views(db)
    num_created_models = _get_num_created_models(db)
    num_deployemnts = _get_num_deployments(db)
    num_pending_feature_set_deployments = _get_num_pending_feature_sets(db)

    return schemas.FeatureStoreSummary(
        num_feature_sets=num_fsets,
        num_deployed_feature_sets=num_deployed_fsets,
        num_features=num_feats,
        num_deployed_features=num_deployed_feats,
        num_training_sets=num_training_sets,
        num_training_views=num_training_views,
        num_models=num_created_models,
        num_deployed_models=num_deployemnts,
        num_pending_feature_set_deployments=num_pending_feature_set_deployments
    )

def update_feature_metadata(db: Session, name: str, desc: str = None,
                            tags: List[str] = None, attributes: Dict[str,str] = None) -> schemas.Feature:
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

def get_features_by_name(db: Session, names: List[str]) -> List[schemas.FeatureDescription]:
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

    df = db.query(f, fset.schema_name, fset.table_name, fset.deployed).\
        select_from(f).\
        join(fset, f.feature_set_id==fset.feature_set_id)


    # If they don't pass in feature names, get all features 
    if names:
        df = df.filter(func.upper(f.name).in_([name.upper() for name in names]))
    
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
    fset_metadata = models.FeatureSet(schema_name=fset.schema_name, table_name=fset.table_name, description=fset.description)
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
            feature_set_id=f.feature_set_id, name=f.name, description=f.description,
            feature_data_type=datatype_to_sql(f.feature_data_type),
            feature_type=f.feature_type, tags=','.join(f.tags) if f.tags else None,
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
    if not all(
        [isinstance(i, schemas.Feature) for i in all_features]):
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
    ts_columns = [Column('asof_ts', TIMESTAMP, primary_key=True), Column('ingest_ts', TIMESTAMP)]
    feature_columns = _sql_to_sqlalchemy_columns({f.name.lower(): f.feature_data_type
                                                  for f in get_features(db, fset)}, False)
    columns = pk_columns + ts_columns + feature_columns
    history = Table(f'{fset.table_name.lower()}_history', metadata, *columns, schema=fset.schema_name.lower())
    history.create(db.connection())
    logger.info('Done.')

    logger.info('Creating Historian Triggers...')
    #TODO: Add third trigger to ensure online table has newest value
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
    updt = update(models.FeatureSet).where(models.FeatureSet.feature_set_id==fset.feature_set_id).\
        values(deployed=True, deploy_ts=text('CURRENT_TIMESTAMP'))
    stmt = updt.compile(dialect=db.get_bind().dialect, compile_kwargs={"literal_binds": True})
    db.execute(str(stmt))
    fset.deployed = True
    logger.info('Done.')
    return fset

def validate_training_view(db: Session, name, sql_text, join_keys, pk_cols, label_col=None) -> None:
    """
    Validates that the training view doesn't already exist.

    :param db: SqlAlchemy Session
    :param name: The training view name
    :param sql: The training view provided SQL
    :param join_keys: The provided join keys when creating the training view
    :param pk_cols: The primary keys of the training view
    :param label_col: The label column
    :return: None
    """
    # Validate name doesn't exist
    if len(get_training_views(db, _filter={'name': name}))> 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=f"Training View {name} already exists!")

    # Column comparison
    # Lazily evaluate sql resultset, ensure that the result contains all columns matching pks, join_keys, tscol and label_col
    from sqlalchemy.exc import ProgrammingError
    try:
        valid_df = db.execute(sql_text)
    except ProgrammingError as e:
        if '[Splice Machine][Splice]' in str(e):
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                            message=f'The provided SQL is incorrect. The following error was raised during '
                                            f'validation:\n\n{str(e)}') from None
        raise e

    # Ensure the label column specified is in the output of the SQL
    if label_col and not label_col.upper() in valid_df.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                        message=f"Provided label column {label_col} is not available in the provided SQL")

    # Ensure the primary key columns are in the output of the SQL
    pks = set(key.upper() for key in pk_cols)
    missing_keys = pks - set(valid_df.keys())
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                        message=f"Provided primary key(s) {missing_keys} are not available in the provided SQL")

    # Confirm that all join_keys provided correspond to primary keys of created feature sets
    jks = set(i[0].upper() for i in db.query(distinct(models.FeatureSetKey.key_column_name)).all())
    missing_keys = set(i.upper() for i in join_keys) - jks
    if missing_keys:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                message=f"Not all provided join keys exist. Remove {missing_keys} or " \
                                f"create a feature set that uses the missing keys")

def create_training_view(db: Session, tv: schemas.TrainingViewCreate) -> None:
    """
    Registers a training view for use in generating training SQL

    :param db: SqlAlchemy Session
    :param tv: The training view to register
    :return: None
    """
    logger.info('Building training sql...')
    train = models.TrainingView(name=tv.name, description=tv.description or 'None Provided', sql_text=tv.sql_text, ts_column=tv.ts_column,
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

def retrieve_training_set_metadata_from_deployment(db: Session, schema_name: str, table_name: str) -> schemas.TrainingSetMetadata:
    """
    Reads Feature Store metadata to retrieve definition of training set used to train the specified model.
    :param schema_name: model schema name
    :param table_name: model table name
    :return:
    """
    d = aliased(models.Deployment, name='d')
    ts = aliased(models.TrainingSet, name='ts')
    tsf = aliased(models.TrainingSetFeature, name='tsf')
    tv = aliased(models.TrainingView, name='tv')
    f = aliased(models.Feature, name='f')

    deploy = db.query(
        tv.name,
        d.training_set_start_ts,
        d.training_set_end_ts,
        d.training_set_create_ts,
        func.string_agg(f.name, literal_column("','"), type_=String).\
            label('features'),
        tv.label_column.label('label')
    ).\
    select_from(d).\
    join(ts, d.training_set_id==ts.training_set_id).\
    join(tsf, tsf.training_set_id==d.training_set_id).\
    outerjoin(tv, tv.view_id==ts.view_id).\
    join(f, tsf.feature_id==f.feature_id).\
    filter(and_(
        d.model_schema_name==schema_name, 
        d.model_table_name==table_name
    )).\
    group_by(tv.name,
        d.training_set_start_ts,
        d.training_set_end_ts,
        d.training_set_create_ts,
        tv.label_column
    ).first()

    if not deploy:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST, 
                                        message=f"No deployment found for {schema_name}.{table_name}")
    return schemas.TrainingSetMetadata(**deploy._asdict())

def delete_feature(db: Session, feature: models.Feature) -> None:
    db.delete(feature)

def get_deployments(db: Session, _filter: Dict[str, str] = None, feature: schemas.FeatureDescription = None,
                    feature_set: schemas.FeatureSet = None) -> List[schemas.DeploymentDescription]:
    d = aliased(models.Deployment, name='d')
    ts = aliased(models.TrainingSet, name='ts')
    f = aliased(models.Feature, name='f')
    tsf = aliased(models.TrainingSetFeature, name='tsf')
    
    q = db.query(ts.name, d).\
        join(ts, ts.training_set_id==d.training_set_id)
    
    if _filter:
        # if filter key is a column in Training_Set, get compare Training_Set column, else compare to Deployment column
        q = q.filter(and_(*[(getattr(ts, name) if hasattr(ts, name) else getattr(d, name)) == value 
                                for name, value in _filter.items()]))
    elif feature:
        q = q.join(tsf, tsf.training_set_id==ts.training_set_id).\
            filter(tsf.feature_id==feature.feature_id)
    elif feature_set:
        p = db.query(f.feature_id).filter(f.feature_set_id==feature_set.feature_set_id)
        q = q.join(tsf, tsf.training_set_id == ts.training_set_id).\
            filter(tsf.feature_id.in_(p))

    deployments = []
    for name, deployment in q.all():
        deployments.append(schemas.DeploymentDescription(**deployment.__dict__, training_set_name=name))
    return deployments

def get_features_from_deployment(db: Session, tsid: int) -> List[schemas.Feature]:
    ids = db.query(models.TrainingSetFeature.feature_id).\
        filter(models.TrainingSetFeature.training_set_id==tsid).\
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
        features_rows = db.query(models.Feature).\
                        filter(models.Feature.feature_set_id==fset.feature_set_id).all()
        features = [model_to_schema_feature(f) for f in features_rows]
    return features

def get_feature_column_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f.name for f in get_features(db, fset)])

def get_current_time(db: Session) -> datetime:
    return db.execute('VALUES(CURRENT_TIMESTAMP)').first()[0]
