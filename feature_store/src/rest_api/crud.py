
from sqlalchemy.orm import Session, aliased, load_only
from typing import List, Dict, Union, Optional, Any
from . import schemas
from .constants import SQL, SQL_TYPES
from shared.models import feature_store_models as models
from shared.services.database import SQLAlchemyClient
from shared.logger.logging_config import logger
from fastapi import status
import re
import json
from datetime import datetime
from sqlalchemy import inspect as peer_into_splice_db
from sqlalchemy import sql, Integer, String, func, distinct, cast, and_, Column, event
from .utils import __get_pk_columns, get_pk_column_str, get_pk_schema_str
from sys import exc_info as get_stack_trace
from sqlalchemy.schema import MetaData, Table, PrimaryKeyConstraint, DDL
from sqlalchemy.types import (CHAR, VARCHAR, DATE, TIME, TIMESTAMP, BLOB, CLOB, TEXT, BIGINT,
                                DECIMAL, FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, BOOLEAN)
from shared.api.exceptions import SpliceMachineException, ExceptionCodes

SQLALCHEMY_TYPES = dict(zip(SQL_TYPES, [CHAR, VARCHAR, VARCHAR, DATE, TIME, TIMESTAMP, BLOB, CLOB, TEXT, BIGINT,
                        DECIMAL, FLOAT, FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, SMALLINT, BOOLEAN, INTEGER]))

def get_db():
    """
    Provides SqlAlchemy Session object to path operations
    """
    db = SQLAlchemyClient.SessionFactory()
    try:
        yield db
    except Exception as e:
        logger.error(e)
        logger.warning("Rolling back...")
        db.rollback()
    else:
        logger.info("Committing...")
        db.commit()
        logger.info("Committed")
    finally:
        logger.info("Closing session")
        db.close()
        SQLAlchemyClient.SessionFactory.remove()

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
    if table_exists(db, fset.schema_name, fset.table_name):
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=str)
    # Validate metadata
    if len(get_feature_sets(db, _filter={'table_name': fset.table_name, 'schema_name': fset.schema_name})) > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=str)

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
    l = len(db.query(models.Feature.name).filter(models.Feature.name == name.upper()).all())
    if l > 0:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_EXISTS, message=str)

    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', name, re.IGNORECASE):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
                                     message='Feature name does not conform. Must start with an alphabetic character, '
                                     'and can only contains letters, numbers and underscores')

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

def get_feature_vector(db: Session, feats: List[schemas.Feature], join_keys: Dict[str, Union[str, int]], feature_sets: List[schemas.FeatureSet], return_sql: bool) -> Union[Dict[str, Any], str]:
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets

    :param db: SqlAlchemy Session
    :param features: List of Features
    :param join_key_values: (dict) join key values to get the proper Feature values formatted as {join_key_column_name: join_key_value}
    :param feature_sets: List of Feature Sets
    :param return_sql: Whether to return the SQL needed to get the vector or the values themselves. Default False
    :return: Dict or str (SQL statement)
    """
    metadata = MetaData(db.get_bind())

    tables = [Table(fset.table_name, metadata, PrimaryKeyConstraint(*[pk.lower() for pk in fset.primary_keys]), schema=fset.schema_name, autoload=True).\
        alias(f'fset{fset.feature_set_id}') for fset in feature_sets]
    columns = [getattr(table.c, f.name.lower()) for table in tables for f in feats if f.name.lower() in table.c]
    
    # For each Feature Set, for each primary key in the given feature set, get primary key value from the user provided dictionary
    filters = [getattr(table.c, pk_col.name)==join_keys[pk_col.name.lower()] 
                for table in tables for pk_col in table.primary_key]

    q = db.query(*columns).filter(and_(*filters))

    if return_sql:
        return str(q.statement.compile(db.get_bind(), compile_kwargs={"literal_binds": True}))
    
    vector = q.first()
    return vector._asdict() if vector else {}

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

    features = []
    for feat in q.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = feat.__dict__
        f['tags'] = json.loads(f['tags'])
        features.append(schemas.Feature(**f))
    return features

def get_feature_sets(db: Session, feature_set_ids: List[int] = None, _filter: Dict[str, str] = None) -> List[schemas.FeatureSet]:
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
    if _filter:
        queries.extend([getattr(fset, name) == value for name, value in _filter.items()])

    # SQLAlchemy does not support STRING_AGG
    p = sql.text(SQL.feature_set_pk_columns).\
                    columns(
                        sql.column('feature_set_id', Integer), 
                        sql.column('pk_columns', String), 
                        sql.column('pk_types', String)).\
                    alias('p')

    q = db.query(
        fset, 
        p.c.pk_columns, 
        p.c.pk_types).\
        join(p, fset.feature_set_id==p.c.feature_set_id).\
        filter(and_(*queries))

    for fs, pk_columns, pk_types in q.all():
        pkcols = pk_columns.split('|')
        pktypes = pk_types.split('|')
        primary_keys = {c: k for c, k in zip(pkcols, pktypes)}
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

    p = sql.text(SQL.training_view_pk_columns).\
            columns(
                sql.column('view_id', Integer),
                sql.column('pk_columns', String)).\
            alias('p')

    c = sql.text(SQL.join_columns).\
            columns(
                sql.column('view_id', Integer),
                sql.column('join_columns', String)).\
            alias('c')

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
        all()[0][0]

def get_features_by_name(db: Session, names: List[str]) -> List[schemas.FeatureDescription]:
    """
    Returns a dataframe or list of features whose names are provided

    :param db: SqlAlchemy Session
    :param names: The list of feature names
    :return: List[Feature] The list of Feature objects and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    # If they don't pass in feature names, raise exception
    if not names:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.MISSING_ARGUMENTS,
                                        message="Please provide at least one name")

    f = aliased(models.Feature, name='f')
    fset = aliased(models.FeatureSet, name='fset')

    df = db.query(fset.schema_name, fset.table_name, f).\
        select_from(f).\
        join(fset, f.feature_set_id==fset.feature_set_id).\
        filter(f.name.in_(names))

    features = []
    for schema, table, feat in df.all():
        # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
        f = feat.__dict__
        f['tags'] = json.loads(str(f['tags']))
        features.append(schemas.FeatureDescription(**f, feature_set_name=f'{schema}.{table}'))
    return features

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
            key_column_data_type=fset.primary_keys[pk])
        db.add(pk_metadata)
    return schemas.FeatureSet(**fset.__dict__, feature_set_id=fsid)

def register_feature_metadata(db: Session, f: schemas.FeatureCreate) -> schemas.Feature:
    """
    Registers the feature's existence in the feature store
    :param db: SqlAlchemy Session
    :param f: (Feature) the feature to register
    :return: None
    """
    feature = models.Feature(
        feature_set_id=f.feature_set_id, name=f.name, description=f.description,
        feature_data_type=f.feature_data_type,
        feature_type=f.feature_type, tags=json.dumps(f.tags)
    )
    db.add(feature)
    db.flush()
    fd = feature.__dict__
    fd['tags'] = json.loads(fd['tags'])
    return schemas.Feature(**fd)

def process_features(db: Session, features: List[Union[schemas.Feature, str]]) -> List[schemas.Feature]:
    """
    Process a list of Features parameter. If the list is strings, it converts them to Features, else returns itself

    :param db: SqlAlchemy Session
    :param features: The list of Feature names or Feature objects
    :return: List[Feature]
    """
    feat_str = [f for f in features if isinstance(f, str)]
    str_to_feat = get_features_by_name(db, names=feat_str) if feat_str else []
    all_features = str_to_feat + [f for f in features if not isinstance(f, str)]
    if not all(
        [isinstance(i, schemas.Feature) for i in all_features]):
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                        message="It seems you've passed in Features that are neither" \
                                        " a feature name (string) or a Feature object")
    return all_features

def deploy_feature_set(db: Session, fset: schemas.FeatureSet) -> schemas.FeatureSet:
    """
    Deploys the current feature set. Equivalent to calling fs.deploy(schema_name, table_name)
    :param db: SqlAlchemy Session
    :param fset: The feature set
    :return: List[Feature]
    """
    old_pk_cols = ','.join(f'OLDW.{p}' for p in __get_pk_columns(fset))
    old_feature_cols = ','.join(f'OLDW.{f.name}' for f in get_features(db, fset))

    metadata = MetaData(db.get_bind())

    pk_columns = [Column(k.lower(), SQLALCHEMY_TYPES[fset.primary_keys[k]], primary_key=True) for k in fset.primary_keys]
    feature_columns = [Column(f.name.lower(), SQLALCHEMY_TYPES[f.feature_data_type]) for f in get_features(db, fset)]

    trigger_sql = SQL.feature_set_trigger.format(
        schema=fset.schema_name, table=fset.table_name, pk_list=get_pk_column_str(fset),
        feature_list=get_feature_column_str(db, fset), old_pk_cols=old_pk_cols, old_feature_cols=old_feature_cols)

    logger.info('Creating Feature Set...')
    pk_columns = [Column(k.lower(), SQLALCHEMY_TYPES[fset.primary_keys[k]], primary_key=True) for k in fset.primary_keys]
    ts_columns = [Column('last_update_ts', TIMESTAMP)]
    feature_columns = [Column(f.name.lower(), SQLALCHEMY_TYPES[f.feature_data_type]) for f in get_features(db, fset)]
    columns = pk_columns + ts_columns + feature_columns
    feature_set = Table(fset.table_name.lower(), metadata, *columns, schema=fset.schema_name.lower())
    feature_set.create(db.connection())
    logger.info('Done.')

    logger.info('Creating Feature Set History...')
    pk_columns = [Column(k.lower(), SQLALCHEMY_TYPES[fset.primary_keys[k]], primary_key=True) for k in fset.primary_keys]
    ts_columns = [Column('asof_ts', TIMESTAMP, primary_key=True), Column('until_ts', TIMESTAMP, primary_key=True)]
    feature_columns = [Column(f.name.lower(), SQLALCHEMY_TYPES[f.feature_data_type]) for f in get_features(db, fset)]
    columns = pk_columns + ts_columns + feature_columns
    history = Table(f'{fset.table_name.lower()}_history', metadata, *columns, schema=fset.schema_name.lower())
    history.create(db.connection())
    logger.info('Done.')

    logger.info('Creating Historian Trigger...')
    trigger = DDL(trigger_sql)
    db.execute(trigger)
    logger.info('Done.')

    logger.info('Updating Metadata...')
    db.query(models.FeatureSet).filter(models.FeatureSet.feature_set_id==fset.feature_set_id).update({models.FeatureSet.deployed: True})
    fset.deployed = True
    logger.info('Done.')
    return fset

def validate_training_view(db: Session, name, sql_text, join_keys, label_col=None) -> None:
    """
    Validates that the training view doesn't already exist.

    :param db: SqlAlchemy Session
    :param name: The training view name
    :param sql: The training view provided SQL
    :param join_keys: The provided join keys when creating the training view
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
        valid_df = db.execute(sql_text).fetchone()
    except ProgrammingError as e:
        if '[Splice Machine][Splice]' in str(e):
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                            message=f'The provided SQL is incorrect. The following error was raised during '
                                            f'validation:\n\n{str(e)}') from None
        raise e

    # Ensure the label column specified is in the output of the SQL
    if label_col and not label_col in valid_df.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_SQL,
                                        message=f"Provided label column {label_col} is not available in the provided SQL")
    # Confirm that all join_keys provided correspond to primary keys of created feature sets
    pks = set(i[0].upper() for i in db.query(distinct(models.FeatureSetKey.key_column_name)).all())
    missing_keys = set(i.upper() for i in join_keys) - pks
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
        for f in features_rows:
            # Have to convert this to a dictionary because the models.Feature object enforces the type of 'tags'
            d = f.__dict__
            d['tags'] = json.loads(d['tags'])
            features.append(schemas.Feature(**d))
    return features

def get_feature_schema_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f'\n\t{f.name}  {f.feature_data_type}' for f in get_features(db, fset)])

def get_feature_column_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f.name for f in get_features(db, fset)])

def table_exists(db, schema_name, table_name):
    inspector = peer_into_splice_db(db.get_bind())
    if schema_name.lower() not in [value.lower() for value in inspector.get_schema_names()]: return False
    return table_name.lower() in [value.lower() for value in inspector.get_table_names(schema=schema_name)]
