from sqlalchemy.orm import Session
from typing import List, Dict, Union, Optional, Any
from . import schemas
from .constants import SQL
from shared.models import feature_store_models
from shared.services.database import SQLAlchemyClient
from shared.logger.logging_config import logger
from fastapi import HTTPException
import re
import json
from datetime import datetime
from sqlalchemy import inspect as peer_into_splice_db
from .utils import __get_pk_columns, get_pk_column_str, get_pk_schema_str

FEATURE_SET_TS_COL = '\n\tLAST_UPDATE_TS TIMESTAMP'
HISTORY_SET_TS_COL = '\n\tASOF_TS TIMESTAMP,\n\tUNTIL_TS TIMESTAMP'

def get_db():
    """
    Provides SqlAlchemy Session object to path operations
    """
    db = SQLAlchemyClient.SessionFactory()
    try:
        yield db
    except Exception as e:
        logger.error(e)
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
    if table_exists(fset.schema_name, fset.table_name):
        raise HTTPException(status_code=409, detail=str)
    # Validate metadata
    if len(get_feature_sets(db, _filter={'table_name': fset.table_name, 'schema_name': fset.schema_name})) > 0:
        raise HTTPException(status_code=409, detail=str)

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
    l = len(db.execute(SQL.get_all_features.format(name=name.upper())).fetchall())
    if l > 0:
        raise HTTPException(status_code=409, detail=str)

    if not re.match('^[A-Za-z][A-Za-z0-9_]*$', name, re.IGNORECASE):
        raise HTTPException(status_code=406, detail='Feature name does not conform. Must start with an alphabetic character, '
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
        raise HTTPException(status_code=400, detail=f"The following keys were not provided and must be: {missing_keys}")

def get_feature_vector(db: Session, feats: List[schemas.Feature], join_keys: Dict[str, str], feature_sets: List[schemas.FeatureSet], return_sql: bool) -> Union[Dict[str, Any], str]:
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets

    :param db: SqlAlchemy Session
    :param features: List of Features
    :param join_key_values: (dict) join key values to get the proper Feature values formatted as {join_key_column_name: join_key_value}
    :param feature_sets: List of Feature Sets
    :param return_sql: Whether to return the SQL needed to get the vector or the values themselves. Default False
    :return: Dict or str (SQL statement)
    """
    feature_names = ','.join([f.name for f in feats])
    fset_tables = ','.join(
        [f'{fset.schema_name}.{fset.table_name} fset{fset.feature_set_id}' for fset in feature_sets])
    sql = "SELECT {feature_names} FROM {fset_tables} ".format(feature_names=feature_names, fset_tables=fset_tables)

    # For each Feature Set, for each primary key in the given feature set, get primary key value from the user provided dictionary
    pk_conditions = [f"fset{fset.feature_set_id}.{pk_col} = {join_keys[pk_col.lower()]}"
                        for fset in feature_sets for pk_col in fset.primary_keys]
    pk_conditions = ' AND '.join(pk_conditions)

    sql += f"WHERE {pk_conditions}"

    if return_sql:
        return sql
    
    vector = db.execute(sql).fetchall()

    return dict(vector[0].items()) if len(vector) > 0 else {}

def get_training_view_features(db: Session, training_view: str) -> List[schemas.FeatureDescription]:
    """
    Returns the available features for the given a training view name

    :param db: SqlAlchemy Session
    :param training_view: The name of the training view
    :return: A list of available Feature objects
    """
    where = f"tc.Name='{training_view}'"

    df = db.execute(SQL.get_training_view_features.format(where=where))

    features = []
    for feat in df.fetchall():
        f = dict((k.lower(), v) for k, v in feat.items())
        f['tags'] = json.loads(f['tags'])
        features.append(schemas.FeatureDescription(**f))
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

    sql = SQL.get_feature_sets

    # Filter by feature_set_id and filter
    if feature_set_ids or _filter:
        sql += ' WHERE '
    if feature_set_ids:
        fsd = tuple(feature_set_ids) if len(feature_set_ids) > 1 else f'({feature_set_ids[0]})'
        sql += f' fset.feature_set_id in {fsd} AND'
    for fl in _filter:
        sql += f" fset.{fl}='{_filter[fl]}' AND"
    sql = sql.rstrip('AND')

    feature_set_rows = db.execute(sql)
    for fs in feature_set_rows.fetchall():
        d = dict((k.lower(), v) for k, v in fs.items())
        pkcols = d.pop('pk_columns').split('|')
        pktypes = d.pop('pk_types').split('|')
        d['primary_keys'] = {c: k for c, k in zip(pkcols, pktypes)}
        feature_sets.append(schemas.FeatureSet(**d))
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

    sql = SQL.get_training_views

    if _filter:
        sql += ' WHERE '
        for k in _filter:
            sql += f"tc.{k}='{_filter[k]}' and"
        sql = sql.rstrip('and')

    training_view_rows = db.execute(sql)

    for tc in training_view_rows.fetchall():
        t = dict((k.lower(), v) for k, v in tc.items())
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
    return db.execute(SQL.get_training_view_id.format(name=name)).fetchall()[0].values()[0]

def get_features_by_name(db: Session, names: Optional[List[str]]) -> List[schemas.Feature]:
    """
    Returns a dataframe or list of features whose names are provided

    :param db: SqlAlchemy Session
    :param names: The list of feature names
    :return: List[Feature] The list of Feature objects and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    # If they don't pass in feature names, get all features
    where_clause = "name in (" + ",".join([f"'{i.upper()}'" for i in names]) + ")"
    df = db.execute(SQL.get_features_by_name.format(where=where_clause))

    features = []
    for feat in df.fetchall():
        f = dict((k.lower(), v) for k, v in feat.items())  # DB returns uppercase column names
        f['tags'] = json.loads(f['tags'])
        features.append(schemas.Feature(**f))
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

def register_feature_set_metadata(db: Session, fset: schemas.FeatureSetCreate) -> None:
    fset_metadata = SQL.feature_set_metadata.format(schema=fset.schema_name, table=fset.table_name,
                                                    desc=fset.description)

    db.execute(fset_metadata)
    fsid_results = db.execute(SQL.get_feature_set_id.format(schema=fset.schema_name,
                                                            table=fset.table_name))
    fsid = fsid_results.fetchall()[0].values()[0]

    for pk in __get_pk_columns(fset):
        pk_sql = SQL.feature_set_pk_metadata.format(
            feature_set_id=fsid, pk_col_name=pk.upper(), pk_col_type=fset.primary_keys[pk]
        )
        db.execute(pk_sql)
    return schemas.FeatureSet(**fset.__dict__, feature_set_id=fsid)

def register_feature_metadata(db: Session, f: schemas.FeatureCreate) -> None:
    """
    Registers the feature's existence in the feature store
    :param db: SqlAlchemy Session
    :param f: (Feature) the feature to register
    :return: None
    """
    feature_sql = SQL.feature_metadata.format(
        feature_set_id=f.feature_set_id, name=f.name, desc=f.description,
        feature_data_type=f.feature_data_type,
        feature_type=f.feature_type, tags=json.dumps(f.tags)
    )
    db.execute(feature_sql)

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
        raise HTTPException(status_code=409, detail="It seems you've passed in Features that are neither" \
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

    feature_set_sql = SQL.feature_set_table.format(
        schema=fset.schema_name, table=fset.table_name, pk_columns=get_pk_schema_str(fset),
        ts_columns=FEATURE_SET_TS_COL, feature_columns=get_feature_schema_str(db, fset),
        pk_list=get_pk_column_str(fset)
    )

    history_sql = SQL.feature_set_table.format(
        schema=fset.schema_name, table=f'{fset.table_name}_history', pk_columns=get_pk_schema_str(fset),
        ts_columns=HISTORY_SET_TS_COL, feature_columns=get_feature_schema_str(db, fset),
        pk_list=get_pk_column_str(fset, history=True))

    trigger_sql = SQL.feature_set_trigger.format(
        schema=fset.schema_name, table=fset.table_name, pk_list=get_pk_column_str(fset),
        feature_list=get_feature_column_str(db, fset), old_pk_cols=old_pk_cols, old_feature_cols=old_feature_cols)

    print('Creating Feature Set...', end=' ')
    db.execute(feature_set_sql)
    print('Done.')
    print('Creating Feature Set History...', end=' ')
    db.execute(history_sql)
    print('Done.')
    print('Creating Historian Trigger...', end=' ')
    db.execute(trigger_sql)
    print('Done.')
    print('Updating Metadata...')
    db.execute(SQL.update_fset_deployment_status.format(status=int(True),
                                                        feature_set_id=fset.feature_set_id))
    fset.deployed = True
    print('Done.')
    return fset

def validate_training_view(db: Session, name, sql, join_keys, label_col=None) -> None:
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
        raise HTTPException(status_code=409, detail=f"Training View {name} already exists!")

    # Column comparison
    # Lazily evaluate sql resultset, ensure that the result contains all columns matching pks, join_keys, tscol and label_col
    from py4j.protocol import Py4JJavaError
    try:
        valid_df = db.execute(f'SELECT * FROM ({sql}) {{limit 1}}')
    except Py4JJavaError as e:
        if 'SQLSyntaxErrorException' in str(e.java_exception):
            raise HTTPException(status_code=406, detail=f'The provided SQL is incorrect. The following error was raised during '
                                            f'validation:\n\n{str(e.java_exception)}') from None
        raise e

    # Ensure the label column specified is in the output of the SQL
    if label_col and not label_col in valid_df.keys():
        raise HTTPException(status_code=400, detail=f"Provided label column {label_col} is not available in the provided SQL")
    # Confirm that all join_keys provided correspond to primary keys of created feature sets
    pks = set(i.values()[0].upper() for i in db.execute(SQL.get_fset_primary_keys).fetchall())
    missing_keys = set(i.upper() for i in join_keys) - pks
    if missing_keys:
        raise HTTPException(status_code=400, detail=f"Not all provided join keys exist. Remove {missing_keys} or " \
                                f"create a feature set that uses the missing keys")

def create_training_view(db: Session, tv: schemas.TrainingViewCreate) -> None:
    """
    Registers a training view for use in generating training SQL

    :param db: SqlAlchemy Session
    :param tv: The training view to register
    :return: None
    """
    
    train_sql = SQL.training_view.format(name=tv.name, desc=tv.description or 'None Provided', sql_text=tv.sql_text, ts_col=tv.ts_column,
                                            label_col=tv.label_column)
    print('Building training sql...')
    # if verbose: print('\t', train_sql)
    db.execute(train_sql)
    print('Done.')

    # Get generated view ID
    vid = get_training_view_id(db, tv.name)

    print('Creating Join Keys')
    for i in tv.join_columns:
        key_sql = SQL.training_view_keys.format(view_id=vid, key_column=i.upper(), key_type='J')
        print(f'\tCreating Join Key {i}...')
        # if verbose: print('\t', key_sql)
        db.execute(key_sql)
    print('Done.')
    print('Creating Training View Primary Keys')
    for i in tv.pk_columns:
        key_sql = SQL.training_view_keys.format(view_id=vid, key_column=i.upper(), key_type='P')
        print(f'\tCreating Primary Key {i}...')
        # if verbose: print('\t', key_sql)
        db.execute(key_sql)
    print('Done.')

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
        features_rows = db.execute(SQL.get_features_in_feature_set.format(feature_set_id=fset.feature_set_id)).fetchall()
        for f in features_rows:
            d = dict((k.lower(), v) for k, v in f.items())
            d['tags'] = json.loads(d['tags'])
            features.append(schemas.Feature(**d))
    return features

def get_feature_schema_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f'\n\t{f.name}  {f.feature_data_type}' for f in get_features(db, fset)])

def get_feature_column_str(db: Session, fset: schemas.FeatureSet):
    return ','.join([f.name for f in get_features(db, fset)])

def table_exists(schema_name, table_name):
    inspector = peer_into_splice_db(SQLAlchemyClient.engine)
    return table_name.lower() in [value.lower() for value in inspector.get_table_names(schema=schema_name)]