from fastapi import APIRouter, status, Depends, HTTPException, Body
from typing import List, Dict, Optional, Union
from shared.logger.logging_config import logger
from shared.models.feature_store_models import FeatureSet, TrainingView, Feature
from sqlalchemy.orm import Session
from datetime import datetime
from ..constants import SQL
from .. import schemas, crud
from ..training_utils import (dict_to_lower, _generate_training_set_history_sql,
                                   _generate_training_set_sql, _create_temp_training_view)

# Synchronous API Router-- we can mount it to the main API
SYNC_ROUTER = APIRouter()


@SYNC_ROUTER.get('/feature-sets', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureSet],
                 description="Returns a list of available feature sets", operation_id='get_feature_sets')
async def get_feature_sets(fsid: Optional[List[int]] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a list of available feature sets
    """
    return crud.get_feature_sets(db, fsid)

@SYNC_ROUTER.delete('/training-views', status_code=status.HTTP_200_OK,
                description="Removes a training view", operation_id='remove_training_view')
async def remove_training_view(override=False, db: Session = Depends(crud.get_db)):
    """
    Note: This function is not yet implemented.
    Removes a training view. This will run 2 checks.
        1. See if the training view is being used by a model in a deployment. If this is the case, the function will fail, always.
        2. See if the training view is being used in any mlflow runs (non-deployed models). This will fail and return
        a warning Telling the user that this training view is being used in mlflow runs (and the run_ids) and that
        they will need to "override" this function to forcefully remove the training view.
    """
    raise NotImplementedError

@SYNC_ROUTER.get('/training-views', status_code=status.HTTP_200_OK, response_model=Union[schemas.TrainingView, List[schemas.TrainingView]],
                description="Returns a list of all available training views with an optional filter", operation_id='get_training_views')
async def get_training_views(name: Optional[str] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a list of all available training views with an optional filter

    :param _filter: Dictionary container the filter keyword (label, description etc) and the value to filter on
        If None, will return all TrainingViews
    :return: List[TrainingView]
    """
    if name:
        tvs = crud.get_training_views(db, {'name': name})
        if len(tv) == 0:
            raise HTTPException(status_code=404, detail=f'Could not find training view with name "{name}"')
        return tvs[0]
    else:
        return crud.get_training_views(db)

@SYNC_ROUTER.get('/training-view-id', status_code=status.HTTP_200_OK, response_model=int,
                description="Returns the unique view ID from a name", operation_id='get_training_view_id')
async def get_training_view_id(name: str, db: Session = Depends(crud.get_db)):
    """
    Returns the unique view ID from a name

    :param name: The training view name
    :return: The training view id
    """
    return crud.get_training_view_id(db, name)

@SYNC_ROUTER.get('/features', status_code=status.HTTP_200_OK, response_model=List[schemas.Feature],
                description="Returns a dataframe or list of features whose names are provided", operation_id='get_features_by_name')
async def get_features_by_name(name: Optional[List[str]] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a dataframe or list of features whose names are provided

    :param names: The list of feature names
    :param as_list: Whether or not to return a list of features. Default False
    :return: SparkDF or List[Feature] The list of Feature objects or Spark Dataframe of features and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    return crud.get_features_by_name(db, name)

@SYNC_ROUTER.delete('/feature-sets', status_code=status.HTTP_200_OK,
                description="Removes a feature set", operation_id='remove_feature_set')
async def remove_feature_set(db: Session = Depends(crud.get_db)):
    # TODO
    raise NotImplementedError


@SYNC_ROUTER.post('/feature-vector', status_code=status.HTTP_200_OK, response_model=Union[str, List[str]],
                description="Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets", operation_id='get_feature_vector')
async def get_feature_vector(self, features: List[Union[str, schemas.Feature]],
                    join_key_values: Dict[str, str], sql: bool = False):
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets

    :param features: List of str Feature names or Features
    :param join_key_values: (dict) join key values to get the proper Feature values formatted as {join_key_column_name: join_key_value}
    :param return_sql: Whether to return the SQL needed to get the vector or the values themselves. Default False
    :return: Pandas Dataframe or str (SQL statement)
    """
    feats: List[schemas.Feature] = crud.process_features(db, features)
    # Match the case of the keys
    join_keys = dict_to_lower(join_key_values)

    # Get the feature sets and their primary key column names
    feature_sets = crud.get_feature_sets(db, [f.feature_set_id for f in feats])
    crud.validate_feature_vector_keys(join_keys, feature_sets)

    return crud.get_feature_vector(db, feats, join_keys, feature_sets, sql)


@SYNC_ROUTER.post('/feature-vector-sql', status_code=status.HTTP_200_OK, response_model=str,
                description="Returns the parameterized feature retrieval SQL used for online model serving.", operation_id='get_feature_vector_sql_from_training_view')
async def get_feature_vector_sql_from_training_view(features: List[schemas.Feature], view: str, db: Session = Depends(crud.get_db)):
    """
    Returns the parameterized feature retrieval SQL used for online model serving.

    :param training_view: (str) The name of the registered training view
    :param features: (List[str]) the list of features from the feature store to be included in the training

        :NOTE:
            .. code-block:: text

                This function will error if the view SQL is missing a view key required to retrieve the\
                desired features

    :return: (str) the parameterized feature vector SQL
    """

    # Get training view information (ctx primary key column(s), ctx primary key inference ts column, )
    # vid = crud.get_training_view_id(db, view)
    # tctx = crud.get_training_views(db, _filter={'view_id': vid})[0]
    tctx = get_training_views(view, db)

    return crud.get_feature_vector_sql(db, features, tctx)

@SYNC_ROUTER.get('/feature-primary-keys', status_code=status.HTTP_200_OK, response_model=Dict[str, List[str]],
                description="Returns a dictionary mapping each individual feature to its primary key(s).", operation_id='get_feature_primary_keys')
async def get_feature_primary_keys(features: List[str], db: Session = Depends(crud.get_db)):
    """
    Returns a dictionary mapping each individual feature to its primary key(s). This function is not yet implemented.

    :param features: (List[str]) The list of features to get primary keys for
    :return: Dict[str, List[str]] A mapping of {feature name: [pk1, pk2, etc]}
    """
    pass

@SYNC_ROUTER.get('/training-view-features', status_code=status.HTTP_200_OK, response_model=List[schemas.Feature],
                description="Returns the available features for the given a training view name", operation_id='get_training_view_features')
async def get_training_view_features(view: str, db: Session = Depends(crud.get_db)):
    """
    Returns the available features for the given a training view name

    :param training_view: The name of the training view
    :return: A list of available Feature objects
    """
    return crud.get_training_view_features(db, view)

@SYNC_ROUTER.get('/feature-description', status_code=status.HTTP_200_OK,
                description="Returns the description of the given feature", operation_id='get_feature_description')
async def get_feature_description(db: Session = Depends(crud.get_db)):
    # TODO
    raise NotImplementedError

@SYNC_ROUTER.get('/training-set', status_code=status.HTTP_200_OK, response_model=str,
                description="Returns the SQL statement to get a set of feature values across feature sets that is not time dependent", operation_id='get_training_set')
async def get_training_set(features: Union[List[schemas.Feature], List[str]], start_time: datetime = Body(None), end_time: datetime = Body(None), 
                            current: bool = False, db: Session = Depends(crud.get_db)):
    """
    Gets a set of feature values across feature sets that is not time dependent (ie for non time series clustering).
    This feature dataset will be treated and tracked implicitly the same way a training_dataset is tracked from
    :py:meth:`features.FeatureStore.get_training_set` . The dataset's metadata and features used will be tracked in mlflow automatically (see
    get_training_set for more details).

    The way point-in-time correctness is guaranteed here is by choosing one of the Feature Sets as the "anchor" dataset.
    This means that the points in time that the query is based off of will be the points in time in which the anchor
    Feature Set recorded changes. The anchor Feature Set is the Feature Set that contains the superset of all primary key
    columns across all Feature Sets from all Features provided. If more than 1 Feature Set has the superset of
    all Feature Sets, the Feature Set with the most primary keys is selected. If more than 1 Feature Set has the same
    maximum number of primary keys, the Feature Set is chosen by alphabetical order (schema_name, table_name).

    :param features: List of Features or strings of feature names

        :NOTE:
            .. code-block:: text

                The Features Sets which the list of Features come from must have common join keys,
                otherwise the function will fail. If there is no common join key, it is recommended to
                create a Training View to specify the join conditions.

    :param current_values_only: If you only want the most recent values of the features, set this to true. Otherwise, all history will be returned. Default False
    :param start_time: How far back in history you want Feature values. If not specified (and current_values_only is False), all history will be returned.
        This parameter only takes effect if current_values_only is False.
    :param end_time: The most recent values for each selected Feature. This will be the cutoff time, such that any Feature values that
        were updated after this point in time won't be selected. If not specified (and current_values_only is False),
        Feature values up to the moment in time you call the function (now) will be retrieved. This parameter
        only takes effect if current_values_only is False.
    :return: Spark DF
    """
    # Get List[Feature]
    features = crud.process_features(db, features)

    # Get the Feature Sets
    fsets = crud.get_feature_sets(db, list({f.feature_set_id for f in features}))

    if current_values_only:
        sql = _generate_training_set_sql(features, fsets)
    else:
        temp_vw = _create_temp_training_view(features, fsets)
        sql = _generate_training_set_history_sql(temp_vw, features, fsets, start_time=start_time, end_time=end_time)
    return sql

@SYNC_ROUTER.get('training-set-from-view', status_code=status.HTTP_200_OK, response_model=Dict[str, Union[str, schemas.TrainingView]],
                description="Returns the SQL statement to get a set of feature values across feature sets that is not time dependent", operation_id='get_training_set_from_view')
async def get_training_set_from_view(view: str, features: Union[List[schemas.Feature], List[str]] = None,
                                start_time: Optional[datetime] = Body(None), end_time: Optional[datetime] = Body(None),
                                db: Session = Depends(crud.get_db)):
    """
    Returns the training set as a Spark Dataframe from a Training View. When a user calls this function (assuming they have registered
    the feature store with mlflow using :py:meth:`~mlflow.register_feature_store` )
    the training dataset's metadata will be tracked in mlflow automatically. The following will be tracked:
    including:
        * Training View
        * Selected features
        * Start time
        * End time
    This tracking will occur in the current run (if there is an active run)
    or in the next run that is started after calling this function (if no run is currently active).

    :param view: (str) The name of the registered training view
    :param features: (List[str] OR List[Feature]) the list of features from the feature store to be included in the training.
        If a list of strings is passed in it will be converted to a list of Feature. If not provided will return all available features.

        :NOTE:
            .. code-block:: text

                This function will error if the view SQL is missing a join key required to retrieve the
                desired features

    :param start_time: (Optional[datetime]) The start time of the query (how far back in the data to start). Default None

        :NOTE:
            .. code-block:: text

                If start_time is None, query will start from beginning of history

    :param end_time: (Optional[datetime]) The end time of the query (how far recent in the data to get). Default None

        :NOTE:
            .. code-block:: text

                If end_time is None, query will get most recently available data

    :param return_sql: (Optional[bool]) Return the SQL statement (str) instead of the Spark DF. Defaults False
    :return: Optional[SparkDF, str] The Spark dataframe of the training set or the SQL that is used to generate it (for debugging)
    """

    # Get features as list of Features
    features = crud.process_features(db, features) if features else get_training_view_features(db, training_view)

    # Get List of necessary Feature Sets
    feature_set_ids = list({f.feature_set_id for f in features})  # Distinct set of IDs
    feature_sets = crud.get_feature_sets(db, feature_set_ids)

    # Get training view information (view primary key column(s), inference ts column, )
    tvw = get_training_views(view, db)
    # Generate the SQL needed to create the dataset
    sql = _generate_training_set_history_sql(tvw, features, feature_sets, start_time=start_time, end_time=end_time)
    return { "sql": sql, "training_view": tvw}

@SYNC_ROUTER.get('/training-sets', status_code=status.HTTP_200_OK, response_model=Dict[str, Optional[str]],
                description="Returns a dictionary a training sets available, with the map name -> description.", operation_id='list_training_sets')
async def list_training_sets(db: Session = Depends(crud.get_db)):
    """
    Returns a dictionary a training sets available, with the map name -> description. If there is no description,
    the value will be an emtpy string

    :return: Dict[str, Optional[str]]
    """
    raise NotImplementedError("To see available training views, run fs.describe_training_views()")

@SYNC_ROUTER.post('/feature-sets', response_model=schemas.FeatureSet, status_code=201,
                description="Creates and returns a new feature set", operation_id='create_feature_set')
async def create_feature_set(fset: schemas.FeatureSetCreate, db: Session = Depends(crud.get_db)):
    """
    Creates and returns a new feature set

    :param fset: The feature set object to be created
    :return: FeatureSet
    """
    crud.validate_feature_set(db, fset)
    logger.info(f'Registering feature set {fset.schema_name}.{fset.table_name} in Feature Store')
    return crud.register_feature_set_metadata(db, fset)
    # return fset

def __validate_feature_data_type(feature_data_type: str):
    """
    Validated that the provided feature data type is a valid SQL data type
    :param feature_data_type: the feature data type
    :return: None
    """
    from ..constants import SQL_TYPES
    if not feature_data_type.split('(')[0] in SQL_TYPES:
        raise HTTPException(status_code=406, detail=f"The datatype you've passed in, {feature_data_type} is not a valid SQL type. "
                                     f"Valid types are {SQL_TYPES}")

@SYNC_ROUTER.post('/features', response_model=schemas.Feature, status_code=201,
                description="Add a feature to a feature set", operation_id='create_feature')
async def create_feature(fc: schemas.FeatureCreate, schema_name: str, table_name: str, db: Session = Depends(crud.get_db)):
    """
    Add a feature to a feature set

    :param schema_name: The feature set schema
    :param table_name: The feature set table name to add the feature to
    :param name: The feature name
    :param feature_data_type: The datatype of the feature. Must be a valid SQL datatype
    :param feature_type: splicemachine.features.FeatureType of the feature. The available types are from the FeatureType class: FeatureType.[categorical, ordinal, continuous].
        You can see available feature types by running

        .. code-block:: python

                from splicemachine.features import FeatureType
                print(FeatureType.get_valid())

    :param desc: The (optional) feature description (default None)
    :param tags: (optional) List of (str) tag words (default None)
    :return: Feature created
    """
    __validate_feature_data_type(fc.feature_data_type)
    if crud.table_exists(schema_name, table_name):
        raise SpliceMachineException(f"Feature Set {schema_name}.{table_name} is already deployed. You cannot "
                                        f"add features to a deployed feature set.")
    fset: schemas.FeatureSet = crud.get_feature_sets(db, _filter={'table_name': table_name, 'schema_name': schema_name})[0]
    crud.validate_feature(db, fc.name)
    f = schemas.Feature(**fc.__dict__, feature_set_id=fset.feature_set_id)
    print(f'Registering feature {f.name} in Feature Store')
    crud.register_feature_metadata(db, f)
    return f

@SYNC_ROUTER.post('/training_views', status_code=201,
                description="Registers a training view for use in generating training SQL", operation_id='create_training_view')
async def create_training_view(tv: schemas.TrainingViewCreate, db: Session = Depends(crud.get_db)):
    """
    Registers a training view for use in generating training SQL

    :param name: The training set name. This must be unique to other existing training sets unless replace is True
    :param sql: (str) a SELECT statement that includes:
        * the primary key column(s) - uniquely identifying a training row/case
        * the inference timestamp column - timestamp column with which to join features (temporal join timestamp)
        * join key(s) - the references to the other feature tables' primary keys (ie customer_id, location_id)
        * (optionally) the label expression - defining what the training set is trying to predict
    :param primary_keys: (List[str]) The list of columns from the training SQL that identify the training row
    :param ts_col: The timestamp column of the training SQL that identifies the inference timestamp
    :param label_col: (Optional[str]) The optional label column from the training SQL.
    :param replace: (Optional[bool]) Whether to replace an existing training view
    :param join_keys: (List[str]) A list of join keys in the sql that are used to get the desired features in
        get_training_set
    :param desc: (Optional[str]) An optional description of the training set
    :param verbose: Whether or not to print the SQL before execution (default False)
    :return:
    """
    assert name != "None", "Name of training view cannot be None!"
    crud.validate_training_view(db, tv.name, tv.sql_text, tv.join_keys, tv.label_column)
    # register_training_view()
    tv.label_col = f"'{tv.label_col}'" if tv.label_col else "NULL"  # Formatting incase NULL
    crud.create_training_view(db, tv)

@SYNC_ROUTER.post('/deploy-feature-set', response_model=schemas.FeatureSet, status_code=status.HTTP_200_OK,
                description="Deploys a feature set to the database", operation_id='deploy_feature_set')
async def deploy_feature_set(schema_name: str, table_name: str, db: Session = Depends(crud.get_db)):
    """
    Deploys a feature set to the database. This persists the feature stores existence.
    As of now, once deployed you cannot delete the feature set or add/delete features.
    The feature set must have already been created with :py:meth:`~features.FeatureStore.create_feature_set`

    :param schema_name: The schema of the created feature set
    :param table_name: The table of the created feature set
    """
    try:
        fset = crud.get_feature_sets(db, _filter={'schema_name': schema_name, 'table_name': table_name})[0]
    except:
        raise HTTPException(
            status_code=404, detail=f"Cannot find feature set {schema_name}.{table_name}. Ensure you've created this"
            f"feature set using fs.create_feature_set before deploying.")
    return crud.deploy()
