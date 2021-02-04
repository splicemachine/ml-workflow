from fastapi import APIRouter, status, Depends, HTTPException, Body, Query
from typing import List, Dict, Optional, Union, Any
from shared.logger.logging_config import logger
from shared.models.feature_store_models import FeatureSet, TrainingView, Feature
from sqlalchemy.orm import Session
from datetime import datetime
from ..constants import SQL
from .. import schemas, crud
from ..training_utils import (dict_to_lower,_get_training_view_by_name, 
                                _get_training_set, _get_training_set_from_view)
from ..utils import __validate_feature_data_type

# Synchronous API Router-- we can mount it to the main API
SYNC_ROUTER = APIRouter()


@SYNC_ROUTER.get('/feature-sets', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureSet],
                 description="Returns a list of available feature sets", operation_id='get_feature_sets')
async def get_feature_sets(fsid: Optional[List[int]] = Query(None), db: Session = Depends(crud.get_db)):
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
    """
    if name:
        return _get_training_view_by_name(db, name)
    else:
        return crud.get_training_views(db)

@SYNC_ROUTER.get('/training-view-id', status_code=status.HTTP_200_OK, response_model=int,
                description="Returns the unique view ID from a name", operation_id='get_training_view_id')
async def get_training_view_id(name: str, db: Session = Depends(crud.get_db)):
    """
    Returns the unique view ID from a name
    """
    return crud.get_training_view_id(db, name)

@SYNC_ROUTER.get('/features', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureDescription],
                description="Returns a dataframe or list of features whose names are provided", operation_id='get_features_by_name')
async def get_features_by_name(name: List[str] = Query([]), db: Session = Depends(crud.get_db)):
    """
    Returns a list of features whose names are provided

    """
    return crud.get_features_by_name(db, name)

@SYNC_ROUTER.delete('/feature-sets', status_code=status.HTTP_200_OK,
                description="Removes a feature set", operation_id='remove_feature_set')
async def remove_feature_set(db: Session = Depends(crud.get_db)):
    # TODO
    raise NotImplementedError

@SYNC_ROUTER.post('/feature-vector', status_code=status.HTTP_200_OK, response_model=Union[Dict[str, Any], str],
                description="Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets", operation_id='get_feature_vector')
async def get_feature_vector(features: List[Union[str, schemas.FeatureDescription]],
                    join_key_values: Dict[str, Union[str, int]], sql: bool = False, db: Session = Depends(crud.get_db)):
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets
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
    """

    tctx = _get_training_view_by_name(db, view)[0]

    return crud.get_feature_vector_sql(db, features, tctx)

@SYNC_ROUTER.get('/feature-primary-keys', status_code=status.HTTP_200_OK, response_model=Dict[str, List[str]],
                description="Returns a dictionary mapping each individual feature to its primary key(s).", operation_id='get_feature_primary_keys')
async def get_feature_primary_keys(features: List[str] = Query([]), db: Session = Depends(crud.get_db)):
    """
    Returns a dictionary mapping each individual feature to its primary key(s). This function is not yet implemented.
    """
    pass

@SYNC_ROUTER.get('/training-view-features', status_code=status.HTTP_200_OK, response_model=List[schemas.Feature],
                description="Returns the available features for the given a training view name", operation_id='get_training_view_features')
async def get_training_view_features(view: str, db: Session = Depends(crud.get_db)):
    """
    Returns the available features for the given a training view name
    """
    return crud.get_training_view_features(db, view)

@SYNC_ROUTER.get('/feature-description', status_code=status.HTTP_200_OK,
                description="Returns the description of the given feature", operation_id='get_feature_description')
async def get_feature_description(db: Session = Depends(crud.get_db)):
    # TODO
    raise NotImplementedError

@SYNC_ROUTER.post('/training-sets', status_code=status.HTTP_200_OK, response_model=str,
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
    """

    return _get_training_set(db, features, start_time, end_time, current)



@SYNC_ROUTER.post('/training-set-from-view', status_code=status.HTTP_200_OK, response_model=Dict[str, Union[str, schemas.TrainingView]],
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
    """

    return _get_training_set_from_view(db, view, features, start_time, end_time)

@SYNC_ROUTER.get('/training-sets', status_code=status.HTTP_200_OK, response_model=Dict[str, Optional[str]],
                description="Returns a dictionary a training sets available, with the map name -> description.", operation_id='list_training_sets')
async def list_training_sets(db: Session = Depends(crud.get_db)):
    """
    Returns a dictionary a training sets available, with the map name -> description. If there is no description,
    the value will be an emtpy string

    """
    raise NotImplementedError("To see available training views, run fs.describe_training_views()")

@SYNC_ROUTER.post('/feature-sets', response_model=schemas.FeatureSet, status_code=status.HTTP_201_CREATED,
                description="Creates and returns a new feature set", operation_id='create_feature_set')
async def create_feature_set(fset: schemas.FeatureSetCreate, db: Session = Depends(crud.get_db)):
    """
    Creates and returns a new feature set
    """
    crud.validate_feature_set(db, fset)
    logger.info(f'Registering feature set {fset.schema_name}.{fset.table_name} in Feature Store')
    return crud.register_feature_set_metadata(db, fset)

@SYNC_ROUTER.post('/features', response_model=schemas.Feature, status_code=status.HTTP_201_CREATED,
                description="Add a feature to a feature set", operation_id='create_feature')
async def create_feature(fc: schemas.FeatureCreate, schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Add a feature to a feature set
    """
    __validate_feature_data_type(fc.feature_data_type)
    if crud.table_exists(db, schema, table):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Feature Set {schema}.{table} is already deployed. You cannot "
                                        f"add features to a deployed feature set.")
    fsets: List[schemas.FeatureSet] = crud.get_feature_sets(db, _filter={'table_name': table, 'schema_name': schema})
    if not fsets:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Feature Set {schema}.{table} does not exist. Please enter "
                                        f"a valid feature set.")
    fset = fsets[0]
    crud.validate_feature(db, fc.name)
    fc.feature_set_id = fset.feature_set_id
    logger.info(f'Registering feature {fc.name} in Feature Store')
    return crud.register_feature_metadata(db, fc)

@SYNC_ROUTER.post('/training-views', status_code=status.HTTP_201_CREATED,
                description="Registers a training view for use in generating training SQL", operation_id='create_training_view')
async def create_training_view(tv: schemas.TrainingViewCreate, db: Session = Depends(crud.get_db)):
    """
    Registers a training view for use in generating training SQL
    """
    # assert tv.name != "None", "Name of training view cannot be None!"
    try:
        crud.validate_training_view(db, tv.name, tv.sql_text, tv.join_columns, tv.label_column)
        tv.label_column = f"'{tv.label_column}'" if tv.label_column else "NULL"  # Formatting incase NULL
        crud.create_training_view(db, tv)
    except HTTPException as e:
        db.rollback()
        raise e

@SYNC_ROUTER.post('/deploy-feature-set', response_model=schemas.FeatureSet, status_code=status.HTTP_200_OK,
                description="Deploys a feature set to the database", operation_id='deploy_feature_set')
async def deploy_feature_set(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Deploys a feature set to the database. This persists the feature stores existence.
    As of now, once deployed you cannot delete the feature set or add/delete features.
    The feature set must have already been created with :py:meth:`~features.FeatureStore.create_feature_set`
    """
    try:
        fset = crud.get_feature_sets(db, _filter={'schema_name': schema, 'table_name': table})[0]
    except:
        raise HTTPException(
            status_code=404, detail=f"Cannot find feature set {schema}.{table}. Ensure you've created this"
            f"feature set using fs.create_feature_set before deploying.")
    return crud.deploy_feature_set(db, fset)

@SYNC_ROUTER.get('/feature-set-descriptions', status_code=status.HTTP_200_OK, response_model=List[Dict[str, Union[schemas.FeatureSet, List[schemas.Feature]]]],
                description="", operation_id='get_feature_set_descriptions')
async def get_feature_set_descriptions(schema: Optional[str] = None, table: Optional[str] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a description of all feature sets, with all features in the feature sets and whether the feature
    set is deployed
    """
    if schema and table:
        fsets = crud.get_feature_sets(db, _filter={"schema_name": schema, "table_name": table})
    
    else:
        fsets = crud.get_feature_sets(db)

    return [{"feature_set": fset, "features": crud.get_features(db, fset)} for fset in fsets]

@SYNC_ROUTER.get('/training-view-descriptions', status_code=status.HTTP_200_OK, response_model=List[Dict[str, Union[schemas.TrainingView, List[schemas.FeatureDescription]]]],
                description="", operation_id='get_training_view_descriptions')
async def get_training_view_descriptions(name: Optional[str] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a description of all (or the specified) training views, the ID, name, description and optional label
    """
    if name:
        tcxs = _get_training_view_by_name(db, name)
    else:
        tcxs = crud.get_training_views(db)
    descs = []
    for tcx in tcxs:
        feats: List[schemas.Feature] = crud.get_training_view_features(db, tcx.name)
        # Grab the feature set info and their corresponding names (schema.table) for the display table
        feat_sets: List[schemas.FeatureSet] = crud.get_feature_sets(db, feature_set_ids=[f.feature_set_id for f in feats])
        feat_sets: Dict[int, str] = {fset.feature_set_id: f'{fset.schema_name}.{fset.table_name}' for fset in feat_sets}
        fds = list(map(lambda f, feat_sets=feat_sets: schemas.FeatureDescription(**f.__dict__, feature_set_name=feat_sets[f.feature_set_id]), feats))
        descs.append({ "training_view": tcx, "features": fds })
    return descs

@SYNC_ROUTER.put('/feature-description', status_code=status.HTTP_200_OK,
                description="", operation_id='set_feature_description')
async def set_feature_description(db: Session = Depends(crud.get_db)):
        raise NotImplementedError

@SYNC_ROUTER.post('/training-set-from-deployment', response_model=Dict[str, Union[str, Dict[str, str]]], status_code=status.HTTP_200_OK,
                description="Reads Feature Store metadata to rebuild orginal training data set used for the given deployed model.", operation_id='get_training_set_from_deployment')
async def get_training_set_from_deployment(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Reads Feature Store metadata to rebuild orginal training data set used for the given deployed model.
    """
    # database stores object names in upper case
    metadata = crud.retrieve_training_set_metadata_from_deployement(schema, table)
    features = metadata['FEATURES'].split(',')
    tv_name = metadata['NAME']
    start_time = metadata['TRAINING_SET_START_TS']
    end_time = metadata['TRAINING_SET_END_TS']
    if tv_name:
        training_set_sql = _get_training_set_from_view(db, view=tv_name, features=features,
                                                            start_time=start_time, end_time=end_time)['sql']
    else:
        training_set_sql = _get_training_set(db, features=features, start_time=start_time, end_time=end_time)
    return { "metadata": metadata, "sql": training_set_sql }
