import json
from datetime import datetime
from fastapi import APIRouter, status, Depends, Query
from typing import List, Dict, Optional, Union, Any
from shared.logger.logging_config import logger
from sqlalchemy.orm import Session
from .auth import authenticate
from .. import schemas, crud
from ..utils.training_utils import (dict_to_lower,_get_training_view_by_name,
                                _get_training_set, _get_training_set_from_view)
from ..utils.feature_utils import _deploy_feature_set, _create_feature_set, _get_feature_sets
from ..utils.pipeline_utils.pipeline_utils import (create_pipeline_entities, generate_backfill_sql,
                                                    generate_backfill_intervals, generate_pipeline_sql, _get_source)
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from ..decorators import managed_transaction
from shared.services.database import DatabaseFunctions
from ..utils.airflow_utils import Airflow

# Synchronous API Router -- we can mount it to the main API
SYNC_ROUTER = APIRouter(
    dependencies=[Depends(authenticate)]
)


@SYNC_ROUTER.get('/feature-sets', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureSet],
                description="Returns a list of available feature sets", operation_id='get_feature_sets', tags=['Feature Sets'])
@managed_transaction
def get_feature_sets(names: Optional[List[str]] = Query([], alias="name"), db: Session = Depends(crud.get_db)):
    """
    Returns a list of available feature sets
    """
    return _get_feature_sets(names, db)

@SYNC_ROUTER.get('/summary', status_code=status.HTTP_200_OK, response_model=schemas.FeatureStoreSummary,
                description="Returns feature store summary metrics", operation_id='get_summary', tags=['Feature Store'])
@managed_transaction
def get_summary(db: Session = Depends(crud.get_db)):
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
        * 5 Most newly added features
        * 5 Most used features (across deployments)
    """
    return crud.get_fs_summary(db)

@SYNC_ROUTER.get('/training-views', status_code=status.HTTP_200_OK, response_model=List[schemas.TrainingView],
                description="Returns a list of all available training views with an optional filter", operation_id='get_training_views', tags=['Training Views'])
@managed_transaction
def get_training_views(name: Optional[str] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a list of all available training views with an optional filter
    """
    if name:
        return _get_training_view_by_name(db, name)
    else:
        return crud.get_training_views(db)

@SYNC_ROUTER.get('/training-view-id', status_code=status.HTTP_200_OK, response_model=int,
                description="Returns the unique view ID from a name", operation_id='get_training_view_id', tags=['Training Views'])
@managed_transaction
def get_training_view_id(name: str, db: Session = Depends(crud.get_db)):
    """
    Returns the unique view ID from a name
    """
    return crud.get_training_view_id(db, name)

@SYNC_ROUTER.get('/features', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureDetail],
                description="Returns a list of all (or the specified) features and details", operation_id='get_features', tags=['Features'])
@managed_transaction
def get_features_by_name(names: List[str] = Query([], alias="name"), db: Session = Depends(crud.get_db)):
    """
    Returns a list of features whose names are provided

    """
    return crud.get_feature_descriptions_by_name(db, names)

@SYNC_ROUTER.get('/feature-exists', status_code=status.HTTP_200_OK, response_model=bool,
                description="Returns whether or not a feature name exists", operation_id='feature_exists', tags=['Features'])
@managed_transaction
def feature_exists(name: str, db: Session = Depends(crud.get_db)):
    """
    Returns whether or not a given feature exists

    """
    return bool(crud.get_features_by_name(db, [name]))

@SYNC_ROUTER.get('/feature-details', status_code=status.HTTP_200_OK, response_model=schemas.FeatureDetail,
                description="Returns Feature Details for a single feature", operation_id='get_feature_details', tags=['Features'])
@managed_transaction
def get_features_details(name: str, db: Session = Depends(crud.get_db)):
    """
    Returns a list of features whose names are provided. Same as /features but we keep it here for continuity with other
    routes (Feature Sets and Training Views have a "details" route
    """
    dets = crud.get_feature_descriptions_by_name(db, [name])
    if not dets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature {name} does not exist. Please enter a valid feature.")
    return dets[0]

@SYNC_ROUTER.post('/feature-vector', status_code=status.HTTP_200_OK, response_model=Union[Dict[str, Any], str],
                description="Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets", 
                operation_id='get_feature_vector', tags=['Features'])
@managed_transaction
def get_feature_vector(fjk: schemas.FeatureJoinKeys, pks: bool = True, sql: bool = False, db: Session = Depends(crud.get_db)):
    """
    Gets a feature vector given a list of Features and primary key values for their corresponding Feature Sets
    """
    feats: List[schemas.Feature] = crud.process_features(db, fjk.features)
    # Match the case of the keys
    join_keys = dict_to_lower(fjk.join_key_values)

    # Get the feature sets and their primary key column names
    feature_sets = crud.get_feature_sets(db, [f.feature_set_id for f in feats])
    crud.validate_feature_vector_keys(join_keys, feature_sets)

    return crud.get_feature_vector(db, feats, join_keys, feature_sets, pks, sql)


@SYNC_ROUTER.post('/feature-vector-sql', status_code=status.HTTP_200_OK, response_model=str,
                description="Returns the parameterized feature retrieval SQL used for online model serving.", 
                operation_id='get_feature_vector_sql_from_training_view', tags=['Features'])
@managed_transaction
def get_feature_vector_sql_from_training_view(features: List[Union[schemas.Feature, str]], view: str, db: Session = Depends(crud.get_db)):
    """
    Returns the parameterized feature retrieval SQL used for online model serving.
    """
    feats = crud.process_features(db, features)

    tctx = _get_training_view_by_name(db, view)[0]

    return crud.get_feature_vector_sql(db, feats, tctx)

@SYNC_ROUTER.get('/feature-primary-keys', status_code=status.HTTP_200_OK, response_model=Dict[str, List[str]],
                description="Returns a dictionary mapping each individual feature to its primary key(s).", 
                operation_id='get_feature_primary_keys', tags=['Features'])
@managed_transaction
def get_feature_primary_keys(features: List[str] = Query([], alias="feature"), db: Session = Depends(crud.get_db)):
    """
    Returns a dictionary mapping each individual feature to its primary key(s). This function is not yet implemented.
    """
    pass

@SYNC_ROUTER.get('/training-view-features', status_code=status.HTTP_200_OK, response_model=List[schemas.Feature],
                description="Returns the available features for the given a training view name", 
                operation_id='get_training_view_features', tags=['Training Views'])
@managed_transaction
def get_training_view_features(view: str, db: Session = Depends(crud.get_db)):
    """
    Returns the available features for the given a training view name
    """
    return crud.get_training_view_features(db, view)


@SYNC_ROUTER.post('/training-sets', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                description="Gets a set of feature values across feature sets that is not time dependent (ie for non time series clustering)", 
                operation_id='get_training_set', tags=['Training Sets'])
@managed_transaction
def get_training_set(ftf: schemas.FeatureTimeframe, current: bool = False, label: str = None,
                            return_pk_cols: bool = Query(False, alias='pks'), return_ts_col: bool = Query(False, alias='ts'), 
                            db: Session = Depends(crud.get_db)):
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
    create_time = crud.get_current_time(db)
    return _get_training_set(db, ftf.features, create_time, ftf.start_time, ftf.end_time, current, label, return_pk_cols, return_ts_col)

@SYNC_ROUTER.post('/training-set-from-view', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                description="Returns the training set as a Spark Dataframe from a Training View", 
                operation_id='get_training_set_from_view', tags=['Training Sets'])
@managed_transaction
def get_training_set_from_view(view: str, ftf: schemas.FeatureTimeframe, return_pk_cols: bool = Query(False, alias='pks'), 
                                        return_ts_col: bool = Query(False, alias='ts'), db: Session = Depends(crud.get_db)):
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
    create_time = crud.get_current_time(db)
    return _get_training_set_from_view(db, view, create_time, ftf.features, ftf.start_time, ftf.end_time, return_pk_cols, return_ts_col)

@SYNC_ROUTER.get('/training-sets', status_code=status.HTTP_200_OK, response_model=Dict[str, Optional[str]],
                description="Returns a dictionary a training sets available, with the map name -> description.", 
                operation_id='list_training_sets', tags=['Training Sets'])
@managed_transaction
def list_training_sets(db: Session = Depends(crud.get_db)):
    """
    Returns a dictionary a training sets available, with the map name -> description. If there is no description,
    the value will be an emtpy string. NOT YET IMPLEMENTED

    """
    raise SpliceMachineException("To see available training views, run fs.describe_training_views()")

@SYNC_ROUTER.post('/feature-sets', status_code=status.HTTP_201_CREATED, response_model=schemas.FeatureSet, 
                description="Creates and returns a new feature set", operation_id='create_feature_set', tags=['Feature Sets'])
@managed_transaction
def create_feature_set(fset: schemas.FeatureSetCreate, db: Session = Depends(crud.get_db)):
    """
    Creates and returns a new feature set
    """
    return _create_feature_set(fset, db)


@SYNC_ROUTER.get('/feature-set-exists', status_code=status.HTTP_200_OK, response_model=bool,
                description="Returns whether or not a feature set exists", operation_id='feature_set_exists', tags=['Feature Set'])
@managed_transaction
def feature_set_exists(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Returns whether or not the provided feature set exists
    """

    return bool(crud.get_feature_sets(db, feature_set_names=[f'{schema}.{table}']))

@SYNC_ROUTER.post('/features', status_code=status.HTTP_201_CREATED, response_model=schemas.Feature,
                description="Add a feature to a feature set", operation_id='create_feature', tags=['Features'])
@managed_transaction
def create_feature(fc: schemas.FeatureCreate, schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Add a feature to a feature set
    """

    if DatabaseFunctions.table_exists(schema, table, db.get_bind()):
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_DEPLOYED,
                                        message=f"Feature Set {schema}.{table} is already deployed. You cannot "
                                        f"add features to a deployed feature set.")
    fsets: List[schemas.FeatureSet] = crud.get_feature_sets(db, _filter={'table_name': table, 'schema_name': schema})
    if not fsets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature Set {schema}.{table} does not exist. Please enter "
                                        f"a valid feature set.")
    fset = fsets[0]
    crud.validate_feature(db, fc.name, fc.feature_data_type)
    fc.feature_set_id = fset.feature_set_id
    logger.info(f'Registering feature {fc.name} in Feature Store')
    return crud.register_feature_metadata(db, fc)

@SYNC_ROUTER.post('/training-views', status_code=status.HTTP_201_CREATED,
                description="Registers a training view for use in generating training SQL", 
                operation_id='create_training_view', tags=['Training Views'])
@managed_transaction
def create_training_view(tv: schemas.TrainingViewCreate, db: Session = Depends(crud.get_db)):
    """
    Registers a training view for use in generating training SQL
    """
    if not tv.name:
        raise SpliceMachineException(
            status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
            message="Name of training view cannot be None!")

    crud.validate_training_view(db, tv)
    crud.create_training_view(db, tv)

@SYNC_ROUTER.post('/deploy-feature-set', status_code=status.HTTP_200_OK, response_model=schemas.FeatureSet,
                description="Deploys a feature set to the database", operation_id='deploy_feature_set', tags=['Feature Sets'])
@managed_transaction
def deploy_feature_set(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Deploys a feature set to the database. This persists the feature stores existence.
    As of now, once deployed you cannot delete the feature set or add/delete features.
    The feature set must have already been created with :py:meth:`~features.FeatureStore.create_feature_set`
    """
    return _deploy_feature_set(schema, table, db)

@SYNC_ROUTER.get('/feature-set-details', status_code=status.HTTP_200_OK, response_model=List[schemas.FeatureSetDetail],
                description="Returns a description of all feature sets, with all features in the feature sets and whether the feature set is deployed", 
                operation_id='get_feature_set_details', tags=['Feature Sets'])
@managed_transaction
def get_feature_set_descriptions(schema: Optional[str] = None, table: Optional[str] = None, db: Session = Depends(crud.get_db)):
    """
    Returns a description of all feature sets, with all features in the feature sets and whether the feature
    set is deployed
    """
    if schema and table:
        fsets = crud.get_feature_sets(db, _filter={"schema_name": schema, "table_name": table})
    
    else:
        fsets = crud.get_feature_sets(db)

    return [schemas.FeatureSetDetail(**fset.__dict__, features=crud.get_features(db, fset)) for fset in fsets]

@SYNC_ROUTER.get('/training-view-details', status_code=status.HTTP_200_OK, response_model=List[schemas.TrainingViewDetail],
                description="Returns a description of all (or the specified) training views, the ID, name, description and optional label", 
                operation_id='get_training_view_details', tags=['Training Views'])
@managed_transaction
def get_training_view_descriptions(name: Optional[str] = None, db: Session = Depends(crud.get_db)):
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
        fds = list(map(lambda f, feat_sets=feat_sets: schemas.FeatureDetail(**f.__dict__, feature_set_name=feat_sets[f.feature_set_id]), feats))
        descs.append(schemas.TrainingViewDetail(**tcx.__dict__, features=fds))
    return descs

@SYNC_ROUTER.put('/features', status_code=status.HTTP_200_OK, response_model=schemas.Feature,
                 description="Updates a feature's metadata (description, tags, attributes)",
                 operation_id='set_feature_description', tags=['Features'])
@managed_transaction
def update_feature_metadata(name: str, metadata: schemas.FeatureMetadata, db: Session = Depends(crud.get_db)):
        fs = crud.get_feature_descriptions_by_name(db, [name])
        if not fs:
            raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature {name} does not exist. Please enter a valid feature.")
        return crud.update_feature_metadata(db, fs[0].name, desc=metadata.description,
                                     tags=metadata.tags, attributes=metadata.attributes)


@SYNC_ROUTER.get('/training-set-from-deployment', status_code=status.HTTP_200_OK, response_model=schemas.TrainingSet,
                description="Reads Feature Store metadata to rebuild orginal training data set used for the given deployed model.", 
                operation_id='get_training_set_from_deployment', tags=['Training Sets'])
@managed_transaction
def get_training_set_from_deployment(schema: str, table: str, label: str = None, 
                            return_pk_cols: bool = Query(False, alias='pks'), return_ts_col: bool = Query(False, alias='ts'), 
                            db: Session = Depends(crud.get_db)):
    """
    Reads Feature Store metadata to rebuild orginal training data set used for the given deployed model.
    """
    # database stores object names in upper case
    metadata = crud.retrieve_training_set_metadata_from_deployment(db, schema, table)
    features = metadata.features.split(',')
    tv_name = metadata.name
    start_time = metadata.training_set_start_ts
    end_time = metadata.training_set_end_ts
    create_time = metadata.training_set_create_ts

    if tv_name:
        ts = _get_training_set_from_view(db, view=tv_name, create_time=create_time, features=features, start_time=start_time, 
                                            end_time=end_time, return_pk_cols=return_pk_cols, return_ts_col=return_ts_col)
    else:
        ts = _get_training_set(db, features=features, create_time=create_time, start_time=start_time, end_time=end_time,
                                label=label, return_pk_cols=return_pk_cols, return_ts_col=return_ts_col)

    ts.metadata = metadata
    return ts

@SYNC_ROUTER.delete('/features', status_code=status.HTTP_200_OK, description="Remove a feature", 
                    operation_id='remove_feature', tags=['Features'])
@managed_transaction
def remove_feature(name: str, db: Session = Depends(crud.get_db)):
    """
    Removes a feature from the Feature Store
    """
    features = crud.get_features_by_name(db, [name])
    if not features:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature {name} does not exist. Please enter a valid feature.")
    feature, schema, table, deployed = features[0]
    if bool(deployed):
        raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.ALREADY_DEPLOYED,
                                        message=f"Cannot delete Feature {feature.name} from deployed Feature Set {schema}.{table}")
    crud.delete_feature(db, feature)

@SYNC_ROUTER.delete('/training-views', status_code=status.HTTP_200_OK, description="Remove a training view",
                    operation_id='remove_training_view', tags=['Training Views'])
@managed_transaction
def remove_training_view(name: str, db: Session = Depends(crud.get_db)):
    """
    Removes a Training View from the feature store as long as the training view isn't being used in a deployment
    """
    tvw: schemas.TrainingView = _get_training_view_by_name(db, name)[0]
    deps = crud.get_training_view_dependencies(db, tvw.view_id)
    if deps:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                     message=f'The training view {name} cannot be deleted because the following models '
                                             f'have been deployed using it: {deps}')
    tset_ids = crud.get_training_sets_from_view(db, tvw.view_id)
    # Delete the dependent training sets
    crud.delete_training_set_features(db, set(tset_ids))
    crud.delete_training_sets(db, set(tset_ids))
    # Delete the training view components (key and view)
    crud.delete_training_view_keys(db, tvw.view_id)
    crud.delete_training_view(db, tvw.view_id)

    

@SYNC_ROUTER.delete('/feature-sets', status_code=status.HTTP_200_OK, description="Removes a feature set",
                    operation_id='remove_feature_set', tags=['Feature Sets'])
@managed_transaction
def remove_feature_set(schema: str, table: str, purge: bool = False, db: Session = Depends(crud.get_db)) :
    """
    Deletes a feature set if appropriate. You can currently delete a feature set in two scenarios:
    1. The feature set has not been deployed
    2. The feature set has been deployed, but not linked to any training sets/model deployments

    :param db: SQLAlchemy Session
    :return: None
    """
    fset = crud.get_feature_sets(db, feature_set_names=[f'{schema}.{table}'])
    if not fset:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND ,code=ExceptionCodes.DOES_NOT_EXIST,
                                     message=f'The feature set ({schema}.{table}) you are trying to delete has not '
                                             'been created. Please ensure the feature set exists.')
    fset = fset[0]
    if not crud.feature_set_is_deployed(db, fset.feature_set_id):
        crud.full_delete_feature_set(db, fset, cascade=False)
    else:
        deps = crud.get_feature_set_dependencies(db, fset.feature_set_id)
        if deps['model']:
            raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                         message='You cannot drop a Feature Set that has an associated model deployment.'
                                         ' The Following models have been deployed with Training Sets that depend on this'
                                         f' Feature Set: {deps["model"]}')
        elif deps['training_set']:
            if not purge:
                raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                             message='You cannot delete a Feature Set that has associated Training Sets. '
                                                     f'The following Training Sets depend on this Feature Set: '
                                                     f'{deps["training_set"]}. To drop this Feature Set anyway, '
                                                     f'set purge=True (be careful!)')
            else:
                crud.full_delete_feature_set(db, fset, cascade=True, training_sets=deps['training_set'])
        else: # No dependencies
            crud.full_delete_feature_set(db, fset, cascade=False)
    if Airflow.is_active:
        Airflow.unschedule_feature_set_calculation(f'{fset.schema_name}.{fset.table_name}')

@SYNC_ROUTER.get('/deployments', status_code=status.HTTP_200_OK, response_model=List[schemas.DeploymentDetail],
                description="Get all deployments given either a deployment (schema/table), a training_view (name), "
                            "a feature (feat), or a feature set (fset)", operation_id='get_deployments', tags=['Deployments'])
@managed_transaction
def get_deployments(schema: Optional[str] = None, table: Optional[str] = None, name: Optional[str] = None,
                    feature = Query('', alias='feat'), feature_set = Query('', alias='fset'),
                    db: Session = Depends(crud.get_db)):
    """
    Returns a list of available deployments. If no parameters are passed in, all deployments are returned.
    Schema and Table can be passed in to get a specific deployment
    name can be passed in as a Training View name, and this will return all deployments from tha Training View
    feat can be passed in and this will return all deployments that use this feature
    fset can be passed in and this will return all deployments that use this feature set.
    You cannot pass in more than 1 of these options (schema+table counting as 1 parameter)
    """
    if schema or table or name:
        _filter = { 'model_schema_name': schema, 'model_table_name': table, 'name': name }
        _filter = { k: v for k, v in _filter.items() if v }
        return crud.get_deployments(db, _filter)
    if feature and feature_set:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message='You cannot pass in both a feature set and a feature. Only 1 is allowed')
    if feature:
        f = crud.get_feature_descriptions_by_name(db, [feature])
        if not f:
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature {feature} does not exist!")
        return crud.get_deployments(db, feature=f[0])
    if feature_set:
        fset = crud.get_feature_sets(db, feature_set_names=[feature_set])
        if not fset:
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature Set {feature_set} does not exist!")
        return crud.get_deployments(db, feature_set=fset[0])
    return crud.get_deployments(db)

@SYNC_ROUTER.get('/training-set-features', status_code=status.HTTP_200_OK, response_model=schemas.DeploymentFeatures,
                description="Returns a training set and the features associated with it", 
                operation_id='get_training_set_features', tags=['Training Sets'])
@managed_transaction
def get_training_set_features(name: str, db: Session = Depends(crud.get_db)):
    """
    Returns a training set and the features associated with it
    """
    crud.validate_schema_table([name])
    schema, table = name.split('.')
    deployments = crud.get_deployments(db, _filter={ 'model_schema_name': schema.upper(), 'model_table_name': table.upper()})
    if not deployments:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Could not find Training Set {schema}.{table}")
    ts = deployments[0]
    features = crud.get_features_from_deployment(db, ts.training_set_id)
    return schemas.DeploymentFeatures(**ts.__dict__, features=features)

@SYNC_ROUTER.post('/source', status_code=status.HTTP_201_CREATED, description="Creates a Source for Pipeline usage",
                  operation_id='create_source', tags=['Source', 'Pipeline'])
@managed_transaction
def create_source(source: schemas.Source, db: Session = Depends(crud.get_db)):
    """
    Creates a new Source
    """
    crud.validate_source(db, source.name, source.sql_text, source.pk_columns, source.event_ts_column, source.update_ts_column)
    logger.info(f'Registering source {source.name} in Feature Store')
    crud.create_source(db, source.name, source.sql_text, source.pk_columns, source.event_ts_column, source.update_ts_column)

@SYNC_ROUTER.delete('/source', status_code=status.HTTP_200_OK, description="Removes a Source from the Feature Store",
                  operation_id='remove_source', tags=['Source', 'Pipeline'])
@managed_transaction
def remove_source(name: str, db: Session = Depends(crud.get_db)):
    """
    Removes a Source if possible. Sources can only be removed if it is not being used in a Pipeline. If you want to
    remove a Source that is being used in a Pipeline, you must first delete the Feature Set that the pipeline is feeding
    (which will delete the Pipeline with it).
    """
    source: schemas.Source = crud.get_source(db, name)
    if not source:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Cannot find source with name {name}")
    fsets = crud.get_source_dependencies(db, source.source_id)
    if fsets:
        raise SpliceMachineException(status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.DEPENDENCY_CONFLICT,
                                     message=f'Source {name} has dependent Feature Sets that are being fed with Pipelines.'
                                             f' Remove the following Feature Sets before removing this Source: {fsets}')
    crud.delete_source(db, source.source_id)

@SYNC_ROUTER.get('/source', status_code=status.HTTP_200_OK, response_model=schemas.Source,
                 description="Gets a Source by name", operation_id='create_source', tags=['Source', 'Pipeline'])
@managed_transaction
def get_source(name: str, db: Session = Depends(crud.get_db)):
    """
    Gets a Source by name
    """
    return _get_source(name, db)


@SYNC_ROUTER.get('/backfill-sql', status_code=status.HTTP_200_OK, response_model=str,
                 description="Get backfill SQL for a feature set/source", operation_id='get_backfill_sql',
                 tags=['Source', 'Pipeline', 'Backfill', 'SQL'])
@managed_transaction
def get_backfill_sql(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Generates the backfill SQL for a feature set and source
    """
    fset: List[schemas.FeatureSet] = _get_feature_sets(names=[f'{schema}.{table}'], db=db)
    if not fset:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Feature Set {schema}.{table} does not exist. Please provide a valid feature set")
    fset: schemas.FeatureSet = fset[0]
    pipeline = crud.get_pipeline(db, fset.feature_set_id)
    source = crud.get_pipeline_source(db, pipeline.source_id)
    feature_aggs: List[schemas.FeatureAggregation] = crud.get_feature_aggregations(db, fset.feature_set_id)
    if not feature_aggs:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Cannot find any aggregations for the feature set {schema}.{table}.")
    return generate_backfill_sql(schema, table, source, feature_aggs)

@SYNC_ROUTER.get('/backfill-intervals', status_code=status.HTTP_200_OK, response_model=List[datetime],
                 description="Get backfill intervals for parameterized backfill SQL", operation_id='get_backfill_intervals',
                 tags=['Source', 'Pipeline', 'Backfill', 'SQL'])
@managed_transaction
def get_backfill_intevals(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Get backfill intervals for parameterized backfill SQL for a particular feature set.
    """
    fset: List[schemas.FeatureSet] = _get_feature_sets(names=[f'{schema}.{table}'], db=db)
    if not fset:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Feature Set {schema}.{table} does not exist. Please provide a valid feature set")
    fset: schemas.FeatureSet = fset[0]
    pipeline = crud.get_pipeline(db, fset.feature_set_id)
    return generate_backfill_intervals(db, pipeline)


@SYNC_ROUTER.post('/agg-feature-set-from-source', status_code=status.HTTP_201_CREATED,
                  description="Creates an aggregation feature set from a Source",
                  operation_id='create_agg_feature_set_from_source', tags=['Feature_Set', 'Source', 'Pipeline'])
@managed_transaction
def create_agg_feature_set_from_source(sf: schemas.SourceFeatureSetAgg, run_backfill: Optional[bool] = False,
                                       db: Session = Depends(crud.get_db)):
    """
    Creates a temporal aggregation feature set by creating a pipeline linking a source to a feature set. Provided
    aggregations will generate the features for the feature set. If the feature set already exists, the feature names
    must match the generated feature names. Otherwise, this will create the feature set along with aggregation
    calculations to create features. Optionally runs the backfill at deploy time.
    """
    source: schemas.Source = _get_source(sf.source_name, db)

    crud.validate_feature_aggregations(db, source, sf.aggregations)

    source_pk_types = crud.get_source_pk_types(db, source)

    fsetc = schemas.FeatureSetCreate(
        schema_name=sf.schema_name,
        table_name=sf.table_name,
        description=sf.description,
        primary_keys=source_pk_types,
    )
    fset = _create_feature_set(fsetc, db)

    crud.create_pipeline(db, sf, fset.feature_set_id, source.source_id, 'FIXME')
    # Create feature aggregations and features
    # This registers the features and pipeline aggregations in the feature store
    create_pipeline_entities(db, sf, source, fset.feature_set_id)
    # Now that the features exist we can deploy the feature set
    _deploy_feature_set(sf.schema_name, sf.table_name, db)
    if run_backfill:
        sql = generate_backfill_sql(sf.schema_name, sf.table_name, source, crud.get_feature_aggregations(db, fset.feature_set_id))
        # TODO RUN backfill with Airflow
    #TODO: RUN incremental pipeline on schedule with Airflow
    return fset


@SYNC_ROUTER.get('/pipeline-sql', status_code=status.HTTP_200_OK, response_model=str,
                 description="Get incremental pipeline extract SQL for a feature set/source. This SQL is used to generate"
                             "the incremental set of data from the source which the pipeline will add to both the history"
                             "table and the serving table. This does NOT update the history and serving tables. That is done"
                             "by an external job.", operation_id='get_pipeline_sql',
                 tags=['Source', 'Pipeline', 'SQL'])
@managed_transaction
def get_pipeline_sql(schema: str, table: str, db: Session = Depends(crud.get_db)):
    """
    Generates the incremental Pipeline SQL for a feature set and source
    """

    fset: List[schemas.FeatureSet] = _get_feature_sets(names=[f'{schema}.{table}'], db=db)
    if not fset:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Feature Set {schema}.{table} does not exist. Please provide a valid feature set")
    fset: schemas.FeatureSet = fset[0]
    pipeline: schemas.Pipeline = crud.get_pipeline(db, fset.feature_set_id)
    if not pipeline:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Cannot find any pipelines for the feature set {schema}.{table}.")
    feature_aggs: List[schemas.FeatureAggregation] = crud.get_feature_aggregations(db, fset.feature_set_id)
    if not feature_aggs:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Cannot find any feature aggregations for the feature set {schema}.{table}.")
    source = crud.get_pipeline_source(db, pipeline.source_id)
    return generate_pipeline_sql(db, source, pipeline, feature_aggs)





