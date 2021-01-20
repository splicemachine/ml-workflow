from fastapi import APIRouter, status, Depends, HTTPException, Query
from typing import List, Dict, Optional, Union
from shared.logger.logging_config import logger
from shared.models.feature_store_models import FeatureSet, TrainingView, Feature
from sqlalchemy.orm import Session
from ..constants import SQL
from .. import schemas, crud
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

@SYNC_ROUTER.get('/training-views', status_code=status.HTTP_200_OK, response_model=List[schemas.TrainingView],
                description="Returns a list of all available training views with an optional filter", operation_id='get_training_views')
async def get_training_views(name: Optional[str] = Query(None), db: Session = Depends(crud.get_db)):
    """
    Returns a list of all available training views with an optional filter

    :param _filter: Dictionary container the filter keyword (label, description etc) and the value to filter on
        If None, will return all TrainingViews
    :return: List[TrainingView]
    """
    if name:
        return crud.get_training_views(db, {'name': name})[0]
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
async def get_features_by_name(name: Optional[List[str]] = Query(None), as_list:Optional[bool] = Query(False), db: Session = Depends(crud.get_db)):
    """
    Returns a dataframe or list of features whose names are provided

    :param names: The list of feature names
    :param as_list: Whether or not to return a list of features. Default False
    :return: SparkDF or List[Feature] The list of Feature objects or Spark Dataframe of features and their metadata. Note, this is not the Feature
    values, simply the describing metadata about the features. To create a training dataset with Feature values, see
    :py:meth:`features.FeatureStore.get_training_set` or :py:meth:`features.FeatureStore.get_feature_dataset`
    """
    return crud.get_features_by_name(db, name, as_list)

@SYNC_ROUTER.delete('/feature-sets', status_code=status.HTTP_200_OK,
                description="Removes a feature set", operation_id='remove_feature_set')
async def remove_feature_set(db: Session = Depends(crud.get_db)):
    # TODO
    raise NotImplementedError

@SYNC_ROUTER.post('/feature-sets', response_model=schemas.FeatureSet, status_code=201,
                description="Creates and returns a new feature set", operation_id='create_feature_set')
def create_feature_set(fset: schemas.FeatureSetCreate, db: Session = Depends(crud.get_db)):
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
def create_feature(fc: schemas.FeatureCreate, schema_name: str, table_name: str, db: Session = Depends(crud.get_db)):
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

@SYNC_ROUTER.post('/deploy-feature-set', response_model=schemas.FeatureSet, status_code=status.HTTP_200_OK,
                description="Deploys a feature set to the database", operation_id='deploy_feature_set')

def deploy_feature_set(schema_name: str, table_name: str, db: Session = Depends(crud.get_db)):
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
