from fastapi import APIRouter, status, Depends
from typing import List, Dict, Optional, Union
from shared.logger.logging_config import logger
from shared.models.feature_store_models import FeatureSet, TrainingView, Feature
from sqlalchemy.orm import Session
from ..constants import SQL
# Synchronous API Router-- we can mount it to the main API
SYNC_ROUTER = APIRouter()



@SYNC_ROUTER.post('/feature-sets', response_model=schemas.FeatureSet, status_code=status.HTTP_200_OK,
                description="Creates and returns a new feature set", operation_id='create_feature_set')
def create_feature_set(fset: schemas.FeatureSetCreate, db: Session = Depends(crud.get_db)):
    """
    Creates and returns a new feature set

    :param fset: The feature set object to be created
    :return: FeatureSet
    """
    logger.info("HERE")
    print(fset)
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
        raise SpliceMachineException(f"The datatype you've passed in, {feature_data_type} is not a valid SQL type. "
                                     f"Valid types are {SQL_TYPES}")

@SYNC_ROUTER.post('/features', response_model=schemas.Feature, status_code=status.HTTP_200_OK,
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
