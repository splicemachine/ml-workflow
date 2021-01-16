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
    crud.register_metadata(db, fset)
    return fset
