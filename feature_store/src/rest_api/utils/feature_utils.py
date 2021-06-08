from .. import crud
from .. import schemas
import shared.models.feature_store_models as models
from sqlalchemy.orm import Session
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.services.database import DatabaseFunctions
from fastapi import status
from shared.logger.logging_config import logger
from ..utils.airflow_utils import Airflow
from typing import List, Set, Union
import json
from .utils import sql_to_datatype, __validate_primary_keys, __get_table_name

def _deploy_feature_set(schema: str, table: str, version: Union[str, int], migrate: bool, db: Session):
    """
    Deploys a feature set to the database. This persists the feature stores existence.
    As of now, once deployed you cannot delete the feature set or add/delete features.
    The feature set must have already been created with :py:meth:`~features.FeatureStore.create_feature_set`
    """
    try:
        fset = crud.get_feature_sets(db, _filter={'table_name': table, 'schema_name': schema, 'feature_set_version': version})[0]
    except:
        version_text = '' if version == 'latest' else f' with version {version}'
        raise SpliceMachineException(
            status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
            message=f"Cannot find feature set {schema}.{table}{version_text}. Ensure you've created this "
            f"feature set using fs.create_feature_set before deploying.")
    if fset.deployed:
        raise SpliceMachineException(
            status_code=status.HTTP_409_CONFLICT, code=ExceptionCodes.ALREADY_DEPLOYED,
            message=f"Feature set {schema}.{table} v{fset.feature_set_version} is already deployed.")

    features = crud.get_features(db, fset)
    if not features:
        raise SpliceMachineException(
            status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
            message=f"Feature set {schema}.{table} has no features. You cannot deploy a feature "
                    f"set with no features")

    fset = crud.deploy_feature_set(db, fset)
    if migrate:
        current = crud.get_feature_sets(db, feature_set_names=[f'{schema}.{table}'])[0]
        if current.deployed:
            crud.migrate_feature_set(db, current, fset)
        else:
            logger.warn('No previous data found to migrate - skipping migration')
    logger.info('Creating Historian Triggers...')
    crud.create_historian_triggers(db, fset)
    logger.info('Done.')
    if Airflow.is_active:
        Airflow.schedule_feature_set_calculation(f'{schema}.{__get_table_name(fset)}')
    return fset

def _create_feature_set(fset: schemas.FeatureSetCreate, db: Session):
    """
    The implementation of the create_feature_set route with logic here so other functions can call it
    :param fset: The feature set schema to create
    :param db: The database session
    :return: The created Feature Set
    """
    crud.validate_feature_set(db, fset)
    logger.info(f'Registering feature set {fset.schema_name}.{fset.table_name} in Feature Store')
    fset_metadata = crud.register_feature_set_metadata(db, fset)
    fset_version = crud.create_feature_set_version(db, fset_metadata, True)
    fset_version.__dict__.pop('feature_set_id')
    created_fset = schemas.FeatureSetDetail(**fset_metadata.__dict__, **fset_version.__dict__)

    if fset.features:
        logger.info("Validating features")
        for fc in fset.features:
            crud.validate_feature(db, fc.name, fc.feature_data_type)
            fc.feature_set_id = created_fset.feature_set_id
        logger.info("Done. Bulk registering features")
        crud.bulk_register_feature_metadata(db, fset.features)
        crud.bulk_register_feature_versions(db, fset.features, created_fset.feature_set_id)
    return created_fset

def _update_feature_set(update: schemas.FeatureSetUpdate, schema: str, table: str, db: Session):
    """
    The implementation of the create_feature_set route with logic here so other functions can call it
    :param fset: The feature set schema to create
    :param db: The database session
    :return: The created Feature Set
    """
    __validate_primary_keys(update.primary_keys)

    fsets: List[schemas.FeatureSetDetail] = crud.get_feature_sets(db, _filter={'table_name': table, 'schema_name': schema, 'feature_set_version': 'latest'})
    if not fsets:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature Set {schema}.{table} does not exist. Please enter "
                                        f"a valid feature set, or create this feature set using fs.create_feature_set()")
    fset = fsets[0]

    if update.description:
        logger.info(f'Updating description for {fset.schema_name}.{fset.table_name}')
        crud.update_feature_set_description(db, fset.feature_set_id, update.description)
        fset.description = update.description

    fset_version = crud.create_feature_set_version(db, fset, fset.feature_set_version + 1)
    fset.__dict__.update(fset_version.__dict__)

    if update.features:
        logger.info("Validating features")
        to_create = []
        for fc in update.features:
            try:
                crud.validate_feature(db, fc.name, fc.feature_data_type)
                fc.feature_set_id = fset.feature_set_id
            except SpliceMachineException as e:
                if e.code != ExceptionCodes.ALREADY_EXISTS:
                    raise e
            else:
                to_create.append(fc)
        logger.info("Done. Bulk registering features")
        crud.bulk_register_feature_metadata(db, to_create)
        crud.bulk_register_feature_versions(db, update.features, fset.feature_set_id, fset.feature_set_version)

    return fset

def _alter_feature_set(alter: schemas.FeatureSetAlter, schema: str, table: str, version: Union[str, int], db: Session):
    """
    The implementation of the update_feature_set route with logic here so other functions can call it
    :param alter: The feature set schema to update
    :param schema: The schema name
    :param table: The table name
    :param db: The database session
    :return: The updated Feature Set
    """
    fsets: List[schemas.FeatureSetDetail] = crud.get_feature_sets(db, _filter={'table_name': table, 'schema_name': schema, 'feature_set_version': version})
    if not fsets:
        v = f'with version {version} ' if isinstance(version, int) else ''
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Feature Set {schema}.{table} {v}does not exist. Please enter "
                                        f"a valid feature set.")
    fset = fsets[0]

    if alter.primary_keys:
        if fset.deployed:
            raise SpliceMachineException(status_code=status.HTTP_406_NOT_ACCEPTABLE, code=ExceptionCodes.ALREADY_DEPLOYED,
                                        message=f"Cannot update primary keys of a deployed feature set. "
                                        "If you wish to change the primary keys, please create a new version of the feature set")
        __validate_primary_keys(alter.primary_keys)
        logger.info(f'Updating keys for {fset.schema_name}.{fset.table_name}')
        alter.feature_set_id = fset.feature_set_id
        crud.update_feature_set_keys(db, alter, fset.feature_set_version)
        fset.primary_keys = alter.primary_keys
    
    if alter.description:
        logger.info(f'Updating description for {fset.schema_name}.{fset.table_name}')
        crud.update_feature_set_description(db, fset.id, alter.description)
        fset.description = alter.description

    return fset

def _get_feature_sets(names: List[str], db: Session) -> List[schemas.FeatureSet]:
    """
    Logic implementation of get_feature_sets route implemented here so other
    functions can call it directly

    :param names: The names of the desired feature sets
    :param db: Session
    :return: List of Feature Sets
    """
    crud.validate_schema_table(names)
    return crud.get_feature_sets(db, feature_set_names=names)

def model_to_schema_feature(feat: models.Feature) -> schemas.FeatureDetail:
    """
    A function that converts a models.Feature into a schemas.Feature through simple manipulations.
    Splice Machine does not support complex data types like JSON or Arrays, so we stringify them and store them as
    Strings in the database, so they need some manipulation when we retrieve them.
        * Turns tags into a list
        * Turns attributes into a Dict
        * Turns feature_data_type into a DataType object (dict)
    :param feat: The feature from the database
    :return: The schemas.Feature representation
    """
    f = feat.__dict__
    f['tags'] = f['tags'].split(',') if f.get('tags') else None
    f['attributes'] = json.loads(f['attributes']) if f.get('attributes') else None
    f['feature_data_type'] = sql_to_datatype(f['feature_data_type'])
    return schemas.FeatureDetail(**f)


def delete_feature_set(db: Session, feature_set_id: int, version: int = None, cascade: bool = False,
                       training_sets: Set[int] = None):
    """
    Deletes a Feature Set. Drops the table. Removes keys. Potentially removes training sets if there are dependencies

    :param db: Database Session
    :param feature_set: feature set to delete
    :param training_sets: Set[int] training sets
    :param cascade: whether to delete dependent training sets. If this is True training_sets must be set.
    """
    if cascade and training_sets:
        logger.info(f'linked training sets: {training_sets}')

        logger.info("Removing training set stats")
        crud.delete_training_set_stats(db, set(training_sets))

        logger.info("Removing training set instances (versions)")
        crud.delete_training_set_instances(db, set(training_sets))
        # Delete training set features if any
        logger.info("Removing training set features")
        crud.delete_training_set_features(db, training_sets)

        # Delete training sets
        logger.info("Removing training sets")
        crud.delete_training_sets(db, training_sets)

    # Delete features
    logger.info("Removing features")
    crud.delete_features_from_feature_set(db, feature_set_id, version)
    # Delete Feature Set Keys
    logger.info("Removing feature set keys")
    crud.delete_feature_set_keys(db, feature_set_id, version)
    # Delete Feature Set Version
    logger.info("Removing feature set version")
    crud.delete_feature_set_version(db, feature_set_id, version)

    # Delete pipeline dependencies
    logger.info("Deleting any Pipeline dependencies")
    crud.delete_pipeline(db, feature_set_id)


def drop_feature_set_table(db: Session, schema_name: str, table_name: str):
    logger.info("Dropping table")
    DatabaseFunctions.drop_table_if_exists(schema_name, table_name, db.get_bind())
    logger.info("Dropping history table")
    DatabaseFunctions.drop_table_if_exists(schema_name, f'{table_name}_history', db.get_bind())

