from typing import List

from requests.auth import HTTPBasicAuth
from shared.logger.logging_config import logger
from shared.models.feature_store_models import Feature, FeatureSet, FeatureSetKey
from shared.services.database import DatabaseFunctions

from ..fixtures.conftest import get_my_session, test_app, override_get_db, APP
from ..fixtures.feature import test_session_create, create_deployed_fset, create_undeployed_fset
from ..fixtures.feature_set import create_schema
from ..fixtures.training_set import create_training_set
from shared.models.feature_store_models import TrainingView, TrainingViewKey, TrainingSet, TrainingSetFeature
from ...rest_api import crud

APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')

no_purge = {'schema':'TEST_FS', 'table': 'FSET_1', 'purge':False}
purge = {'schema':'TEST_FS', 'table': 'FSET_1', 'purge':True}

def test_delete_feature_set_no_auth(test_app, create_undeployed_fset):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset)

    response = test_app.post('/feature-sets', json=no_purge)
    assert response.status_code == 401, 'Should fail because there is no authentication'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes

def test_delete_feature_set(test_app, create_undeployed_fset):
    """
    Tests creating a feature set with an emtpy string as the table name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset) # Give the "server" the same db session

    assert len(create_undeployed_fset.query(FeatureSetKey).all()) == 1, 'setup'
    assert len(create_undeployed_fset.query(FeatureSet).all()) == 1, 'setup'

    response = test_app.post('/feature-sets', json=no_purge, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 200, 'Should delete feature set'
    assert len(create_undeployed_fset.query(FeatureSetKey).all()) == 0, 'Feature Set Key should be removed'
    assert len(create_undeployed_fset.query(FeatureSet).all()) == 0, 'Feature Set should be removed'

def test_delete_deployed_feature_set(test_app, create_fset_with_features):
    """
    Tests creating a feature set with an emtpy string as the schema name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_fset_with_features) # Give the "server" the same db session

    assert len(create_fset_with_features.query(Feature).all()) == 2, 'setup'
    assert len(create_fset_with_features.query(FeatureSetKey).all()) == 2, 'setup'
    assert len(create_fset_with_features.query(FeatureSet).all()) == 2, 'setup'

    assert DatabaseFunctions.table_exists('TEST_FS', 'FSET_1', create_fset_with_features.get_bind()), 'Table should exist but does not!'

    response = test_app.post('/feature-sets', json=no_purge, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 200, 'Should delete feature set'
    assert len(create_fset_with_features.query(Feature).all()) == 1, 'Feature should be removed'
    assert len(create_fset_with_features.query(FeatureSetKey).all()) == 1, 'Feature Set Key should be removed'
    assert len(create_fset_with_features.query(FeatureSet).all()) == 1, 'Feature Set should be removed'

    assert not DatabaseFunctions.table_exists('TEST_FS', 'FSET_1', create_fset_with_features.get_bind()), \
        'Table should be dropped!'

def test_delete_feature_set_with_training_set(test_app, create_training_set):
    """
    Tests creating a feature set with an invalid table name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_training_set) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=no_purge, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the purge was false'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_delete_feature_set_with_training_set_purge(test_app, create_training_set):
    """
    Tests creating a feature set with an invalid schema name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_training_set) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=purge, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 200, 'Should delete everything because purge was True'

    assert len(create_training_set.query(TrainingSetFeature).all()) == 0
    assert len(create_training_set.query(TrainingSet).all()) == 0
    assert len(create_training_set.query(TrainingView).all()) == 0
    assert len(create_training_set.query(TrainingViewKey).all()) == 0


# def test_delete_feature_set_with_model(test_app, create_schema):
#     """
#     Tests creating a feature set without a primary key
#     """
#     APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session
#
#     response = test_app.post('/feature-sets', json=feature_set_null_pk, auth=basic_auth)
#
#     logger.info(f'status: {response.status_code}, -- message: {response.json()}')
#
#     assert response.status_code in range(400,500), 'Should fail because the feature set has no primary key'
#     c = response.json()['code']
#     assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'
#
# def test_delete_feature_set_with_model_purge(test_app, create_schema):
#     """
#     Tests creating a feature set with a primary key that has an invalid datatype (not a splice supported type)
#     """
#     APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session
#
#     response = test_app.post('/feature-sets', json=feature_set_bad_pk_bad_datatype, auth=basic_auth)
#
#     logger.info(f'status: {response.status_code}, -- message: {response.json()}')
#
#     assert response.status_code in range(400,500), 'Should fail because the feature set has a bad pk datatype'
#     c = response.json()['code']
#     assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

