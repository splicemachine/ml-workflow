from typing import List

from requests.auth import HTTPBasicAuth

from shared.logger.logging_config import logger
from shared.models.feature_store_models import Feature, FeatureSet
from shared.db.functions import DatabaseFunctions

from ...rest_api import crud
from ..fixtures.conftest import APP, get_my_session, override_get_db, test_app
from ..fixtures.feature import (create_deployed_fset, create_undeployed_fset,
                                test_session_create)
from ..fixtures.feature_set import create_schema

APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')


good_feature_set = {
    "schema_name": 'test_fs',
    "table_name": 'good_table_name',
    "description": 'a feature set that should be created',
    "primary_keys": {'ID': {'data_type':"INTEGER"}}
}

good_feature = {
    "name": 'good_feature',
    "description": 'a feature that should succeed because there is a feature set',
    "feature_data_type": {'data_type':'VARCHAR','length':250},
    "feature_type": 'C',
    "tags": ['test','tag']
}

good_feature2 = {
    "name": 'good_feature_2',
    "description": 'a feature that should succeed because there is a feature set',
    "feature_data_type": {'data_type':'DECIMAL','precision':10, 'scale':2},
    "feature_type": 'C',
    "tags": ['test','tag']
}

def test_deploy_feature_set_no_auth(test_app, create_schema):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema)

    response = test_app.post('/feature-sets', json=good_feature_set)
    assert response.status_code == 401, 'Should fail because there is no authentication'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes

def test_deploy_no_feature_set(test_app, create_schema):
    """
    Tests deploying a feature set that doesn't exist
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/deploy-feature-sets', params={'schema': 'fake_schema', 'table': 'missingtable'}, auth=basic_auth)
    assert response.status_code in range(400,500), "Should not throw internal server error, should fail well"
    logger.info(f'status: {response.status_code}, -- message: {response.json()}')


def test_deploy_feature_set_no_features(test_app, create_schema):
    """
    Tests deploying a feature set without any features
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=good_feature_set, auth=basic_auth)
    assert response.status_code == 201, "this shouldn't be an issue"
    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    response = test_app.post('/deploy-feature-sets', auth=basic_auth,
                             params={'schema': good_feature_set['schema_name'],
                                     'table':  good_feature_set['table_name']
                                     }
                             )

    assert response.status_code in range(400,500), 'Should fail because the feature set has no features'

def test_deploy_good_feature_set(test_app, create_schema):
    """
    Tests deploying a good feature set (base case)
    """
    sess = create_schema
    APP.dependency_overrides[crud.get_db] = lambda: (yield sess) # Give the "server" the same db session

    schema = good_feature_set['schema_name']
    table = good_feature_set['table_name']

    # Create feature set
    response = test_app.post('/feature-sets', json=good_feature_set, auth=basic_auth)
    logger.info(f'status: {response.status_code}, -- message: {response.json()}')
    assert response.status_code == 201, "this shouldn't be an issue"
    fsets = sess.query(FeatureSet).all()
    assert len(fsets) == 1, f'Feature set should have been created. There are {len(fsets)} feature sets'
    logger.info(str(fsets[0].__dict__))

    # Create feature
    response = test_app.post('/features', json=good_feature, auth=basic_auth,
                             params={
                                 'schema': schema,
                                 'table':  table
                             })
    logger.info(f'status: {response.status_code}, -- message: {response.json()}')
    assert response.status_code == 201, "this shouldn't be an issue"
    feats = sess.query(Feature).all()
    assert len(feats) == 1, f'Feature should have been created. There are {len(feats)} features'

    # Create feature
    response = test_app.post('/features', json=good_feature2, auth=basic_auth,
                             params={
                                 'schema': schema,
                                 'table':  table
                             })
    logger.info(f'status: {response.status_code}, -- message: {response.json()}')
    assert response.status_code == 201, "this shouldn't be an issue"
    feats = sess.query(Feature).all()
    assert len(feats) == 2, f'Feature should have been created. There are {len(feats)} features'

    logger.info(str(feats[0].__dict__))


    response = test_app.post('/deploy-feature-set', auth=basic_auth, json=None,
                             params={'schema': schema,
                                     'table':  table
                                     }
                             )
    logger.info(f'status: {response.status_code}, -- message: {response.json()}')
    assert response.status_code == 200, 'Should deploy'

    assert DatabaseFunctions.table_exists(schema, table, sess.get_bind()), 'Table should exist but does not!'
