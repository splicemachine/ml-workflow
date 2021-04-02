from typing import List

from requests.auth import HTTPBasicAuth
from shared.logger.logging_config import logger
from shared.models.feature_store_models import Feature, FeatureSet
from ..fixtures.conftest import get_my_session, test_app, override_get_db, APP
from ..fixtures.feature import test_session_create, create_deployed_fset, create_undeployed_fset
from ...rest_api import crud

APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')

bad_feature = {
    "name": 'feature_without_feature_set',
    "description": 'a feature that should fail because there is no feature set',
    "feature_data_type": {'data_type': 'VARCHAR','length':250},#'VARCHAR(250)',
    "feature_type": 'C',
    "tags": None
}

good_feature = {
    "name": 'good_feature',
    "description": 'a feature that should succeed because there is a feature set',
    "feature_data_type": {'data_type': 'VARCHAR','length':250},#'VARCHAR(250)',
    "feature_type": 'C',
    "tags": None
}

def test_create_feature_no_auth(test_app):
    response = test_app.post('/features',params={'schema': 'fake_schema', 'table': 'badtable'}, json=bad_feature)
    assert response.status_code == 401, 'Should fail because there is no authentication'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes

def test_create_feature_no_feature_set(test_app, create_undeployed_fset):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset) # Give the "server" the same db session

    response = test_app.post('/features',params={'schema': 'fake_schema', 'table': 'badtable'},
                             json=bad_feature, auth=basic_auth)

    assert response.status_code == 404, 'Should fail because there is no feature set with provided name'
    mes = response.json()['message']
    assert 'Feature Set ' in mes and 'does not exist' in mes, mes


def test_create_feature_existing_feature_set(test_app, create_undeployed_fset):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset) # Give the "server" the same db session
    logger.info("============================= Starting test_create_feature_existing_feature_set =============================")

    response = test_app.post('/features',params={'schema': 'test_fs', 'table': 'FSET_1'},
                             json=good_feature, auth=basic_auth)

    assert response.status_code == 201, response.json()['message']

    # Assert feature exists
    fs: List[Feature] = create_undeployed_fset.query(Feature).all()
    assert len(fs) == 1, f'Only one feature should exist, but {len(fs)} do'
    assert fs[0].name == 'good_feature'

def test_create_feature_deployed_feature_set_upper_case(test_app, create_deployed_fset):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_deployed_fset) # Give the "server" the same db session
    logger.info("============================= Starting test_create_feature_existing_feature_set =============================")

    response = test_app.post('/features',params={'schema': 'test_fs', 'table': 'FSET_1'}, # Upper case schema/table
                             json=good_feature, auth=basic_auth)

    assert response.status_code == 409, f'Should fail because the Feature Set is already deployed. ' \
                                        f'Status Code: {response.status_code}, response: {response.json()}'


def test_create_feature_deployed_feature_set_lower_case(test_app, create_deployed_fset):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_deployed_fset) # Give the "server" the same db session
    logger.info("============================= Starting test_create_feature_existing_feature_set =============================")

    response = test_app.post('/features',params={'schema': 'test_fs', 'table': 'FSET_1'}, # Lower case schema/table
                             json=good_feature, auth=basic_auth)

    assert response.status_code == 409, response.json()['message']

def test_db_setup(test_session_create):
    logger.info("============================= Starting test_db_setup =============================")
    sess = test_session_create
    assert len(sess.query(FeatureSet).all())==1
    assert not sess.query(FeatureSet).one().deployed
