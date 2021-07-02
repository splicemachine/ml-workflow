from typing import List

from requests.auth import HTTPBasicAuth

from shared.logger.logging_config import logger
from shared.models.feature_store_models import Feature, FeatureSet

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
    "feature_data_type": {'data_type':'VARCHAR', 'length':250},
    "feature_type": 'C',
    "tags": None
}

def test_get_feature_sets_no_auth(test_app, create_undeployed_fset):
    """
    Get a feature set without being authenticated
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset)

    # Get feature sets with no auth
    response = test_app.get('/feature-sets', json={'fsid': [1]})
    assert response.status_code in range(400,500), 'Should fail because unauthenticated'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes



def test_get_good_feature_set(test_app, create_undeployed_fset):
    """
    Tests the successful request of a good feature set
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset) # Give the "server" the same db session

    response = test_app.get('/feature-sets', json={'fsid':[1]}, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 200, 'Should succeed'
    r = response.json()
    logger.info("RESPONSE FROM FSET get")
    logger.info(r)
    for fset in r:
        assert 'schema_name' in fset, f'response should contain created feature set, but had {fset}'
    
def test_get_none_feature_set(test_app, create_undeployed_fset):
    """
    Test request of feature sets without passing in any IDs
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset) # Give the "server" the same db session

    response = test_app.get('/feature-sets', json={'fsid':None}, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 200, 'Should succeed'
    r = response.json()
    logger.info("RESPONSE FROM FSET get")
    logger.info(r)
    for fset in r:
        assert 'schema_name' in fset, f'response should contain created feature set, but had {fset}'
    
def test_get_missing_feature_set(test_app, create_undeployed_fset):
    """
    Test request of feature sets that don't exist
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset) # Give the "server" the same db session

    response = test_app.get('/feature-sets', json={'fsid':[1,4]}, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in (400,500), "Should fail because we've requested a feature that doesn't exist"
    r = response.json()
    logger.info("RESPONSE FROM FSET get")
    logger.info(r)
    # assert 'schema_name' in r, f'response should contain created feature set, but had {r}'
