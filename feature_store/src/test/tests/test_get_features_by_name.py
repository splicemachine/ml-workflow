from typing import List

from requests.auth import HTTPBasicAuth

from shared.logger.logging_config import logger

from ...rest_api import crud
from ..fixtures.conftest import APP, get_my_session, override_get_db, test_app
from ..fixtures.feature_set import create_fset_with_features

APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')

def test_get_feature_no_auth(test_app):
    response = test_app.get('/features',params={"name": ['name','income']})
    assert response.status_code == 401, 'Should fail because there is no authentication'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes


def test_get_feature_no_features(test_app, create_fset_with_features):
    """
    Test get_features_by_name and not passing in any feature names
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_fset_with_features) # Give the "server" the same db session
    response = test_app.get('/features',params={"name": []}, auth=basic_auth)
    assert response.status_code in range(400,500), 'Should fail because no names were provided'
    mes = response.json()['message']
    assert mes == 'Please provide at least one name', mes

def test_get_feature_bad_features(test_app, create_fset_with_features):
    """
    Test get_features_by_name and passing in a name that doesn't have a corresponding feature
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_fset_with_features) # Give the "server" the same db session
    response = test_app.get('/features',params={"name": ["name","not_a_feature"]}, auth=basic_auth)
    assert response.status_code in range(400,500), 'Should fail because a feature that does not exist was requested'
    mes = response.json()['message']
    logger.info('MESSAGE for bad_features')
    logger.info(mes)


def test_get_features(test_app, create_fset_with_features):
    """
    Test get_features_by_name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_fset_with_features) # Give the "server" the same db session
    response = test_app.get('/features',params={"name": ["name","income"]}, auth=basic_auth)

    assert response.status_code == 200, 'succeed'
    mes = response.json()
    logger.info('MESSAGE for get_features')
    logger.info(mes)
    assert len(mes) == 2, "Should return 2 features because 2 were requested"

