import requests
from fastapi.testclient import TestClient
from requests.auth import HTTPBasicAuth
from shared.logger.logging_config import logger
from shared.models.feature_store_models import (Feature, FeatureSet, FeatureSetKey, TrainingView,
                                                TrainingViewKey, TrainingSet, TrainingSetFeature,
                                                TrainingSetFeatureStats, Deployment, DeploymentHistory,
                                                DeploymentFeatureStats)

from ..rest_api import crud
from ..rest_api.main import APP
from .conftest import override_get_db, feature_create_1, test_app


APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')


# def test_create_feature_no_feature_set(test_app):
#     d = { "name": 'feature_without_feature_set',
#           "description": 'a feature that should fail because there is no feature set',
#           "feature_data_type": 'VARCHAR(250)',
#           "feature_type": 'C',
#           "tags": None
#           }
#     response = test_app.post('/features',params={'schema': 'fake_schema', 'table': 'badtable'}, json=d)
#     logger.info(response.status_code)
#     logger.info(str(response.text))
#     assert response.status_code != 200, 'Should fail because there is no feature set with provided name'
#     mes = response.json()['message']
#     assert 'Feature Set ' in mes and 'does not exist' in mes, mes
#
#
def test_create_feature_existing_feature_set(test_app, feature_create_1):
    sess = feature_create_1
    assert len(sess.query(FeatureSet).all())==1

    d = { "name": 'good_feature',
          "description": 'a feature that should succeed because there is a feature set',
          "feature_data_type": 'VARCHAR(250)',
          "feature_type": 'C',
          "tags": None
          }
    response = test_app.post('/features',params={'schema': 'test_fs', 'table': 'fset_1'}, json=d, auth=basic_auth)
    logger.info(response.status_code)
    logger.info(str(response.text))
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as error:
        to_print = str(error) if error.response.status_code == 500 else error.response.json()["message"]
        logger.error(to_print)
    assert response.status_code == 201, response.json()['message']

def test_db_setup(feature_create_1):
    sess = feature_create_1
    assert len(sess.query(FeatureSet).all())==1
    assert not sess.query(FeatureSet).one().deployed
