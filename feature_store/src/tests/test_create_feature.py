from fastapi.testclient import TestClient
from shared.logger.logging_config import logger

from ..rest_api.crud import get_db
from ..rest_api.main import APP
from .conftest import *


APP.dependency_overrides[get_db] = override_get_db



def test_create_feature_no_feature_set(test_app):
    d = { "name": 'feature_without_feature_set',
          "description": 'a feature that should fail because there is no feature set',
          "feature_data_type": 'VARCHAR(250)',
          "feature_type": 'C',
          "tags": None
          }
    response = test_app.post('/features',params={'schema': 'fake_schema', 'table': 'badtable'}, json=d)
    logger.info(response.status_code)
    logger.info(str(response.text))
    assert response.status_code != 200, 'Should fail because there is no feature set with provided name'
    assert 'Feature Set ' in response.json() and 'does not exist' in response.json()
