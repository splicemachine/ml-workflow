from typing import List

from requests.auth import HTTPBasicAuth
from shared.logger.logging_config import logger
from shared.models.feature_store_models import Feature, FeatureSet, FeatureSetKey
from shared.services.database import DatabaseFunctions

from ..fixtures.conftest import get_my_session, test_app, override_get_db, APP
from ..fixtures.feature import test_session_create, create_deployed_fset, create_undeployed_fset
from ..fixtures.feature_set import create_schema, create_fset_with_features, create_undeployed_fset
from ..fixtures.training_set import create_training_set, create_deployment
from shared.models.feature_store_models import TrainingView, TrainingViewKey, TrainingSet, TrainingSetFeature
from ...rest_api import crud

APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')


name = {'name':'test_vw'}

ROUTE = '/training-views'

def test_delete_training_view_no_auth(test_app, create_undeployed_fset):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_undeployed_fset)

    response = test_app.delete(ROUTE, params=name)
    assert response.status_code == 401, 'Should fail because there is no authentication'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes

def test_delete_training_view(test_app, create_training_set):
    """
    Tests deleting a training view
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_training_set) # Give the "server" the same db session

    assert len(create_training_set.query(TrainingView).all()) == 1, 'should have 1 training view'
    assert len(create_training_set.query(TrainingViewKey).all()) == 2, 'shoudl have 2 training view keys'
    assert len(create_training_set.query(TrainingSet).all()) == 1, 'should have 1 training set'
    assert len(create_training_set.query(TrainingSetFeature).all()) == 1, 'should have 1 training set feature'

    response = test_app.delete(ROUTE, params=name, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 200, f'Should delete training view, but got error {response.json()}'
    assert len(create_training_set.query(TrainingView).all()) == 0, 'TrainingView should be removed'
    assert len(create_training_set.query(TrainingViewKey).all()) == 0, 'TrainingViewKey should be removed'
    assert len(create_training_set.query(TrainingSet).all()) == 0, 'TrainingSet should be removed'
    assert len(create_training_set.query(TrainingSetFeature).all()) == 0, 'TrainingSetFeature should be removed'

    assert len(create_training_set.query(Feature).all()) == 2, 'Feature should NOT be removed'
    assert len(create_training_set.query(FeatureSetKey).all()) == 2, 'Feature Set Key should NOT be removed'
    assert len(create_training_set.query(FeatureSet).all()) == 2, 'Feature Set should NOT be removed'

def test_delete_training_view_with_deployment(test_app, create_deployment):
    """
    Tests deleting a training view where a model has been deployed
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_deployment) # Give the "server" the same db session

    assert len(create_deployment.query(TrainingView).all()) == 1, 'setup'
    assert len(create_deployment.query(TrainingViewKey).all()) == 2, 'setup'
    assert len(create_deployment.query(TrainingSet).all()) == 1, 'setup'
    assert len(create_deployment.query(TrainingSetFeature).all()) == 1, 'setup'


    response = test_app.delete(ROUTE, params=name, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert len(create_deployment.query(TrainingView).all()) == 1, 'should still be there'
    assert len(create_deployment.query(TrainingViewKey).all()) == 2, 'should still be there'
    assert len(create_deployment.query(TrainingSet).all()) == 1, 'should still be there'
    assert len(create_deployment.query(TrainingSetFeature).all()) == 1, 'should still be there'

    # postgres has case sensitivity issues that seem incompatible with Splice for testing
    # assert not DatabaseFunctions.table_exists('test_fs', '"FSET_1"', create_fset_with_features.get_bind()), \
    #     'Table should be dropped!'

