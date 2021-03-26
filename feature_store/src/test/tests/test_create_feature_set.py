from typing import List

from requests.auth import HTTPBasicAuth
from shared.logger.logging_config import logger
from shared.models.feature_store_models import Feature, FeatureSet
from ..fixtures.conftest import get_my_session, test_app, override_get_db, APP
from ..fixtures.feature import test_session_create, create_deployed_fset, create_undeployed_fset
from ..fixtures.feature_set import create_schema
from ...rest_api import crud

APP.dependency_overrides[crud.get_db] = override_get_db

basic_auth = HTTPBasicAuth('user','password')

feature_set_no_schema = {
    "schema_name": '',
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because there is no feature set table name',
    "primary_keys": {'ID': "INTEGER"}
}

feature_set_no_table = {
    "schema_name": 'good_table_name',
    "table_name": '',
    "description": 'a feature set that should fail because there is no feature set table name',
    "primary_keys": {'ID': "INTEGER"}
}

feature_set_invalid_table = {
    "schema_name": 'good_schema_name',
    "table_name": '%%stl24!',
    "description": 'a feature set that should fail because there is an invalid feature set table name',
    "primary_keys": {'ID': "INTEGER"}
}

feature_set_invalid_schema = {
    "schema_name": '%%stl24!',
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because there is an invalid feature set schema name',
    "primary_keys": {'ID': "INTEGER"}
}

feature_set_null_pk = {
    "schema_name": 'a_schema',
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because there is no primary key',
    "primary_keys": None
}

feature_set_bad_pk_bad_datatype = {
    "schema_name": 'a_schema',
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because the PK is invalid',
    "primary_keys": {'ID': "NOTADATATYPE"}
}

feature_set_bad_pk_bad_datatype_2 = {
    "schema_name": 'a_schema',
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because the the PK data type is invalid',
    "primary_keys": {'ID': "VARCHAR"} # Not valid because the varchar doesnt have a length
}

feature_set_bad_pk_bad_name = {
    "schema_name": 'a_schema',
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because the the PK name is invalid',
    "primary_keys": {'%%stl24!': "DOUBLE"}
}

feature_set_bad_pk_schema_location = {
    "schema_name": 'featurestore', # Cant create feature sets here
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because the schema is reserved (featurestore)',
    "primary_keys": {'%%stl24!': "DOUBLE"}
}

feature_set_bad_pk_schema_location_2 = {
    "schema_name": 'sys', # Cant create feature sets here
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because the schema is reserved (sys)',
    "primary_keys": {'%%stl24!': "DOUBLE"}
}

feature_set_bad_pk_schema_location_3 = {
    "schema_name": 'mlmanager', # Cant create feature sets here
    "table_name": 'good_table_name',
    "description": 'a feature set that should fail because the schema is reserved (mlmanager)',
    "primary_keys": {'%%stl24!': "DOUBLE"}
}

good_feature_set = {
    "schema_name": 'test_fs',
    "table_name": 'good_table_name',
    "description": 'a feature set that should be created',
    "primary_keys": {'ID': "INTEGER"}
}

def test_create_feature_set_no_auth(test_app, create_schema):
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema)

    response = test_app.post('/feature-sets', json=good_feature_set)
    assert response.status_code == 401, 'Should fail because there is no authentication'
    mes = response.json()['message']
    assert mes == 'Not authenticated', mes

def test_create_feature_set_missing_table_name(test_app, create_schema):
    """
    Tests creating a feature set with an emtpy string as the table name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_no_table, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set has no table name'

def test_create_feature_set_missing_schema_name(test_app, create_schema):
    """
    Tests creating a feature set with an emtpy string as the schema name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_no_schema, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set has no schema name'

def test_create_feature_set_invalid_table_name(test_app, create_schema):
    """
    Tests creating a feature set with an invalid table name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_invalid_table, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set has an invalid table name'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_create_feature_set_invalid_schema_name(test_app, create_schema):
    """
    Tests creating a feature set with an invalid schema name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_invalid_schema, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set has an invalid schema name'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'


def test_create_feature_set_null_pk(test_app, create_schema):
    """
    Tests creating a feature set without a primary key
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_null_pk, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set has no primary key'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_create_feature_set_invalid_pk_datatype(test_app, create_schema):
    """
    Tests creating a feature set with a primary key that has an invalid datatype (not a splice supported type)
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_bad_pk_bad_datatype, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set has a bad pk datatype'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_create_feature_set_invalid_pk_datatype_2(test_app, create_schema):
    """
    Tests creating a feature set with a primary key that has an invalid datatype (varchar without a length)
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_bad_pk_bad_datatype_2, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set PK is a varchar without a length'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_create_feature_set_invalid_pk_name(test_app, create_schema):
    """
    Tests creating a feature set with a PK name that is an invalid column name
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_bad_pk_bad_name, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because the feature set PK has an invalid column name'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_create_feature_set_in_featurestore_schema(test_app, create_schema):
    """
    Tests creating a feature set in the featurestore schema
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_bad_pk_schema_location, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because you cannot create feature sets in the ' \
                                                   'featurestore schema'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'


def test_create_feature_set_in_featurestore_schema_2(test_app, create_schema):
    """
    Tests creating a feature set in the sys schema
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_bad_pk_schema_location_2, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because you cannot create feature sets in the ' \
                                                   'sys schema'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'


def test_create_feature_set_in_featurestore_schema_3(test_app, create_schema):
    """
    Tests creating a feature set in the mlmanager schema
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=feature_set_bad_pk_schema_location_3, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code in range(400,500), 'Should fail because you cannot create feature sets in the ' \
                                                   'mlmanager schema'
    c = response.json()['code']
    assert 'BAD_ARGUMENTS' in c, f'Should get a validation error but got {c}'

def test_create_good_feature_set(test_app, create_schema):
    """
    Tests the successful creation of a good feature set 
    """
    APP.dependency_overrides[crud.get_db] = lambda: (yield create_schema) # Give the "server" the same db session

    response = test_app.post('/feature-sets', json=good_feature_set, auth=basic_auth)

    logger.info(f'status: {response.status_code}, -- message: {response.json()}')

    assert response.status_code == 201, 'Should succeed'
    r = response.json()
    logger.info("RESPONSE FROM FSET create")
    logger.info(r)
    assert 'schema_name' in r, f'response should contain created feature set, but had {r}'
