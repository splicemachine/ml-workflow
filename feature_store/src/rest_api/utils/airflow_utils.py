from os import environ as env_vars
import requests
import json

from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.logger.logging_config import logger

class Endpoints:
    """
    Enum for Airflow Endpoints
    """
    VARIABLES: str = "variables"

class Variables:
    """
    Enum for the Variables stored in Airflow
    """
    FEATURE_SETS: str = "feature_sets"

AIRFLOW_URL = env_vars['AIRFLOW_URL']
auth = (env_vars['AIRFLOW_USER'], env_vars['AIRFLOW_PASSWORD'])

def get_variable_if_exists(variable: str) -> bool:
    r = requests.get(f'{AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', auth=auth)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as error:
        if error.response.status_code != 404:
            raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                message=str(error))
        return None
    return json.loads(r.json()['value'])

def schedule_feature_set_calculation(fset: str):
    variable = Variables.FEATURE_SETS
    fsets = get_variable_if_exists(variable)
    if fsets == None:
        logger.info(f'Variable {variable} not found in airflow - creating...')
        fsets = { fset: { 'schedule_interval': '@daily'} }
        body = { 'key': variable, 'value': json.dumps(fsets) }
        r = requests.post(f'{AIRFLOW_URL}/{Endpoints.VARIABLES}', json=body, auth=auth)
    else:
        logger.info(f'Variable {variable} found in airflow - updating...')
        fsets[fset] = { 'schedule_interval': '@daily'}
        body = { 'key': variable, 'value': json.dumps(fsets) }
        r = requests.patch(f'{AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body, auth=auth)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as error:
        raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                message=str(error))

def unschedule_feature_set_calculation(fset: str):
    variable = Variables.FEATURE_SETS
    fsets = get_variable_if_exists(variable)
    if fsets != None:
        fsets.pop(fset)
        body = { 'key': variable, 'value': json.dumps(fsets) }
        try:
            requests.patch(f'{AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body, auth=auth).raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                    message=str(error))