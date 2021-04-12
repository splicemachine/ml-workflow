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

class Airflow:

    AIRFLOW_URL = None
    auth = None
    is_active = False

    @staticmethod
    def get_variable_if_exists(variable: str):
        r = requests.get(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', auth=Airflow.auth)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as error:
            if error.response.status_code != 404:
                raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                    message=str(error))
            return None
        return json.loads(r.json()['value'])

    @staticmethod
    def schedule_feature_set_calculation(fset: str):
        variable = Variables.FEATURE_SETS
        fsets = Airflow.get_variable_if_exists(variable)
        if fsets == None:
            logger.info(f'Variable {variable} not found in airflow - creating...')
            fsets = { fset: { 'schedule_interval': '@daily'} }
            body = { 'key': variable, 'value': json.dumps(fsets) }
            r = requests.post(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}', json=body, auth=Airflow.auth)
        else:
            logger.info(f'Variable {variable} found in airflow - updating...')
            fsets[fset] = { 'schedule_interval': '@daily'}
            body = { 'key': variable, 'value': json.dumps(fsets) }
            r = requests.patch(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body, auth=Airflow.auth)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                    message=str(error))

    @staticmethod  
    def unschedule_feature_set_calculation(fset: str):
        variable = Variables.FEATURE_SETS
        fsets = Airflow.get_variable_if_exists(variable)
        if fsets != None:
            fsets.pop(fset)
            body = { 'key': variable, 'value': json.dumps(fsets) }
            try:
                requests.patch(f'{Airflow.AIRFLOW_URL}/{Endpoints.VARIABLES}/{variable}', json=body, auth=Airflow.auth).raise_for_status()
            except requests.exceptions.HTTPError as error:
                raise SpliceMachineException(status_code=error.response.status_code, code=ExceptionCodes.UNKNOWN,
                        message=str(error))

    @staticmethod
    def setup():
        url = env_vars.get('AIRFLOW_URL')
        if url:
            Airflow.AIRFLOW_URL = url
        else:
            Airflow.is_active = False
            return
        
        user = env_vars.get('AIRFLOW_USER')
        password = env_vars.get('AIRFLOW_PASSWORD')
        if user and password:
            Airflow.auth = (user, password)
        else:
            Airflow.is_active = False
            return

        try:
            requests.get(f'{Airflow.AIRFLOW_URL}/health').raise_for_status()
        except requests.exceptions.HTTPError:
            Airflow.is_active = False