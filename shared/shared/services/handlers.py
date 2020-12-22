from typing import Callable
import json
from shared.environments.cloud_environment import CloudEnvironments
from shared.models.splice_models import Handler
from shared.models.field import Field


class HandlerNames:
    """
    Class containing valid Handler Names
    """
    enable_service: str = 'ENABLE_SERVICE'
    disable_service: str = 'DISABLE_SERVICE'
    deploy_k8s: str = 'DEPLOY_KUBERNETES'
    undeploy_k8s: str = 'UNDEPLOY_KUBERNETES'
    schedule_retrain: str = 'SCHEDULE_RETRAIN'
    watch_job: str = 'WATCH_JOB'
    deploy_database: str = 'DEPLOY_DATABASE'
    deploy_csp: str = CloudEnvironments.get_current().handler_mapping.get('deploy')

    @staticmethod
    def get_run_handlers():
        """
        Get handlers that can run operations
        """
        run_handlers = [HandlerNames.deploy_database, HandlerNames.deploy_k8s]

        if HandlerNames.deploy_csp:
            run_handlers.append(HandlerNames.deploy_csp)

        return run_handlers


class KnownHandlers:
    """
    Class containing handler
    definitions
    """
    # Service Togglers
    MAPPING: dict = {
        HandlerNames.enable_service: Handler(
            payload_args=[
                Field('service', callback=lambda value: value.upper())
            ],
            name=HandlerNames.enable_service,
            url='/access',
            modifiable=False,
        ),
        HandlerNames.schedule_retrain: Handler(
            payload_args=[
                Field('cron_exp'),
                Field('name'),
                Field('run_id'),
                Field('conda_artifact'),
                Field('retrainer_artifact', use_default=True, default='retrainer.pkl')
            ],
            name=HandlerNames.schedule_retrain,
            url='/schedule_retrain',
            modifiable=True
        ),
        HandlerNames.watch_job: Handler(
            payload_args=[
                Field('k8s-identifier')
            ],
            name=HandlerNames.watch_job,
            url='/watch_k8s',
            modifiable=False
        ),
        HandlerNames.disable_service: Handler(
            payload_args=[
                Field('service', callback=lambda value: value.upper())
            ],
            name=HandlerNames.disable_service,
            url='/access',
            modifiable=False
        ),
        HandlerNames.deploy_database: Handler(
            payload_args=[
                Field('db_schema'),
                Field('db_table'),
                Field('run_id'),
                Field('create_model_table', callback=Field.string_to_boolean_converter, callback_on=str, default=False,
                      use_default=True),
                Field('df_schema', default=None, use_default=True, callback=json.loads, callback_on=str),
                Field('primary_key', default=None, use_default=True, callback=lambda value: dict([value.split(' ')]),
                      callback_on=str),
                Field('reference_schema', use_default=True, default=None),
                Field('reference_table', use_default=True, default=None),
                Field('model_cols', use_default=True, default=None, callback=lambda value: value.split(','),
                      callback_on=str),
                Field('classes', default=None, use_default=True, callback=lambda value: value.split(','),
                      callback_on=str),
                Field('library_specific', use_default=True, default={}, callback=lambda value: dict(
                    (key.strip(), val.strip()) for key, val in (element.split('=') for element in value.split(','))),
                      callback_on=str),
                Field('replace', use_default=True, default=False, callback=Field.string_to_boolean_converter,
                      callback_on=str)
            ],
            name=HandlerNames.deploy_database,
            modifiable=True,
            url='/deploy/database'
        ),
        HandlerNames.deploy_k8s: Handler(
            payload_args=[
                Field('run_id'),
                Field('service_port', use_default=True, default=80),
                Field('base_replicas', use_default=True, default=1),
                Field('autoscaling_enabled', use_default=True, default="false", callback=str, callback_on=bool),
                Field('max_replicas', use_default=True, default=2),
                Field('target_cpu_utilization', use_default=True, default=50),
                Field('disable_nginx', default="false", use_default=True, callback=str, callback_on=bool),
                Field('gunicorn_workers', use_default=True, default=1),
                Field('resource_requests_enabled', default="false", callback=str, callback_on=bool, use_default=True),
                Field('resource_limits_enabled', default="false", callback=str, callback_on=bool, use_default=True),
                Field('cpu_request', use_default=True, default="0.5"),
                Field('cpu_limit', use_default=True, default="1"),
                Field('memory_request', use_default=True, default="512Mi"),
                Field('memory_limit', use_default=True, default="2048Mi"),
                Field('expose_external', use_default=True, default="false", callback=str, callback_on=bool)
            ],
            name=HandlerNames.deploy_k8s,
            modifiable=True,
            url='/deploy/kubernetes'
        )
    }

    # Environment-Specific Handlers
    if CloudEnvironments.get_current_name() == CloudEnvironments.aws:
        MAPPING[HandlerNames.deploy_csp] = Handler(
            payload_args=[
                Field('run_id'),
                Field('app_name'),
                Field('deployment_mode', use_default=True, default='replace'),
                Field('sagemaker_region', use_default=True, default='us-east-2'),
                Field('instance_type', use_default=True, default='ml.m5.xlarge'),
                Field('instance_count', use_default=True, default=1),
                Field('pipeline', use_default=True, default='model')
            ],
            name=HandlerNames.deploy_csp,
            modifiable=True,
            url='/deploy/deploy_aws'
        )
    elif CloudEnvironments.get_current_name() == CloudEnvironments.azure:
        MAPPING[HandlerNames.deploy_csp] = Handler(
            payload_args=[
                Field('run_id'),
                Field('workspace'),
                Field('endpoint_name'),
                Field('resource_group'),
                Field('model_name', use_default=True, default=None),
                Field('region', use_default=True, default='East US'),
                Field('cpu_cores', use_default=True, default=0.1),
                Field('allocated_ram', use_default=True, default=0.5)
            ],
            name=HandlerNames.deploy_csp,
            modifiable=True,
            url='/deploy/deploy_azure'
        )

    @staticmethod
    def get_class(handler_name: str) -> Callable:
        """
        Get the associated handler class of a
        given handler object

        :param handler_name: (str) the handler name
            to retrieve the handler class of
        :return: (object) associated handler class
        """
        return KnownHandlers.MAPPING[handler_name].handler_class

    @staticmethod
    def get_url(handler_name: str) -> str:
        """
        Get the handler url (in Flask) for a given
        handler

        :param handler_name: (str) the name of the
            handler to retrieve for
        :return: (str) handler url
        """
        return KnownHandlers.MAPPING[handler_name].url

    @staticmethod
    def register(handler_name: str, handler: type) -> None:
        """
        Associate the given handler
        to a handler class

        :param handler_name: (str) the handler name
            to associate
        :param handler: (object) the handler class
            to associate the handler with
        """
        KnownHandlers.MAPPING[handler_name].assign_handler(handler)

    @staticmethod
    def get_valid() -> tuple:
        """
        Return a list of valid handlers
        :return: (tuple) list of valid handlers
        """
        return tuple(KnownHandlers.MAPPING.values())

    @staticmethod
    def get_modifiable() -> tuple:
        """
        Return the names of modifiable handlers
        :return: (tuple) list of modifiable handlers
        """
        return tuple(handler.name for handler in KnownHandlers.get_valid() if handler.modifiable)


def populate_handlers(Session) -> None:
    """
    Populates the handlers table with
    handlers which have their names
    specified in the 'handlers_list'
    argument, if they don't exist already

    :param Session: (Session) current
        database.py-object namespace for thread
    """
    db_handler_names: list = [db_handler.name for db_handler in Session.query(Handler).all()]

    for handler in KnownHandlers.get_valid():
        if handler.name not in db_handler_names:
            Session.add(handler)

    Session.commit()
