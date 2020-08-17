from typing import Callable

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
    deploy_database: str = 'DEPLOY_DATABASE'
    deploy_csp: str = CloudEnvironments.get_current().handler_mapping.get('deploy')

    @staticmethod
    def get_run_handlers():
        """
        Get handlers that can run operations
        """
        run_handlers = [HandlerNames.deploy_k8s, HandlerNames.deploy_database]

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
                Field('service')
            ],
            name=HandlerNames.enable_service,
            url='/access',
            modifiable=False,
        ),
        HandlerNames.disable_service: Handler(
            payload_args=[
                Field('service')
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
                Field('df_schema', default=None),
                Field('primary_key', default=None, callback=lambda value: dict([value.split(' ')]), callback_on=str),
                Field('create_model_table', default=False),
                Field('reference_schema', default=None),
                Field('reference_table', default=None),
                Field('model_cols', default=None, callback=lambda value: value.split(','), callback_on=str),
                Field('classes', default=None, callback=lambda value: value.split(','), callback_on=str),
                Field('library_specific', default={}, callback=lambda value: dict(
                    (key.strip(), val.strip()) for key, val in (element.split('=') for element in value.split(','))),
                      callback_on=str),
                Field('replace', default=False, callback=Field.string_to_boolean_converter, callback_on=str)
            ],
            name=HandlerNames.deploy_database,
            modifiable=True,
            url='/deploy/database'
        ),
        HandlerNames.deploy_k8s: Handler(
            payload_args=[
                Field('run_id'),
                Field('service_port', default=80),
                Field('base_replicas', default=1),
                Field('autoscaling_enabled', default=False, callback=Field.string_to_boolean_converter,
                      callback_on=str),
                Field('max_replicas', default=2),
                Field('target_cpu_utilization', default=50),
                Field('disable_nginx', default=False, callback=Field.string_to_boolean_converter, callback_on=str),
                Field('gunicorn_workers', default=1),
                Field('resource_requests_enabled', default=False, callback=Field.string_to_boolean_converter,
                      callback_on=str),
                Field('resource_limits_enabled', default=False, callback=Field.string_to_boolean_converter,
                      callback_on=str),
                Field('cpu_request', default="0.5"),
                Field('cpu_limit', default="1"),
                Field('memory_request', default="512Mi"),
                Field('memory_limit', default="2048Mi"),
                Field('expose_external', default=False, callback=Field.string_to_boolean_converter, callback_on=str)
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
                Field('deployment_mode', default='replace'),
                Field('sagemaker_region', default='us-east-2'),
                Field('instance_type', default='ml.m5.xlarge'),
                Field('instance_count', default=1),
                Field('pipeline', default='model')
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
                Field('model_name', default=None),
                Field('region', default='East US'),
                Field('cpu_cores', default=0.1),
                Field('allocated_ram', default=0.5)
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
