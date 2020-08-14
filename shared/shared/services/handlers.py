from typing import Callable

from shared.environments.cloud_environment import CloudEnvironments
from shared.models.splice_models import Handler


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
            required_payload_args=('service',),
            optional_payload_args=dict(),
            name=HandlerNames.enable_service,
            url='/access',
            modifiable=False,
        ),
        HandlerNames.disable_service: Handler(
            required_payload_args=('service',),
            optional_payload_args=dict(),
            name=HandlerNames.disable_service,
            url='/access',
            modifiable=False
        ),
        HandlerNames.deploy_database: Handler(
            required_payload_args=('db_schema', 'db_table', 'run_id'),
            optional_payload_args=dict(
                df_schema=None,
                primary_key=None,
                create_model_table=False,
                reference_schema=None,
                reference_table=None,
                model_cols=None,
                classes=None,
                library_specific={},
                replace=False
            ),
            name=HandlerNames.deploy_database,
            url='/deploy/database'
        ),
        HandlerNames.deploy_k8s: Handler(
            required_payload_args=('run_id',),
            optional_payload_args=dict(
                service_port=80,
                base_replicas=1,
                autoscaling_enabled=False,
                max_replicas=1,
                target_cpu_utilization=50,
                disable_nginx=False,
                gunicorn_workers=1,  # strongly recommended on spark models to prevent OOM
                resource_requests_enabled=True,
                resource_limits_enabled=True,
                cpu_request="0.5",
                cpu_limit="1",
                memory_request="512Mi",
                memory_limit="2048Mi",
                expose_external=False,
            ),
            name=HandlerNames.deploy_k8s,
            url='/deploy/kubernetes'
        )
    }

    # Environment-Specific Handlers
    if CloudEnvironments.get_current_name() == CloudEnvironments.aws:
        MAPPING[HandlerNames.deploy_csp] = Handler(
            required_payload_args=('run_id', 'app_name'),
            optional_payload_args=dict(
                deployment_mode='replace',
                sagemaker_region='us-east-2',
                instance_type='ml.m5.xlarge',
                instance_count=1,
                model_dir='pipeline'
            ),
            name=HandlerNames.deploy_csp,
            modifiable=True,
            url='/deploy/deploy_aws'
        )
    elif CloudEnvironments.get_current_name() == CloudEnvironments.azure:
        MAPPING[HandlerNames.deploy_csp] = Handler(
            required_payload_args=('run_id', 'workspace', 'endpoint_name', 'resource_group'),
            optional_payload_args=dict(
                model_name=None,
                region='East US',
                cpu_cores=0.1,
                allocated_ram=0.5
            ),
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
