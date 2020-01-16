"""
Contains handler and functions
pertaining to Azure Model Deployment
"""
import logging
from copy import copy
from functools import partial
from os import environ as env_vars

from azureml.core import Workspace
from azureml.core.webservice import AciWebservice, Webservice
from azureml.core.authentication import AzureCliAuthentication

from mlflow import azureml as mlflow_azureml
from .base_deployment_handler import BaseDeploymentHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)


class AzureDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for handling Azure Deployment Jobs
    """

    def __init__(self, task_id: int, spark_context=None) -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) Id of job to process
        :param spark_context: (SparkContext) Global FAIR SparkContext for deserializing models
        """
        BaseDeploymentHandler.__init__(self, task_id, spark_context)

        self.Workspace: Workspace or None = None
        self.azure_image: object = None
        self.azure_model: object = None

    @staticmethod
    def _generate_dockerfile(initial_function, output_path: str, mlflow_path: str = None) -> None:
        """
        Unfortunately, MLFlow AzureML doesn't really support
        Spark (ironically enough, since the maker of MLFlow,
        Databricks, also made spark). Thus, we have to monkey
        patch the MLFlow code in order for this deployment
        to work.

        This function must match the signature of
        mlflow.azureml._create_dockerfile. It will need
        to be updated if this function is moved/updated
        in a future release.

        :param initial_function: (callable) the initial mlflow
            create dockerfile function
        :param output_path: (str) the local path to save
            the Dockerfile to
        :param mlflow_path: (str) a local version of the MLFlow
            source. If not specified, will download from Pypi.
        """
        jre: str = 'default-jre'
        commands: list = [f'RUN apt-get update && apt-get install -y {jre}']

        initial_function(output_path, mlflow_path)  # write the initial Dockerfile

        with open(output_path, 'r+') as Dockerfile:
            content: str = Dockerfile.read()
            Dockerfile.seek(0)
            prepend_commands: str = "\n".join(commands)
            updated_dockerfile: str = f'{prepend_commands}\n{content}'

            LOGGER.info(f"Dockerfile Content:\n{updated_dockerfile}")

            Dockerfile.write(updated_dockerfile)

    def _add_spark_setup_to_dockerfile(self) -> None:
        """
        Monkey patch the AzureML Dockerfile
        to install a Java Runtime Environment
        or container will die.
        """
        self.update_task_in_db(info="Adjusting Dockerfile for Spark Support")
        # we need to create a copy so the function
        # won't be overwritten when we change its value.
        # functions are passed by reference.
        mlflow_create_dockerfile_function = copy(mlflow_azureml._create_dockerfile)

        # "freeze" the first argument of the function (the initial function) to be
        # the original copy, so we can sort of do a super() on the function
        mlflow_azureml._create_dockerfile = partial(AzureDeploymentHandler._generate_dockerfile,
                                                    mlflow_create_dockerfile_function)

    def _get_or_create_azureml_workspace(self):
        """
        Create/Retrieve the Specified AzureML
        Workspace
        """
        # self.update_task_in_db(info='Creating AzureML Workspace')
        self.update_task_in_db(info='Trying cli auth')

        cli_auth = AzureCliAuthentication()
        self.update_task_in_db(info=f'{type(cli_auth)}')
        self.Workspace = Workspace.create(
            name=self.task.parsed_payload['workspace'],
            subscription_id=env_vars['AZURE_SUBSCRIPTION_ID'],  # extracted from az login
            resource_group=self.task.parsed_payload['resource_group'],
            location=self.task.parsed_payload['region'],
            create_resource_group=True,  # create resource group if it doesn't exist
            exist_ok=True,  # get the Workspace if it already exists, otherwise create it
            auth=cli_auth # to avoid calling InteactiveLogin
        )

    def _build_docker_image(self):
        """
        Build the MLFlow Docker container and
        push it to the specified workspace
        :return:
        """
        self.update_task_in_db(info='Building MLFlow Docker Container')
        self.azure_image, self.azure_model = mlflow_azureml.build_image(
            model_name=self.task.parsed_payload['model_name'],  # name of model
            model_uri=self.downloaded_model_path,
            workspace=self.Workspace,
            synchronous=True  # block thread until job completes
        )
        LOGGER.error(
            f"Access the following URI for build logs: {self.azure_image.image_build_log_uri}"
        )

    def _deploy_model_to_azure(self):
        """
        Deploy the generated model from the Docker
        Image to AzureML
        """
        self.update_task_in_db(info="Waiting for AzureML Deployment to finish")

        webservice_deployment_config: AciWebservice.deploy_configuration = \
            AciWebservice.deploy_configuration(  # deployment specs
                cpu_cores=self.task.parsed_payload['cpu_cores'],
                memory_gb=self.task.parsed_payload['allocated_ram']
            )

        webservice = Webservice.deploy_from_image(
            deployment_config=webservice_deployment_config,
            image=self.azure_image,
            workspace=self.Workspace,
            name=self.task.parsed_payload['endpoint_name']  # name of azureml endpoint
        )
        webservice.wait_for_deployment()  # block

    def execute(self) -> None:
        """
        Execute the steps required to
        deploy a given MLFlow model to AzureML
        """
        # we copy the initial function as otherwise, when we monkey-patch it
        # it will be passed by reference and overwritten. this prevents that.

        steps: tuple = (
            self._retrieve_model_binary_stream_from_db,
            self._deserialize_artifact_stream,
            self._get_or_create_azureml_workspace,
            self._add_spark_setup_to_dockerfile,
            self._build_docker_image,
            self._deploy_model_to_azure,
        )

        for execute_step in steps:
            execute_step()
