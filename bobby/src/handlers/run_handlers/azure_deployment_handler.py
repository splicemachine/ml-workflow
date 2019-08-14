"""
Contains handler and functions
pertaining to Azure Model Deployment
"""
import logging
from os import environ as env_vars

from azureml.core import Workspace
from azureml.core.webservice import AciWebservice, Webservice

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

    def _get_or_create_azureml_workspace(self):
        """
        Create/Retrieve the Specified AzureML
        Workspace
        """
        self.update_task_in_db(info='Creating AzureML Workspace')

        self.Workspace = Workspace.create(
            name=self.task.parsed_payload['workspace'],
            subscription_id=env_vars['AZURE_SUBSCRIPTION_ID'],  # extracted from az login
            resource_group=self.task.parsed_payload['resource_group'],
            location=self.task.parsed_payload['region'],
            create_resource_group=True,  # create resource group if it doesn't exist
            exist_ok=True  # get the Workspace if it already exists, otherwise create it
        )

    def _build_docker_image(self):
        """
        Build the MLFlow Docker container and
        push it to the specified workspace
        :return:
        """
        self.update_task_in_db(info='Building MLFlow Docker Container')
        self.azure_image, self.azure_image = mlflow_azureml.build_image(
            image_name=self.task.parsed_payload['image_name'],  # name of container
            model_name=self.task.parsed_payload['model_name'],  # name of model
            model_uri=self.downloaded_model_path,
            workspace=self.Workspace,
            synchronous=True  # block thread until job completes
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
        steps: tuple = (
            self._retrieve_model_binary_stream_from_db,
            self._deserialize_artifact_stream,
            self._get_or_create_azureml_workspace,
            self._build_docker_image,
            self._deploy_model_to_azure,
            self._cleanup
        )

        for execute_step in steps:
            execute_step()