"""
Contains handler and functions
pertaining to AWS Model Deployment
"""
import logging
from os import environ as env_vars

from mlflow import sagemaker
from .base_deployment_handler import BaseDeploymentHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)


class SageMakerDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for handling AWS deployment jobs
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

    def _deploy_model_to_sagemaker(self) -> None:
        """
        Deploy a model to sagemaker using a specified iam role, app name, model path and region
        """

        self.update_task_in_db(
            info='Waiting for SageMaker to activate endpoint'
        )  # Update Information

        payload: dict = self.task.parsed_payload

        sagemaker.deploy(
            payload['app_name'],
            self.downloaded_model_path,
            # local path of model with suffix
            execution_role_arn=env_vars['SAGEMAKER_ROLE'],
            region_name=payload['sagemaker_region'],
            mode=payload['deployment_mode'],
            instance_type=payload['instance_type'],
            instance_count=int(payload['instance_count'])
        )

    def execute(self) -> None:
        """
        Deploy Job to SageMaker
        """
        env_vars['AWS_DEFAULT_REGION']: str = self.task.parsed_payload['sagemaker_region']

        steps: tuple = (
            self._retrieve_model_binary_stream_from_db,  # Retrieve Model BLOB
            self._deserialize_artifact_stream,  # Deserialize it to the Disk
            self._deploy_model_to_sagemaker,  # Deploy model to SageMaker
        )

        for execute_step in steps:
            execute_step()
