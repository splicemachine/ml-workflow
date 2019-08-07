import logging
from os import environ as env_vars
from subprocess import check_call as run_shell_command

import mlflow.sagemaker

import mlflow
from .base_run_handler import BaseRunHandler

"""
Contains handler and functions
pertaining to AWS Model Deployment
"""

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)

DOWNLOAD_PATH: str = env_vars["WORKER_HOME"] + "/pmml"


class SageMakerDeploymentHandler(BaseRunHandler):
    """
    Handler for handling deployment jobs
    """

    def __init__(self, task_id: int) -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (int) Id of job to process

        """
        BaseRunHandler.__init__(self, task_id)
        self.downloaded_model_path: str = DOWNLOAD_PATH
        self.model_dir: str or None = None

    def _retrieve_model_from_s3(self) -> None:
        """
        Retrieve the model specified in the
        task payload from S3
        """
        self.update_task_in_db(info="Downloading Model PMML from S3")
        LOGGER.info(
            f"Downloading Model MLFlow Artifact from S3 @ {self.mlflow_run.info.artifact_uri}"
        )

        shell_command: tuple = (
            'aws', 's3', 'sync', self.mlflow_run.info.artifact_uri, self.downloaded_model_path
        )
        run_shell_command(shell_command)
        LOGGER.debug(f"Completed download of Artifact from S3 -> {self.downloaded_model_path}")

    def _build_and_push_image(self) -> None:
        """
        Build and push MLFlow Container to ECR
        """

        self.update_task_in_db(info="Building and Pushing Model Container to AWS")

        LOGGER.debug("Running Bash Command to build and push MLFlow Docker Container to ECR")
        shell_commands = ("mlflow", "sagemaker", "build-and-push-container")

        run_shell_command(shell_commands)
        LOGGER.info("Done Building and Pushing MLFlow Docker container to ECR")

    def _deploy_model_to_sagemaker(self) -> None:
        """
        Deploy a model to sagemaker using a specified iam role, app name, model path and region
        """

        self.update_task_in_db(info='Deploying model to SageMaker')  # Update Information

        payload: dict = self.task.parsed_payload

        mlflow.sagemaker.deploy(
            payload['app_name'],
            f"{self.downloaded_model_path}/{self.model_dir}",
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
        LOGGER.info(f"Using Retrieved MLFlow Run: {self.mlflow_run}")
        self.model_dir: str = self.mlflow_run.data.tags['model_dir']
        env_vars['AWS_DEFAULT_REGION']: str = self.task.parsed_payload['sagemaker_region']

        steps: tuple = (
            self._retrieve_model_from_s3,
            self._build_and_push_image,
            self._deploy_model_to_sagemaker
        )

        for execute_step in steps:
            execute_step()
