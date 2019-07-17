import logging
from os import environ as env_vars
from subprocess import check_call as run_shell_command
from yaml import load as load_yaml, dump as write_yaml

from .definitions import Job
from .base_handler import BaseHandler

import mlflow
import mlflow.sagemaker

logger = logging.getLogger(__name__)

# Anaconda Versions for Deployed Docker Container
VERSIONS: dict = {
    "python": "3.6",
    "pandas": "0.22.0",
    "pyspark": "2.4.3",
    "mlflow": "1.0.0"
}

DOCKER_CONDA_ENVIRONMENT: str = \
    f"""
    name: pyspark_conda
    channels:
        - defaults
    dependencies:
        - python={VERSIONS['python']}
        - pandas={VERSIONS['pandas']}
        - pip:
            - pyspark=={VERSIONS['pyspark']}
            - mlflow=={VERSIONS['mlflow']}
    """

DOWNLOAD_PATH: str = env_vars["BOBBY_WORKER_HOME"] + "/pmml"


class SageMakerDeploymentHandler(BaseHandler):
    """
    Handler for handling deployment jobs
    """

    def __init__(self, task: Job, _handler_name='DEPLOY_AWS') -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the disable
            handler job to process

        """
        BaseHandler.__init__(self, task, _handler_name)
        self.downloaded_model_path: str = DOWNLOAD_PATH

    def _retrieve_model_from_s3(self) -> None:
        """
        Retrieve the model specified in the
        task payload from S3
        """
        self.update_task_in_db(info="Downloading Model PMML from S3")

        artifact_uri: str = f'{env_vars["S3_BUCKET_NAME"]}/{env_vars["MLFLOW_PERSIST_PATH"]}/' \
            f'{self.task.payload["experiment_id"]}/{self.task.payload["run_id"]}/'

        logger.info(f"Downloading Model MLFlow Artifact from S3 @ {artifact_uri}")

        shell_command: tuple = ('aws', 's3', 'sync', artifact_uri, self.downloaded_model_path)

        run_shell_command(shell_command)
        logger.debug(f"Completed download of Artifact from S3 -> {self.downloaded_model_path}")

    def _configure_anaconda_environment(self) -> None:
        """
        MLFlow uses a conda environment to configure
        Python in Prediction containers deployed to SageMaker.
        We need to configure the model to use a conda environment
        once it is deployed to retrieve its dependencies.
        """
        self.update_task_in_db(info="Configuring Model for Deployment")

        conda_file_path: str = f"{self.downloaded_model_path}/conda.yaml"
        ml_model_file_path: str = f"{self.downloaded_model_path}/MLmodel"

        logger.debug(f"Configuring Conda File for Deployment @ {conda_file_path}")

        with open(conda_file_path, 'w') as conda_file:
            conda_file.write(DOCKER_CONDA_ENVIRONMENT)

        logger.debug(f"Configuring MLmodel file to use Anaconda @ {ml_model_file_path}")

        with open(ml_model_file_path, 'r+') as ml_model_file:
            ml_model_yaml: dict = load_yaml(ml_model_file)
            ml_model_yaml['flavors']['python_function']['env'] = 'conda.yaml'  # reference above file when deployed
            ml_model_file.seek(0)  # go back to beginning of file so we can overwrite it
            write_yaml(ml_model_yaml, stream=ml_model_file, default_flow_style=False)
            ml_model_file.truncate()

        logger.info("Done configuring MLmodel file to use conda environment when deployed.")

    def _build_and_push_image(self) -> None:
        """
        Build and push MLFlow Container to ECR
        """

        self.update_task_in_db(info="Building and Pushing Model Container to AWS")

        logger.debug("Running Bash Command to build and push MLFlow Docker Container to ECR")
        shell_commands = ("mlflow", "sagemaker", "build-and-push-container")

        # TODO @amrit: Do this in jenkins (somehow) so we can get rid of DIND and just deploy because
        # image will already be in ECR

        run_shell_command(shell_commands)
        logger.info("Done Building and Pushing MLFlow Docker container to ECR")

    def _deploy_model_to_sagemaker(self) -> None:
        """
        Deploy a model to sagemaker using a specified iam role, app name, model path and region
        """

        self.update_task_in_db(info='Deploying model to SageMaker')  # Update Information

        payload: dict = self.task.payload

        mlflow.sagemaker.deploy(
            payload['app_name'],
            f"{self.downloaded_model_path}/{payload['model_dir']}",  # local path of model with suffix
            execution_role_arn=env_vars['SAGEMAKER_ROLE'],
            region_name=payload['sagemaker_region'],
            mode=payload['deployment_mode'],
            instance_type=payload['instance_type'],
            instance_count=int(payload['instance_count'])
        )

    def _handle(self) -> None:
        """
        Deploy Job to SageMaker
        """
        steps: tuple = (
            self._retrieve_model_from_s3,
            self._configure_anaconda_environment,
            self._build_and_push_image,
            self._deploy_model_to_sagemaker
        )

        for execute_step in steps:
            execute_step()
