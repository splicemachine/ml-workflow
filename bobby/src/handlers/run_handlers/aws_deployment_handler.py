import logging
from os import environ as env_vars
from subprocess import check_call as run_shell_command

from mlmanager_lib.database.mlflow_models import SqlArtifact
from pyspark.ml import PipelineModel

from mlflow import sagemaker
from mlflow import spark
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

    def __init__(self, task_id: int, spark_context=None) -> None:
        """
        Initialize Base Haåndler
        constructor (set instance variables
        etc.)

        :param task_id: (int) Id of job to process

        """
        BaseRunHandler.__init__(self, task_id, spark_context)
        self.downloaded_model_path: str = DOWNLOAD_PATH
        self.model_dir: str or None = None
        self.artifact: bytearray or None = None

    def _retrieve_model_binary_stream_from_db(self) -> None:
        """
        Use SQLAlchemy to retrieve the model artifact
        from Database with the specified path
        and associated Run UUID
        """
        self.update_task_in_db(info="Reading Model Artifact Stream from Splice Machine")
        run_id: str = self.mlflow_run.info.run_uuid

        LOGGER.info(f"Extracting Model from DB with Path: {self.model_dir} and ")
        try:
            self.artifact = bytearray(list(self.Session.query(SqlArtifact.binary).filter_by(
                name=self.model_dir).filter_by(run_uuid=run_id).first())[0])

            LOGGER.info(self.artifact)
        except IndexError:
            LOGGER.exception(
                f"No artifact could be found in database with path {self.model_dir} and run_id "
                f"{run_id}"
            )
            raise Exception("Model with the specified Run ID and Name could not be found!")

    def _deserialize_artifact_stream(self) -> None:
        """
        Take the BLOB Retrieved from the database,
        convert it into a Pyspark PipelineModel,
        and then serialize it to the disk for deployment
        """
        self.update_task_in_db(info="Decoding Model Artifact Binary Stream for Deployment")

        # Convert BLOB (Java byte[]) to a Python Pipeline Model via py4j
        binary_input_stream = self.spark_context._jvm.java.io.ByteArrayInputStream(self.artifact)
        object_input_stream = self.spark_context._jvm.java.io.ObjectInputStream(binary_input_stream)

        deserialized_pipeline: PipelineModel = PipelineModel._from_java(
            object_input_stream.readObject()
        )  # convert Java PipelineModel into a Python Pipeline Model

        spark.save_model(deserialized_pipeline, f'{DOWNLOAD_PATH}/')
        # MLFlow's Spark.save_model will create an MLModel and conda environment
        # opposed to Spark... saves model to disk

        # close the stream
        object_input_stream.close()

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
            f"{DOWNLOAD_PATH}/",
            # local path of model with suffix
            execution_role_arn=env_vars['SAGEMAKER_ROLE'],
            region_name=payload['sagemaker_region'],
            mode=payload['deployment_mode'],
            instance_type=payload['instance_type'],
            instance_count=int(payload['instance_count'])
        )

    def _cleanup(self) -> None:
        """
        Cleanup after the model is deployed
        """
        self.update_task_in_db(info='Cleaning Up')
        run_shell_command(('rm', '-Rf', DOWNLOAD_PATH))

    def execute(self) -> None:
        """
        Deploy Job to SageMaker
        """
        LOGGER.info(f"Using Retrieved MLFlow Run: {self.mlflow_run}")
        self.model_dir: str = self.mlflow_run.data.tags['splice.model_name']
        env_vars['AWS_DEFAULT_REGION']: str = self.task.parsed_payload['sagemaker_region']

        steps: tuple = (
            self._retrieve_model_binary_stream_from_db,  # Retrieve Model BLOB
            self._deserialize_artifact_stream,  # Deserialize it to the Disk
            self._deploy_model_to_sagemaker,  # Deploy model to SageMaker
            self._cleanup  # Cleanup unnecessary files
        )

        for execute_step in steps:
            execute_step()
