"""
Definition of Base Run Handler
"""
import logging
from abc import abstractmethod
from os import environ as env_vars
from subprocess import check_call as run_shell_command

from mlflow.tracking import MlflowClient
from mlmanager_lib.database.mlflow_models import SqlArtifact
from pyspark.ml import PipelineModel

from mlflow import spark
from ..base_handler import BaseHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)

DOWNLOAD_PATH: str = env_vars["WORKER_HOME"] + "/pmml"


class BaseDeploymentHandler(BaseHandler):
    """
    Base class for run handlers--
    handlers that execute jobs (AWS Deployment etc.)
    """

    def __init__(self, task_id: int, spark_context) -> None:
        """
        :param task_id: (int) id of the task to execute
        """
        BaseHandler.__init__(self, task_id, spark_context=spark_context)
        self.downloaded_model_path: str = DOWNLOAD_PATH
        self.mlflow_run: object = None
        self.artifact: bytearray or None = None

    def retrieve_run_from_mlflow(self) -> None:
        """
        Retrieve the current run from mlflow tracking server
        """
        client: MlflowClient = MlflowClient(tracking_uri=env_vars['MLFLOW_URL'])
        try:
            self.mlflow_run: object = client.get_run(self.task.parsed_payload['run_id'])
        except Exception:
            raise Exception(
                "Error: The Run associated with the ID specified could not be retrieved"
            )

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

    def _cleanup(self) -> None:
        """
        Cleanup after the model is deployed
        """
        temp_glob: str = "/tmp/tmp*"
        self.update_task_in_db(info='Cleaning Up')
        run_shell_command(('rm', '-Rf', self.downloaded_model_path))
        run_shell_command(('rm', '-Rf', temp_glob))  # cleanup azure deployment files

    @abstractmethod
    def execute(self) -> None:
        """
        Subclass specific run functionality
        """
        pass

    def _handle(self) -> None:
        """
        We add the MLFlow Run URL as a parameter
        that can be displayed in a GUI for all
        of these Jobs.
        """
        try:
            self.retrieve_run_from_mlflow()
            run_url: str = f"/#/experiments/{self.mlflow_run.info.experiment_id}/" \
                f"runs/{self.mlflow_run.info.run_uuid}"

            LOGGER.info(f"Using `Retrieved MLFlow Run: {self.mlflow_run}")

            self.task.mlflow_url: str = f"<a href='{run_url}' target='_blank' onmouseover=" \
                f"'javascript:event.target.port={env_vars['MLFLOW_PORT']}'>Link to Mlflow Run</a>"

            self.model_dir: str = self.mlflow_run.data.tags['splice.model_name']

            # populates a link to the associated Mlflow run that opens in a new tab.
            self.Session.add(self.task)
            self.Session.commit()
            self.execute()
        except Exception as e:
            raise e

        finally:
            self._cleanup()  # always run cleanup, regardless of success or failure
