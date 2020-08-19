"""
Definition of Base Run Handler
"""

from abc import abstractmethod
from io import BytesIO
from os import environ as env_vars
from os.path import exists
from subprocess import check_call as run_shell_command
from zipfile import ZipFile

from mlflow.tracking import MlflowClient
from shared.models.mlflow_models import SqlArtifact
from shared.logger.job_lifecycle_manager import JobLifecycleManager

from ..base_handler import BaseHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

DOWNLOAD_PATH: str = f'{env_vars["WORKER_HOME"]}/pmml'


class BaseDeploymentHandler(BaseHandler):
    """
    Base class for run handlers--
    handlers that execute jobs (AWS Deployment etc.)
    """

    def __init__(self, task_id: int) -> None:
        """
        :param task_id: (int) id of the task to execute
        """
        BaseHandler.__init__(self, task_id)
        self.downloaded_model_path: str = DOWNLOAD_PATH + str(
            task_id)  # So when we temporarily download the model we don't overwrite other models
        self.mlflow_run: object = None
        self.artifact_buffer: bytearray or None = None

    def retrieve_run_from_mlflow(self) -> None:
        """
        Retrieve the current run from mlflow tracking server
        """
        self.logger.info("Retrieving Run from MLFlow Tracking Server...", send_db=True)
        client: MlflowClient = MlflowClient(tracking_uri=env_vars['MLFLOW_URL'])
        try:
            self.mlflow_run: object = client.get_run(self.task.parsed_payload['run_id'].strip())
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
        self.logger.info("Reading Model Artifact Stream from Splice Machine", send_db=True)
        run_id: str = self.mlflow_run.info.run_uuid

        self.logger.info(f"Extracting Model from DB with Name: {self.model_dir}", send_db=True)
        try:
            self.artifact = self.Session.query(SqlArtifact) \
                .filter_by(name=self.model_dir) \
                .filter_by(run_uuid=run_id).one()

        except IndexError:
            self.logger.exception(
                f"No artifact could be found in database with name {self.model_dir} and run_id "
                f"{run_id}", send_db=True
            )
            raise Exception("Model with the specified Run ID and Name could not be found!")

    def _deserialize_artifact_stream(self) -> None:
        """
        Take the BLOB Retrieved from the database.py,
        convert it into a model,
        and then serialize it to the disk for deployment
        """
        self.logger.info("Decoding Model Artifact Binary Stream for Deployment", send_db=True)

        try:
            if exists(self.downloaded_model_path):
                run_shell_command(('rm', '-Rf', self.downloaded_model_path))

            artifact_buffer = BytesIO()
            artifact_buffer.write(self.artifact.binary)
            artifact_buffer.seek(0)
            self.logger.info("Decompressing Model Artifact", send_db=True)
            ZipFile(artifact_buffer).extractall(path=self.downloaded_model_path)

        except KeyError as e:
            self.logger.exception("Unable to find the specified file extension handler", send_db=True)
            raise Exception(f"Unable to find the specified fix extension {self.artifact.file_extension}")

    def _cleanup(self) -> None:
        """
        Cleanup after the model is deployed
        """
        self.logger.info("Cleaning up deployment", send_db=True)
        temp_glob: str = "/tmp/tmp*"  # remove all temp files generated by MLFlow

        run_shell_command(('rm', '-Rf', self.downloaded_model_path))
        run_shell_command(('rm', '-Rf', temp_glob))  # cleanup azure deployment files

    @abstractmethod
    def execute(self) -> None:
        """
        Subclass specific run functionality
        """
        pass

    def exception_handler(self, exc: Exception):
        """
        Function that runs if there is an error
        executing a job
        :param exc: the exception thrown
        """
        self.logger.error(f"Running Exception Callback because of encountered: '{exc}'", send_db=True)
        self.logger.warning("Rolling back.", send_db=True)
        self.Session.rollback()
        raise exc

    def _handle(self) -> None:
        """
        We add the MLFlow Run URL as a parameter
        that can be displayed in a GUI for all
        of these Jobs.
        """
        try:
            self.retrieve_run_from_mlflow()
            run_url: str = f"#/experiments/{self.mlflow_run.info.experiment_id}/" \
                           f"runs/{self.mlflow_run.info.run_uuid}"

            self.logger.info(f"Retrieved MLFlow Run", send_db=True)

            self.model_dir: str = self.mlflow_run.data.tags['splice.model_name']

            self.Session.merge(self.task)
            # populates a link to the associated Mlflow run that opens in a new tab.
            self.logger.info("Updating MLFlow Run for the UI", send_db=True)
            self.task.mlflow_url = f"<a href='/mlflow/{run_url}' target='_blank' onmouseover=" \
                                   f">Link to Mlflow Run</a>"

            JobLifecycleManager.Session.add(self.task)  # when editing jobs, we must use the manager
            JobLifecycleManager.Session.commit()

            self.execute()
            self._cleanup()
        except Exception as e:
            self._cleanup()  # always run cleanup, regardless of success or failure
            self.exception_handler(exc=e)  # can be overriden by subclasses
            raise e
