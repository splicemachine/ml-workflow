"""
Splice Machine Custom
Artifact Store that uses
BLOBS
"""
import os
import re
import tempfile
from contextlib import contextmanager
from os.path import splitext

from sqlalchemy.orm import load_only

from mlflow.entities.file_info import FileInfo
from mlflow.exceptions import INTERNAL_ERROR, MlflowException
from mlflow.protos.databricks_pb2 import (INVALID_PARAMETER_VALUE,
                                          RESOURCE_DOES_NOT_EXIST)
from mlflow.store.artifact.artifact_repo import ArtifactRepository
from mlflow.store.tracking.sqlalchemy_store import SqlTag
from shared.logger.logging_config import logger
from shared.models.mlflow_models import SqlArtifact
from shared.services.database import SQLAlchemyClient

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


class SpliceMachineArtifactStore(ArtifactRepository):

    def __init__(self, artifact_uri: str) -> None:
        super().__init__(artifact_uri)

        _match = re.compile("^.*?:\/\/(?P<experiment>.*?)\/(?P<runid>.*?)\/\w+$").match(artifact_uri)

        if not _match:
            raise MlflowException("The Artifact Path Specified is Invalid")

        # extract data from regex
        self.experiment_id: int = int(_match.group("experiment"))
        self.run_uuid: str = _match.group("runid")
        self.engine = SQLAlchemyClient.engine
        self.ManagedSessionMaker = self._get_managed_session_maker()

    @staticmethod
    def _get_managed_session_maker():
        """
        Get a managed DB Session context manager
        that accounts for thread-local db sessions.
        Automatic closing of connection and handling
        of exceptions.
        """

        @contextmanager
        def make_managed_session():
            session = SQLAlchemyClient.SessionFactory()
            try:
                yield session
            except Exception as e:
                logger.exception("Encountered an error in Managed Session Context")
                session.rollback()
                raise MlflowException(message=e, error_code=INTERNAL_ERROR)
            finally:
                session.close()

        return make_managed_session

    def log_artifact(self, local_file: str, artifact_path: str = None):
        """
        Specifically Implemented in MLManager class,
        does not use plugin
        """
        pass

    def log_artifacts(self, local_dir: str, artifact_path: str = None):
        """
        Specifically Implemented in MLManager class,
        does not use plugin
        """
        pass

    def list_artifacts(self, path: str = None) -> list:
        """
        Get a list of FileInfo objects rendered
        in the MLFlow GUI.
        :return:
        """

        with self.ManagedSessionMaker() as Session:
            # if we render the file as a directory, it prevents
            # users from expanding (which they won't be able to do
            # since objects are stored as BLOBs)
            columns: tuple = ('name', 'size')
            sqlalchemy_query = Session.query(SqlArtifact).options(load_only(*columns)).filter_by(
                run_uuid=self.run_uuid)

            if path:
                sqlalchemy_query = sqlalchemy_query.filter_by(name=path)

            return [
                FileInfo(path=artifact.name, is_dir=False, file_size=artifact.size)
                for artifact in sqlalchemy_query.all()
            ]  # We do NOT want to load the BLOB as it would slow down the GUI

    def download_artifacts(self, artifact_path, dst_path=None):
        """
        Download an artifact file or directory to a local directory if applicable, and return a
        local path for it.
        The caller is responsible for managing the lifecycle of the downloaded artifacts.
        :param artifact_path: Relative source path to the desired artifacts.
        :param dst_path: Absolute path of the local filesystem destination directory to which to
                         download the specified artifacts. This directory must already exist.
                         If unspecified, the artifacts will either be downloaded to a new
                         uniquely-named directory on the local filesystem or will be returned
                         directly in the case of the LocalArtifactRepository.
        :return: Absolute path of the local filesystem location containing the desired artifacts.
        """
        if dst_path is None:
            dst_path = tempfile.mkdtemp()
        dst_path = os.path.abspath(dst_path)
        if not os.path.exists(dst_path):
            raise MlflowException(
                message=(
                    "The destination path for downloaded artifacts does not"
                    " exist! Destination path: {dst_path}".format(dst_path=dst_path)),
                error_code=RESOURCE_DOES_NOT_EXIST)
        elif not os.path.isdir(dst_path):
            raise MlflowException(
                message=(
                    "The destination path for downloaded artifacts must be a directory!"
                    " Destination path: {dst_path}".format(dst_path=dst_path)),
                error_code=INVALID_PARAMETER_VALUE)

        with self.ManagedSessionMaker() as Session:
            columns: tuple = ('binary', 'file_extension', 'name')
            sqlalchemy_query = Session.query(SqlArtifact).options(
                load_only(*columns)).filter_by(run_uuid=self.run_uuid).filter_by(name=artifact_path)

        obj = sqlalchemy_query.one()

        # Get the name of the model for this run if it exists. If that is this current artifact, the file_ext needs to
        # Be .zip because we store a directory
        with self.ManagedSessionMaker() as Session:
            model_name = Session.query(SqlTag.value).\
                filter(SqlTag.run_uuid==self.run_uuid).\
                filter(SqlTag.key=='splice.model_name').\
                first()
            model_name = model_name[0] if model_name else None

        # If the requested artifact is the model, add a .zip extension
        if model_name == obj.name:
            full_file_path_name = f'{dst_path}/{obj.name}.zip'
        else:
            # If the user included the file extension in the file name (or there isn't a file extension) don't append the extension
            full_file_path_name = f'{dst_path}/' + obj.name \
                if not obj.file_extension or splitext(obj.name)[1].lstrip('.') == obj.file_extension  \
                else f'{obj.name}.{obj.file_extension}'

        with open(full_file_path_name, 'wb') as downloaded_file:
            downloaded_file.write(obj.binary)

        return full_file_path_name

    def _download_file(self, remote_file_path, local_path):
        # We implement this in download_artifacts
        pass
