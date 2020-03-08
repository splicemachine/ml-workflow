"""
Splice Machine Custom
Artifact Store that uses
BLOBS
"""
from contextlib import contextmanager

from mlflow.entities.file_info import FileInfo
from mlflow.exceptions import MlflowException, INTERNAL_ERROR
from mlflow.store.artifact.artifact_repo import ArtifactRepository
from mlmanager_lib.database.constants import Extraction
from mlmanager_lib.database.mlflow_models import Base, SqlArtifact
from mlmanager_lib.database.models import ENGINE, SessionFactory
from mlmanager_lib.logger.logging_config import logging
from sqlalchemy.orm import load_only

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

LOGGER = logging.getLogger(__name__)


class SpliceMachineArtifactStore(ArtifactRepository):

    def __init__(self, artifact_uri: str) -> None:
        super().__init__(artifact_uri)

        _match = Extraction.ARTIFACT_PATH_REGEX.match(self.artifact_uri)

        if not _match:
            raise MlflowException("The Artifact Path Specified is Invalid")

        # extract data from regex
        self.experiment_id: int = int(_match.group(1))
        self.run_uuid: str = _match.group(2)
        self.ManagedSessionMaker = self._get_managed_session_maker()

        Base.metadata.bind = ENGINE

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
            session = SessionFactory()
            try:
                yield session
            except Exception as e:
                LOGGER.exception("Encountered an error in Managed Session Context")
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
            sqlalchemy_query = Session.query(
                SqlArtifact).options(load_only(*columns)).filter_by(
                run_uuid=self.run_uuid)

            if path:
                sqlalchemy_query = sqlalchemy_query.filter_by(name=path)

            return [
                FileInfo(path=artifact.name, is_dir=False, file_size=artifact.size)
                for artifact in sqlalchemy_query.all()
            ]  # We do NOT want to load the BLOB as it would slow down the GUI

    def _download_file(self, remote_file_path, local_path):
        print(remote_file_path, local_path)
