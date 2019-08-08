from mlflow.store.artifact_repo import ArtifactRepository
from mlmanager_lib.database.constants import Database
from sm_mlflow.table_definitions import SqlArtifact, Base
from sqlalchemy import create_engine

from sm_mlflow import add_schemas_to_tables


class SpliceMachineArtifactStore(ArtifactRepository):
    TABLES: tuple = (SqlArtifact,)

    def __init__(self, artifact_uri: str) -> None:
        add_schemas_to_tables(self.TABLES)
        super().__init__(artifact_uri)
        self.db_uri: str = artifact_uri
        self.engine = create_engine(artifact_uri, **Database.engine)

        Base.metadata.bind = self.engine
        Base.metadata.create_all(bind=self.engine, checkfirst=True)

    def log_artifact(self, local_file, artifact_path=None):
        """
        Specifically Implemented in MLManager class,
        does not use plugin
        """
        pass

    def log_artifacts(self, local_dir, artifact_path=None):
        """
        Specifically Implemented in MLManager class,
        does not use plugin
        """
        pass

    def list_artifacts(self, path):
        print(path)

    def _download_file(self, remote_file_path, local_path):
        print(remote_file_path, local_path)
