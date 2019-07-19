"""
Custom MLFlow Tracking Store for Splice Machine
DB
"""
from sqlalchemy import inspect as reflection, create_engine
from sqlalchemy.orm import sessionmaker

from mlflow.store.sqlalchemy_store import SqlAlchemyStore
from mlflow.store.dbmodels.models import Base, SqlMetric, SqlParam, SqlTag, SqlRun, SqlExperiment
from mlflow.tracking.utils import _is_local_uri
from mlflow.utils.file_utils import mkdir, local_file_uri_to_path

from mlmanager_lib.constants import Database

from sm_mlflow.utils import SpliceMachineImpl

# ^ we need this in our global namespace so that alembic will be able to find our dialect during
# DB migrations

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


def add_schemas_to_tables():
    """
    These tables should be under the MLMANAGER
    schema in Splicemachine, not under the default
    schema
    """
    tables: list = [SqlExperiment, SqlRun, SqlTag, SqlParam, SqlMetric]

    for table in tables:
        table.__table_args__.update({'schema': Database.database_schema})


class SpliceMachineTrackingStore(SqlAlchemyStore):
    def __init__(self, store_uri: str = None, artifact_uri: str = None) -> None:
        """
        Create a database backed store.

        :param store_uri: (str) The SQLAlchemy database URI string to connect to the database.
        :param artifact_uri: (str) Path/URI to location suitable for large data (such as a blob
                                      store object, DBFS path, or shared NFS file system).
        """
        super(SqlAlchemyStore, self).__init__()

        add_schemas_to_tables()

        self.db_uri: str = store_uri
        self.db_type: str = 'splicemachinesa'
        self.artifact_root_uri: str = artifact_uri
        self.engine = create_engine(store_uri, **Database.engine_options)

        insp: reflection = reflection(self.engine)

        expected_tables: set = {
            SqlExperiment.__tablename__,
            SqlRun.__tablename__,
            SqlMetric.__tablename__,
            SqlParam.__tablename__, SqlTag.__tablename__
        }

        if len(expected_tables & set(insp.get_table_names())) == 0:
            SqlAlchemyStore._initialize_tables(self.engine)

        Base.metadata.bind = self.engine
        SessionMaker: sessionmaker = sessionmaker(bind=self.engine)
        self.ManagedSessionMaker = self._get_managed_session_maker(SessionMaker)
        SqlAlchemyStore._verify_schema(self.engine)

        if _is_local_uri(artifact_uri):
            mkdir(local_file_uri_to_path(artifact_uri))

        if len(self.list_experiments()) == 0:
            with self.ManagedSessionMaker() as session:
                self._create_default_experiment(session)

    def _get_artifact_location(self, experiment_id: int) -> str:
        """
        Get the default artifact location (that is actually quoted)
        because Python 2.7 doesn't support Python 3 strings. This
        causes errors upon execution of the insert command for the
        default experiment

        :param experiment_id: (int) the experiment id to retrieve the artifact
            URI for
        :return: (str) the default artifact location for the given experiment
        """
        return str(super(SpliceMachineTrackingStore, self)._get_artifact_location(experiment_id))
