"""
Custom MLFlow Tracking Store for Splice Machine
DB
"""
import uuid
import posixpath

from mlflow.exceptions import MlflowException

from sqlalchemy import inspect as reflection, create_engine
from sqlalchemy.orm import sessionmaker

from mlflow.store.sqlalchemy_store import SqlAlchemyStore
from mlflow.store.dbmodels.models import Base, SqlMetric, SqlParam, SqlTag, SqlRun, SqlExperiment
from mlflow.entities import RunStatus, SourceType, Experiment
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE, RESOURCE_ALREADY_EXISTS, \
    INVALID_STATE, RESOURCE_DOES_NOT_EXIST, INTERNAL_ERROR
from mlflow.tracking.utils import _is_local_uri
from mlflow.utils.file_utils import mkdir, local_file_uri_to_path
from mlflow.entities.lifecycle_stage import LifecycleStage

from mlmanager_lib.database.constants import Database

from sm_mlflow.alembic_support import SpliceMachineImpl

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
        table.__table_args__ = table.__table_args__ + ({'schema': Database.database_schema},)


class SpliceMachineTrackingStore(SqlAlchemyStore):
    def __init__(self, store_uri: str = None, artifact_uri: str = None) -> None:
        """
        Create a database backed store.

        :param store_uri: (str) The SQLAlchemy database URI string to connect to the database.
        :param artifact_uri: (str) Path/URI to location suitable for large data (such as a blob
                                      store object, DBFS path, or shared NFS file system).
        """
        add_schemas_to_tables()
        super(SqlAlchemyStore, self).__init__()

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
        return str(super()._get_artifact_location(experiment_id))

    def create_run(self, experiment_id, user_id, start_time, tags):
        """
        We override this method so that MLFlow doesn't log users/names
        based on the current logged in user to the system/none-- rather,
        it bases it off of tags.
        """
        with self.ManagedSessionMaker() as session:
            experiment = self.get_experiment(experiment_id)

            if experiment.lifecycle_stage != LifecycleStage.ACTIVE:
                raise MlflowException('Experiment id={} must be active'.format(experiment_id),
                                      INVALID_STATE)

            run_id = uuid.uuid4().hex
            artifact_location = posixpath.join(experiment.artifact_location, run_id,
                                               SqlAlchemyStore.ARTIFACTS_FOLDER_NAME)
            tags_dict = {}
            for tag in tags:
                tags_dict[tag.key] = tag.value

            if not "user" in tags_dict:
                raise MlflowException("Tag 'user' must be specified for governance")

            run_name = tags_dict.get("run_name", "")
            run = SqlRun(name=run_name, artifact_uri=artifact_location, run_uuid=run_id,
                         experiment_id=experiment_id,
                         source_type=SourceType.to_string(SourceType.UNKNOWN),
                         source_name=run_name, entry_point_name="",
                         user_id=tags_dict['user'], status=RunStatus.to_string(RunStatus.RUNNING),
                         start_time=start_time, end_time=None,
                         source_version="", lifecycle_stage=LifecycleStage.ACTIVE)

            run.tags = [SqlTag(key=key, value=value) for key, value in tags_dict.items()]
            self._save_to_db(objs=run, session=session)

            return run.to_mlflow_entity()
