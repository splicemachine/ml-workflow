"""
Custom MLFlow Tracking Store for Splice Machine
DB
"""
import posixpath
import uuid

from mlflow.entities import RunStatus, SourceType, LifecycleStage, ViewType
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import INVALID_STATE
import mlflow.protos.databricks_pb2 as databricks_pb2
from mlflow.utils.validation import _validate_experiment_id
from mlflow.utils.file_utils import read_yaml
from mlflow.store.db.utils import _upgrade_db, _get_managed_session_maker, _verify_schema, _initialize_tables
from mlflow.store.tracking.dbmodels.initial_models import Base as InitialBase, SqlMetric as InitialSqlMetric, \
    SqlParam as InitialSqlParam, SqlTag as InitialSqlTag, SqlRun as InitialSqlRun, \
    SqlExperiment as InitialSqlExperiment  # pre-migration sqlalchemy tables
from mlflow.store.tracking.dbmodels.models import SqlRun, SqlTag, SqlExperiment
from mlflow.store.db.base_sql_model import Base
from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore
from mlmanager_lib.database.mlflow_models import SqlArtifact
from mlmanager_lib.database.models import ENGINE
from mlmanager_lib.logger.logging_config import logging
from sm_mlflow.alembic_support import SpliceMachineImpl
from sqlalchemy import inspect as peer_into_splice_db
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

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

LOGGER = logging.getLogger(__name__)


class SpliceMachineTrackingStore(SqlAlchemyStore):
    ALEMBIC_TABLES: tuple = (
        InitialSqlExperiment, InitialSqlRun, InitialSqlTag, InitialSqlMetric, InitialSqlParam
    )  # alembic migrations will be applied to these initial tables

    NON_ALEMBIC_TABLES: tuple = (SqlArtifact,)
    TABLES: tuple = ALEMBIC_TABLES + NON_ALEMBIC_TABLES

    def __init__(self, store_uri: str = None, artifact_uri: str = None) -> None:
        """
        Create a database backed store.

        :param store_uri: (str) The SQLAlchemy database URI string to connect to the database.
        :param artifact_uri: (str) Path/URI to location suitable for large data (such as a blob
                                      store object, DBFS path, or shared NFS file system).
        """
        # super(SqlAlchemyStore, self).__init__()

        self.db_type: str = 'splicemachinesa'
        self.artifact_root_uri: str = artifact_uri
        self.engine = ENGINE

        expected_tables = {table.__tablename__ for table in self.TABLES}
        inspector = peer_into_splice_db(self.engine)

        if len(expected_tables & set(inspector.get_table_names(schema='MLMANAGER'))) == 0:
            _initialize_tables(self.engine)
        self._initialize_tables()

        Base.metadata.bind = self.engine
        SessionMaker: sessionmaker = sessionmaker(bind=ENGINE)
        self.ManagedSessionMaker = _get_managed_session_maker(SessionMaker)
        _verify_schema(ENGINE)
        if len(self.list_experiments()) == 0:
            with self.ManagedSessionMaker() as session:
                self._create_default_experiment(session)

    def _initialize_tables(self):
        """
        This function creates MLFlow tables
        that require alembic revisions first and
        applies alembic revisions to those tables.
        Then, it creates all tables (that we added)
        that don't require alembic revisions
        """
        # LOGGER.info("Creating initial Alembic MLflow database tables...")
        InitialBase.metadata.create_all(
            self.engine,
            tables=[table.__table__ for table in self.ALEMBIC_TABLES]
        )  # create older versions of alembic tables to apply upgrades

        engine_url = str(self.engine.url)
        _upgrade_db(engine_url)  # apply alembic revisions serially

        LOGGER.info("Creating Non-Alembic database tables...")
        Base.metadata.create_all(
            self.engine,
            tables=[table.__table__ for table in self.NON_ALEMBIC_TABLES]
        )

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

    def create_run(self, experiment_id, user_id, start_time, tags):
        """
        We override this method so that MLFlow doesn't log users
        based on the current logged in user to the system-- rather,
        it bases it off of tags.
        """

        with self.ManagedSessionMaker() as session:
            try:
                experiment = self.get_experiment(experiment_id)

                if experiment.lifecycle_stage != LifecycleStage.ACTIVE:
                    raise MlflowException('Experiment id={} must be active'.format(experiment_id),
                                          INVALID_STATE)
                run_id: str = uuid.uuid4().hex[:12]
                artifact_location: str = posixpath.join(experiment.artifact_location, run_id,
                                                        SqlAlchemyStore.ARTIFACTS_FOLDER_NAME)
                tags_dict: dict = {}
                for tag in tags:
                    tags_dict[tag.key] = tag.value

                if "mlflow.user" not in tags_dict:
                    raise MlflowException("Tag 'mlflow.user' must be specified for governance")

                tags_dict['mlflow.runName'] = run_id

                run: SqlRun = SqlRun(name="", artifact_uri=artifact_location, run_uuid=run_id,
                                     experiment_id=experiment_id,
                                     source_type=SourceType.to_string(SourceType.UNKNOWN),
                                     source_name="MLManager", entry_point_name="",
                                     user_id=tags_dict['mlflow.user'],
                                     status=RunStatus.to_string(RunStatus.RUNNING),
                                     start_time=start_time, end_time=None,
                                     source_version=tags_dict.get('version', ''),
                                     lifecycle_stage=LifecycleStage.ACTIVE)

                LOGGER.info(f"Creating Run: {run}")
                run.tags: list = [SqlTag(key=key, value=value) for key, value in tags_dict.items()]
                self._save_to_db(objs=run, session=session)

                return run.to_mlflow_entity()
            except IntegrityError:  # Hash Collision (very low probability)
                LOGGER.exception(
                    f"Violated PK Constraint for Run ID: Hash Collision. Regenerating ID..."
                )
                session.rollback()
                self.create_run(experiment_id, user_id, start_time, tags)



    def _get_experiment(self, experiment_id, view_type=ViewType.ALL):
        super(SpliceMachineTrackingStore,self)._check_root_dir()
        _validate_experiment_id(experiment_id)
        experiment_dir = super(SpliceMachineTrackingStore,self)._get_experiment_path(experiment_id, view_type)
        if experiment_dir is None:
            raise MlflowException("Could not find experiment with ID %s" % experiment_id,
                                  databricks_pb2.RESOURCE_DOES_NOT_EXIST)
        meta = read_yaml(experiment_dir, super(SpliceMachineTrackingStore,self).FileStore.META_DATA_FILE_NAME)
        if experiment_dir.startswith(super(SpliceMachineTrackingStore,self).trash_folder):
            meta['lifecycle_stage'] = LifecycleStage.DELETED
        else:
            meta['lifecycle_stage'] = LifecycleStage.ACTIVE
        meta['tags'] = super(SpliceMachineTrackingStore,self).get_all_experiment_tags(experiment_id)
        experiment = super(SpliceMachineTrackingStore,self)._read_persisted_experiment_dict(meta)
        if experiment_id != experiment.experiment_id:
            logging.warning("Experiment ID mismatch for exp %s. ID recorded as '%s' in meta data. "
                            "Experiment will be ignored.",
                            experiment_id, experiment.experiment_id, exc_info=True)
            return None
        return experiment

    def get_experiment(self, experiment_id):
        """
        Fetch the experiment.
        Note: This API will search for active as well as deleted experiments.
        :param experiment_id: Integer id for the experiment
        :return: A single Experiment object if it exists, otherwise raises an Exception.
        """
        experiment_id = self.FileStore.DEFAULT_EXPERIMENT_ID if experiment_id is None else experiment_id
        experiment = self._get_experiment(experiment_id)
        if experiment is None:
            raise MlflowException("Experiment '%s' does not exist." % experiment_id,
                                  databricks_pb2.RESOURCE_DOES_NOT_EXIST)
        return experiment

if __name__ == "__main__":
    print(SpliceMachineImpl)
