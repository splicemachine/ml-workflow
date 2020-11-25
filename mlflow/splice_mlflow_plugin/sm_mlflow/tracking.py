"""
Custom MLFlow Tracking Store for Splice Machine
DB
"""
import posixpath
import re
import uuid

from sm_mlflow.alembic_support import SpliceMachineImpl
from sm_mlflow.db_utils import get_orderby_clauses_test
from sqlalchemy import inspect as peer_into_splice_db
from sqlalchemy.exc import IntegrityError

from mlflow.entities import LifecycleStage, RunStatus, SourceType, ViewType
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import (INVALID_PARAMETER_VALUE,
                                          INVALID_STATE,
                                          RESOURCE_DOES_NOT_EXIST)
from mlflow.store.db.base_sql_model import Base
from mlflow.store.db.utils import (_get_managed_session_maker,
                                   _initialize_tables, _upgrade_db,
                                   _verify_schema)
from mlflow.store.tracking import SEARCH_MAX_RESULTS_THRESHOLD
from mlflow.store.tracking.dbmodels.initial_models import Base as InitialBase
from mlflow.store.tracking.dbmodels.initial_models import \
    SqlExperiment as InitialSqlExperiment
from mlflow.store.tracking.dbmodels.initial_models import \
    SqlMetric as InitialSqlMetric
from mlflow.store.tracking.dbmodels.initial_models import \
    SqlParam as InitialSqlParam
from mlflow.store.tracking.dbmodels.initial_models import \
    SqlRun as InitialSqlRun
from mlflow.store.tracking.dbmodels.initial_models import \
    SqlTag as InitialSqlTag
from mlflow.store.tracking.dbmodels.models import (SqlExperiment,
                                                   SqlLatestMetric, SqlRun,
                                                   SqlTag)
from mlflow.store.tracking.sqlalchemy_store import (
    SqlAlchemyStore, _get_attributes_filtering_clauses,
    _get_sqlalchemy_filter_clauses)
from mlflow.utils.search_utils import SearchUtils
from shared.logger.logging_config import logger
from shared.models.mlflow_models import (DatabaseDeployedMetadata, SqlArtifact,
                                         SysTables, SysTriggers, SysUsers,
                                         live_model_status_view)
from shared.services.database import SQLAlchemyClient, DatabaseSQL

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


class SpliceMachineTrackingStore(SqlAlchemyStore):
    MLFLOW_PROVIDED_TABLES: tuple = (
        InitialSqlExperiment, InitialSqlRun, InitialSqlTag, InitialSqlMetric, InitialSqlParam
    )  # alembic migrations will be applied to these initial tables

    MLFLOW_CUSTOM_TABLES: tuple = (
        SqlArtifact, SysUsers, SysTriggers, SysTables, DatabaseDeployedMetadata
    )

    TABLES: tuple = MLFLOW_PROVIDED_TABLES + MLFLOW_CUSTOM_TABLES

    def __init__(self, store_uri: str = None, artifact_uri: str = None) -> None:
        """
        Create a database.py backed store.

        :param store_uri: (str) The SQLAlchemy database.py URI string to connect to the database.py.
        :param artifact_uri: (str) Path/URI to location suitable for large data (such as a blob
                                      store object, DBFS path, or structures NFS file system).
        """
        try:
            self.db_type: str = 'splicemachinesa'
            self.artifact_root_uri: str = artifact_uri
            self.engine = SQLAlchemyClient.engine
            self.inspector = peer_into_splice_db(self.engine)

            expected_tables = {table.__tablename__ for table in self.TABLES}

            logger.info("Initializing DB...")
            if len(expected_tables & set(self.inspector.get_table_names())) == 0:
                _initialize_tables(SQLAlchemyClient.engine)
            self._initialize_tables()
            logger.info("Finished Initialization...")

            Base.metadata.bind = self.engine
            self.ManagedSessionMaker = _get_managed_session_maker(SQLAlchemyClient.SessionMaker)
            _verify_schema(SQLAlchemyClient.engine)

            if len(self.list_experiments()) == 0:
                with self.ManagedSessionMaker() as session:
                    self._create_default_experiment(session)
        except Exception:
            logger.exception("Failed to initialize Tracking Store:")
            raise

    def _initialize_tables(self):
        """
        This function creates MLFlow tables
        that require alembic revisions first and
        applies alembic revisions to those tables.
        Then, it creates all tables (that we added)
        that don't require alembic revisions
        """
        InitialBase.metadata.create_all(
            self.engine,
            tables=[table.__table__ for table in self.MLFLOW_PROVIDED_TABLES]
        )  # create older versions of alembic tables to apply upgrades

        _upgrade_db(self.engine)  # apply alembic revisions serially

        logger.info("Creating Non-Alembic tables...")
        Base.metadata.create_all(
            self.engine,
            tables=[table.__table__ for table in self.MLFLOW_CUSTOM_TABLES],
            checkfirst=True
        )
        logger.info("Creating Non-Alembic Views")

        if 'live_model_status' not in self.inspector.get_view_names():
            logger.warning("Could not locate Live Model Status View... Creating")
            with SQLAlchemyClient.engine.begin() as transaction:
                transaction.execute(live_model_status_view)

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

                logger.info(f"Creating Run: {run}")
                run.tags = [SqlTag(key=key, value=value) for key, value in tags_dict.items()]
                self._save_to_db(objs=run, session=session)

                return run.to_mlflow_entity()
            except IntegrityError:  # Hash Collision (very low probability)
                logger.exception(
                    f"Violated PK Constraint for Run ID: Hash Collision. Regenerating ID..."
                )
                session.rollback()
                self.create_run(experiment_id, user_id, start_time, tags)

    def _get_experiment(self, session, experiment_id, view_type, eager=False):
        """
        :param eager: If ``True``, eagerly loads the experiments's tags. If ``False``, these tags
                      are not eagerly loaded and will be loaded if/when their corresponding
                      object properties are accessed from the resulting ``SqlExperiment`` object.
        """
        experiment_id = experiment_id or SqlAlchemyStore.DEFAULT_EXPERIMENT_ID
        stages = LifecycleStage.view_type_to_stages(view_type)
        query_options = self._get_eager_experiment_query_options() if eager else []

        print('query options', query_options, type(query_options))
        experiment = session \
            .query(SqlExperiment) \
            .options(*query_options) \
            .filter(
            SqlExperiment.experiment_id == int(experiment_id),
            SqlExperiment.lifecycle_stage.in_(stages)) \
            .one_or_none()

        if experiment is None:
            raise MlflowException("No darn Experiment with id={} exists".format(experiment_id),
                                  RESOURCE_DOES_NOT_EXIST)

        return experiment

    def get_experiment(self, experiment_id):
        try:
            with self.ManagedSessionMaker() as session:
                return self._get_experiment(
                    session, experiment_id, ViewType.ALL, eager=True).to_mlflow_entity()
        except:
            logger.exception(f"Unable to retrieve experiment #{experiment_id}")

    def _search_runs(self, experiment_ids, filter_string, run_view_type, max_results, order_by,
                     page_token):

        def compute_next_token(current_size):
            next_token = None
            if max_results == current_size:
                final_offset = offset + max_results
                next_token = SearchUtils.create_page_token(final_offset)

            return next_token

        if max_results > SEARCH_MAX_RESULTS_THRESHOLD:
            raise MlflowException("Invalid value for request parameter max_results. It must be at "
                                  "most {}, but got value {}".format(SEARCH_MAX_RESULTS_THRESHOLD,
                                                                     max_results),
                                  INVALID_PARAMETER_VALUE)

        stages = set(LifecycleStage.view_type_to_stages(run_view_type))

        with self.ManagedSessionMaker() as session:
            # Fetch the appropriate runs and eagerly load their summary metrics, params, and
            # tags. These run attributes are referenced during the invocation of
            # ``run.to_mlflow_entity()``, so eager loading helps avoid additional database.py queries
            # that are otherwise executed at attribute access time under a lazy loading model.
            parsed_filters = SearchUtils.parse_search_filter(filter_string)
            parsed_orderby, sorting_joins = get_orderby_clauses_test(order_by, session)

            query = session.query(SqlRun)
            for j in _get_sqlalchemy_filter_clauses(parsed_filters, session):
                query = query.join(j)
            # using an outer join is necessary here because we want to be able to sort
            # on a column (tag, metric or param) without removing the lines that
            # do not have a value for this column (which is what inner join would do)
            for j in sorting_joins:
                query = query.outerjoin(j)

            offset = SearchUtils.parse_start_offset_from_page_token(page_token)

            queried_runs = query.distinct() \
                .options(*self._get_eager_run_query_options()) \
                .filter(
                SqlRun.experiment_id.in_([int(i) for i in experiment_ids]),
                SqlRun.lifecycle_stage.in_(stages),
                *_get_attributes_filtering_clauses(parsed_filters)) \
                .offset(offset).limit(max_results).all()

            # Splice Machine has 2 limitations here:
            # 1. We don't support ? in all CASE statement returns: ERROR 42X87:
            #    At least one result expression (THEN or ELSE) of the 'conditional' expression must not be a '?
            # 2. We don't support IS as a boolean operator (CASE WHEN x IS 5)

            runs = [run.to_mlflow_entity() for run in queried_runs]

            # Try to order them
            if order_by:
                try:  # FIXME: We need a smarter comparison
                    x = re.sub("[.'\[\]]", "", str(order_by)).split('`')
                    # Start time is funky
                    if 'start_time' in x[0]:
                        col = 'start_time'
                        default_val = 0
                        reverse = 'ASC' not in x[0]  # If ASC, keep sorted order
                        runs = sorted(runs, key=lambda i: i.to_dictionary()['info'].get(col, default_val),
                                      reverse=reverse)
                    else:
                        reverse = x[-1].strip() != 'ASC'
                        typ = x[0]  # metrics or params or tags
                        col = x[1]  # the metric/param to order by
                        # Fix parsing for mlflow columns
                        if 'mlflow' in col and typ == 'tags':
                            col = col[:6] + '.' + col[6:]
                            if col == 'mlflow.sourcegitcommit':
                                col = 'mlflow.source.git.commit'
                            elif col == 'mlflow.sourcename':
                                col = 'mlflow.source.name'

                        # Determine if the value is an number or string
                        for i, j in enumerate(runs):
                            if (col in j.to_dictionary()['data'][typ]):
                                ind = i
                                break
                        compare_val = runs[ind].to_dictionary()['data'][typ][col]
                        default_val = 'z' if str(
                            compare_val) == compare_val else 0  # in case a run doesn't have that value
                        runs = sorted(runs, key=lambda i: i.to_dictionary()['data'][typ].get(col, default_val),
                                      reverse=reverse)
                except:  # If this fails just don't order. We shouldn't throw up
                    import traceback
                    traceback.print_exc()
                    raise Exception(str(x))
                    # pass
            next_page_token = compute_next_token(len(runs))

        return runs, next_page_token

    @staticmethod
    def _update_latest_metric_if_necessary(logged_metric, session):
        def _compare_metrics(metric_a, metric_b):
            """
            Override base function to remove locking because Splice Machine doesn't need to lock
            https://doc.splicemachine.com/developers_fundamentals_transactions.html#Snapshot
            :return: True if ``metric_a`` is strictly more recent than ``metric_b``, as determined
                     by ``step``, ``timestamp``, and ``value``. False otherwise.
            """
            return (metric_a.step, metric_a.timestamp, metric_a.value) > \
                   (metric_b.step, metric_b.timestamp, metric_b.value)

        # Fetch the latest metric value corresponding to the specified run_id and metric key
        latest_metric = session \
            .query(SqlLatestMetric) \
            .filter(
            SqlLatestMetric.run_uuid == logged_metric.run_uuid,
            SqlLatestMetric.key == logged_metric.key) \
            .one_or_none()

        if latest_metric is None or _compare_metrics(logged_metric, latest_metric):
            session.merge(
                SqlLatestMetric(
                    run_uuid=logged_metric.run_uuid, key=logged_metric.key,
                    value=logged_metric.value, timestamp=logged_metric.timestamp,
                    step=logged_metric.step, is_nan=logged_metric.is_nan))


if __name__ == "__main__":
    print(SpliceMachineImpl)

