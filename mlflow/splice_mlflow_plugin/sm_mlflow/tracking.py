"""
Custom MLFlow Tracking Store for Splice Machine
DB
"""
import posixpath
import uuid

from mlflow.entities import RunStatus, SourceType, LifecycleStage, ViewType
from mlflow.entities.run import Run
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import INVALID_STATE, INVALID_PARAMETER_VALUE
from mlflow.protos.databricks_pb2 import RESOURCE_DOES_NOT_EXIST
from mlflow.store.db.utils import _upgrade_db, _get_managed_session_maker, _verify_schema, _initialize_tables
from mlflow.store.tracking import SEARCH_MAX_RESULTS_THRESHOLD
from mlflow.store.tracking.dbmodels.initial_models import Base as InitialBase, SqlMetric as InitialSqlMetric, \
    SqlParam as InitialSqlParam, SqlTag as InitialSqlTag, SqlRun as InitialSqlRun, \
    SqlExperiment as InitialSqlExperiment  # pre-migration sqlalchemy tables
from mlflow.store.tracking.dbmodels.models import SqlRun, SqlTag, SqlExperiment, SqlLatestMetric, SqlParam
from mlflow.store.db.base_sql_model import Base
from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore, _get_sqlalchemy_filter_clauses, \
    _get_attributes_filtering_clauses#, _get_orderby_clauses
from mlflow.utils.search_utils import SearchUtils
from mlmanager_lib.database.mlflow_models import SqlArtifact, Model
from mlmanager_lib.database.models import ENGINE
from mlmanager_lib.logger.logging_config import logging
from sm_mlflow.alembic_support import SpliceMachineImpl
from sqlalchemy import inspect as peer_into_splice_db
import sqlalchemy.sql.expression as sql
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.sqltypes import Integer
import re

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


def _get_orderby_clauses_test(order_by, session):
    """Sorts a set of runs based on their natural ordering and an overriding set of order_bys.
    Runs are naturally ordered first by start time descending, then by run id for tie-breaking.
    """

    clauses = []
    ordering_joins = []
    clause_id = 0
    # contrary to filters, it is not easily feasible to separately handle sorting
    # on attributes and on joined tables as we must keep all clauses in the same order
    if order_by:
        for order_by_clause in order_by:
            clause_id += 1
            (key_type, key, ascending) = SearchUtils.parse_order_by(order_by_clause)
            if SearchUtils.is_attribute(key_type, '='):
                order_value = getattr(SqlRun, SqlRun.get_attribute_name(key))
            else:
                if SearchUtils.is_metric(key_type, '='):  # any valid comparator
                    entity = SqlLatestMetric
                elif SearchUtils.is_tag(key_type, '='):
                    entity = SqlTag
                elif SearchUtils.is_param(key_type, '='):
                    entity = SqlParam
                else:
                    raise MlflowException("Invalid identifier type '%s'" % key_type,
                                          error_code=INVALID_PARAMETER_VALUE)

                # build a subquery first because we will join it in the main request so that the
                # metric we want to sort on is available when we apply the sorting clause
                subquery = session \
                    .query(entity) \
                    .filter(entity.key == key) \
                    .subquery()

                ordering_joins.append(subquery)
                order_value = subquery.c.value

            # sqlite does not support NULLS LAST expression, so we sort first by
            # presence of the field (and is_nan for metrics), then by actual value
            # As the subqueries are created independently and used later in the
            # same main query, the CASE WHEN columns need to have unique names to
            # avoid ambiguity
            if SearchUtils.is_metric(key_type, '='):
                clauses.append(sql.case([
                    (subquery.c.is_nan==True,1),
                    (order_value.is_(None), 1)
                ], else_=0).cast(Integer).label('clause_%s' % clause_id))
            else:  # other entities do not have an 'is_nan' field
                clauses.append(sql.case([(order_value.is_(None), 1)], else_=0).cast(Integer)
                               .label('clause_%s' % clause_id))

            if ascending:
                clauses.append(order_value)
            else:
                clauses.append(order_value.desc())

    clauses.append(SqlRun.start_time.desc())
    clauses.append(SqlRun.run_uuid)
    return clauses, ordering_joins




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
            import traceback
            traceback.print_exc()

    def fix_CASE_clause(self, sql: str) -> str:
        """
        Splice Machine has 2 limitations here:
        1. We don't support ? in all CASE statement returns: ERROR 42X87:
            At least one result expression (THEN or ELSE) of the 'conditional' expression must not be a '?
        2. We don't support IS as a boolean operator (CASE WHEN x IS 5)
        Fix for 1. Add a cast to the ELSE
        Fix for 2. Check for IS or IS NOT
        :param sql: (str) the SQL
        :return: (str) the fixed SQL
        """
        reg = 'CASE(.*)END'
        result = re.search(reg,sql)
        CASE_SQL = result.group(1)

        indexes = [m.start() for m in re.finditer('THEN', CASE_SQL)] + [re.search('ELSE',CASE_SQL).start()]
        needs_cast = True
        for i in indexes:
            # Check for all "THEN ?" or "ELSE ?"
            if CASE_SQL[i+5] != '?':
                needs_cast = False
                break
        if needs_cast:
            new_CASE_SQL = CASE_SQL.replace('ELSE ?', 'ELSE CAST(? as INT)')


        # Replace IS <> with = <>
        # We rerun the search every time because after each replacement, the length of the string has changed
        is_clause = 'IS [^NULL]'
        x = re.search(is_clause, new_CASE_SQL)
        while x:
            ind = x.start()
            new_CASE_SQL = new_CASE_SQL[:ind] + '=' + new_CASE_SQL[ind+2:] # skip IS
            x = re.search(is_clause, new_CASE_SQL)

        # Replace IS NOT <> with = <>
        # We rerun the search every time because after each replacement, the length of the string has changed
        is_not_clause = 'IS NOT [^NULL]'
        x = re.search(is_not_clause, new_CASE_SQL)
        while x:
            ind = x.start()
            new_CASE_SQL = new_CASE_SQL[:ind] + '!=' + new_CASE_SQL[ind+6:] # skip IS NOT
            x = re.search(is_clause, new_CASE_SQL)

        fixed_sql = sql.replace(CASE_SQL,new_CASE_SQL)
        return fixed_sql

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
            # ``run.to_mlflow_entity()``, so eager loading helps avoid additional database queries
            # that are otherwise executed at attribute access time under a lazy loading model.
            parsed_filters = SearchUtils.parse_search_filter(filter_string)
            parsed_orderby, sorting_joins = _get_orderby_clauses_test(order_by, session)

            query = session.query(SqlRun)
            for j in _get_sqlalchemy_filter_clauses(parsed_filters, session):
                query = query.join(j)
            # using an outer join is necessary here because we want to be able to sort
            # on a column (tag, metric or param) without removing the lines that
            # do not have a value for this column (which is what inner join would do)
            for j in sorting_joins:
                query = query.outerjoin(j)

            offset = SearchUtils.parse_start_offset_from_page_token(page_token)
            # queried_runs = query.distinct() \
            #     .options(*self._get_eager_run_query_options()) \
            #     .filter(
            #         SqlRun.experiment_id.in_([int(i) for i in experiment_ids]),
            #         SqlRun.lifecycle_stage.in_(stages),
            #         *_get_attributes_filtering_clauses(parsed_filters)) \
            #     .order_by(*parsed_orderby) \
            #     .offset(offset).limit(max_results)
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
                try: #FIXME: We need a smarter comparison
                    x = re.sub("[.'\[\]]","",str(order_by)).split('`')
                    reverse = x[-1].strip() != 'ASC'
                    typ = x[0] # metrics or params
                    col = x[1] # the metric/param to order by

                    # Determine if the value is an number or string
                    for i,j in enumerate(runs):
                        if(col in j.to_dictionary()['data'][typ]):
                            ind = i
                            break
                    compare_val = runs[ind].to_dictionary()['data'][typ][col]
                    default_val = 'z' if str(compare_val) == compare_val else 0 # in case a run doesn't have that value
                    runs = sorted(runs, key=lambda i:i.to_dictionary()['data'][typ].get(col,default_val), reverse=reverse)
                except: # If this fails just don't order. We shouldn't throw up
                    import traceback
                    traceback.print_exc()
                    raise Exception(str(x))
                    #pass
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
