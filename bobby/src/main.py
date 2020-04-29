"""
This module has a Master, which polls Splice Machine
for new jobs and dispatches them to Workers for execution
(in threads). This execution happens in parallel.
"""
from os import environ as env_vars
from time import sleep as wait

from flask import Flask
from handlers.modifier_handlers import EnableServiceHandler, DisableServiceHandler
from handlers.run_handlers import SageMakerDeploymentHandler, AzureDeploymentHandler
from mlmanager_lib import CloudEnvironments, CloudEnvironment
from mlmanager_lib.database.constants import JobStatuses
from mlmanager_lib.database.handlers import KnownHandlers, HandlerNames, populate_handlers
from mlmanager_lib.database.models import Job, SessionFactory, execute_sql
from mlmanager_lib.logger.logging_config import logging
from mlmanager_lib.worker.ledger import JobLedger
from py4j.java_gateway import java_import
from pyspark import SparkConf, SparkContext
from workerpool import Job as ThreadedTask, WorkerPool
from pysparkling import *
from retrying import retry
from traceback import format_exc

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

APP: Flask = Flask(__name__)
LOGGER = logging.getLogger(__name__)
LOGGER.warning(f"Using Logging Level {logging.getLevelName(LOGGER.getEffectiveLevel())}")

# Jobs
POLL_INTERVAL: int = 5  # check for new jobs every 2 seconds
LEDGER_MAX_SIZE: int = int(env_vars['WORKER_THREADS'] * 2)  # how many previous jobs to account for

# Spark
SPARK_SCHEDULING_FILE: str = "configuration/fair_scheduling.xml"

# Thread Local Database Session
Session = SessionFactory()

# We only need spark context for modifiable handlers
RUN_HANDLERS: tuple = KnownHandlers.get_modifiable()

# Initializes workerpool on construction of object
WORKER_POOL: WorkerPool = WorkerPool(size=30)
LEDGER: JobLedger = JobLedger(LEDGER_MAX_SIZE)

ID_COL: str = "id"  # column name for id in Jobs
TIMESTAMP_COL: str = "timestamp"
STATUS_COL: str = "status"
HANDLER_COL: str = "handler_name"
POLL_SQL_QUERY: str = \
    f"""
    SELECT {ID_COL}, {HANDLER_COL} FROM {Job.__tablename__}
    WHERE {STATUS_COL}='{JobStatuses.pending}'
    ORDER BY "{TIMESTAMP_COL}"
    """

def create_spark() -> SparkContext:
    """
    Create a Global Spark Context
    that runs in the FAIR
    scheduling mode. This means that
    it shares resources across threads.
    We need a Spark Context to create
    the directory structure from a
    deserialized PipelineModel (formerly
    a byte stream in the database)

    :return: (SparkContext) a Global Spark
        Context
    """

    spark_config: SparkConf = SparkConf(). \
        setAppName(env_vars['TASK_NAME']). \
        setMaster('local[*]'). \
        set('spark.scheduler.mode', 'FAIR'). \
        set('spark.scheduler.allocation.file', f'{env_vars["SRC_HOME"]}/{SPARK_SCHEDULING_FILE}')

    LOGGER.debug(f"Spark Configuration is: {spark_config.getAll()}")
    spark_context: SparkContext = SparkContext(conf=spark_config)
    java_import(spark_context._jvm, 'java.io.{ByteArrayInputStream, ObjectInputStream}')
    # we need these to deserialize byte stream.

    # Create pysparkling context for H2O model serialization/deserialization
    conf = H2OConf().setInternalClusterMode()
    hc = H2OContext.getOrCreate(conf)

    return spark_context, hc


SPARK_CONTEXT, HC = create_spark()  # Global Spark Context


def register_handlers() -> None:
    """
    Register all handlers
    to their associated
    handler classes
    """
    current_environment: CloudEnvironment = CloudEnvironments.get_current()

    if current_environment == CloudEnvironments.aws:
        KnownHandlers.register(HandlerNames.deploy_csp, SageMakerDeploymentHandler)

    elif current_environment == CloudEnvironments.azure:
        KnownHandlers.register(HandlerNames.deploy_csp, AzureDeploymentHandler)

    KnownHandlers.register(HandlerNames.enable_service, EnableServiceHandler)
    KnownHandlers.register(HandlerNames.disable_service, DisableServiceHandler)


class Runner(ThreadedTask):
    """
    A Threaded Worker that will be
    scaled across a pool via threading
    """

    def __init__(self, spark_context: SparkContext, hc: H2OContext, task_id: int, handler_name: str) -> None:
        """
        :param task_id: (int) the job id to process.
            Unfortunately, one of the limitations
            of SQLAlchemy is that its Sessions aren't thread safe.
            So that means we have to retrieve the id in
            the main thread and then the actual object
            in the worker thread, rather than passing the
            object directly from the main thread.
            This conforms to SQLAlchemy's 'thread-local' architecture.
        """
        super().__init__()
        self.spark_context: SparkContext = spark_context
        self.hc: H2OContext = hc
        self.task_id: id = task_id
        self.handler_name = handler_name

    def run(self) -> None:
        """
        Execute the job
        """
        try:
            LOGGER.info(f"Runner executing job id {self.task_id} --> {self.handler_name}")
            options: dict = dict(
                spark_context=self.spark_context
            ) if self.handler_name in RUN_HANDLERS else {}

            # for handlers that execute a job, e.g. retraining and deployment, we need to specify
            # a spark context. for modification handlers, that is not necessary

            KnownHandlers.get_class(self.handler_name)(self.task_id,
                                                       **options).handle()
            # execute the handler of the registered class with the specified handler_name

        except Exception:  # uncaught exceptions should't break the runner
            LOGGER.exception(
                f"Uncaught Exception Encountered while processing task #{self.task_id}")

def _get_pending_task_ids_handler() -> list:
    """
    Returns the earliest task id and handler in the
    database with the status 'PENDING'
    from the database
    """
    # Since this is running a lot, we will
    # use SQL so that it is more efficient
    #FIXME: If the database does not exist, SQLAlchemy will keep trying to make a new connection and
    #FIXME: eventually have a seg fault. We can't try/catch a seg fault.
    return execute_sql(POLL_SQL_QUERY)
    # SELECT only the `id` + `handler_name` column from the first inserted pending task

@APP.route('/job', methods=['POST'])
def get_new_pending_jobs() -> str:
    """
    Gets the currently pending jobs for Bobby to handle. This gets called both on Bobby startup (to populate the
    queue of jobs and on the api POST request /job for every new job submitted on the job-tracker/API code to mlflow.
    :return: str return code 200 or 500
    """
    try:
        job_datas = _get_pending_task_ids_handler()
        for job_data in job_datas:
            if job_data[0] not in LEDGER:
                job_id, handler_name = job_data
                LOGGER.info(f"Found New Job with id #{job_id} --> {handler_name}")
                LEDGER.record(job_id)
                WORKER_POOL.put(Runner(SPARK_CONTEXT, HC, job_id, handler_name))
        return "200: OK"
    except Exception:
        LOGGER.exception("Error: Encountered Fatal Error while locating and executing jobs")
        return f"500: Encountered Fatal Error while locating and executing jobs. {format_exc()}"

def main():
    if CloudEnvironments.get_current().can_deploy:
        LOGGER.info('Registering handlers...')
        register_handlers()
        LOGGER.info('Populating handlers...')
        populate_handlers(Session)
        LOGGER.info('Dispatching master...') # Done by the App
        LOGGER.info('Waiting for new jobs...')
        get_new_pending_jobs()
        # APP.run(host='0.0.0.0', port=2375)
    else:
        LOGGER.info(f'Cloud service {CloudEnvironments.get_current().name} does not support endpoint deployment. Sitting idly.')
        while True: # Sit idly. Nothing to do
            continue

main()
