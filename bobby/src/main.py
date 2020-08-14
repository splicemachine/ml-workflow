"""
This module has a Master, which polls Splice Machine
for new jobs and dispatches them to Workers for execution
(in threads). This execution happens in parallel.
"""
from os import environ as env_vars

from flask import Flask
from handlers.modifier_handlers import (DisableServiceHandler,
                                        EnableServiceHandler)
from handlers.run_handlers import (AzureDeploymentHandler,
                                   KubernetesDeploymentHandler,
                                   SageMakerDeploymentHandler,
                                   DatabaseDeploymentHandler)
from pyspark.sql import SparkSession
from pysparkling import H2OConf, H2OContext
from workerpool import Job as ThreadedTask
from workerpool import WorkerPool

from shared.api.responses import HTTP
from shared.environments.cloud_environment import (CloudEnvironment,
                                                   CloudEnvironments)
from shared.logger.logging_config import logger
from shared.models.splice_models import create_bobby_tables
from shared.services.database import DatabaseSQL, SQLAlchemyClient
from shared.services.handlers import (HandlerNames, KnownHandlers,
                                      populate_handlers)
from shared.structures.ledger import JobLedger

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

APP: Flask = Flask("bobby")

# Jobs
POLL_INTERVAL: int = 5  # check for new jobs every 2 seconds
LEDGER_MAX_SIZE: int = int(env_vars['WORKER_THREADS'] * 2)  # how many previous jobs to account for

# Spark
SPARK_SCHEDULING_FILE: str = "configuration/fair_scheduling.xml"

# We only need spark context for modifiable handlers
RUN_HANDLERS: tuple = KnownHandlers.get_modifiable()

WORKER_POOL: WorkerPool = WorkerPool(size=30)
LEDGER: JobLedger = JobLedger(LEDGER_MAX_SIZE)


def create_run_contexts() -> tuple:
    """
    Create a Global Spark Context that runs in the FAIR scheduling mode, and an H2O context. This means that
    it shares resources across threads. We need a Spark Context to create the directory structure from a
    deserialized PipelineModel (formerly a byte stream in the database.py)
    :return: (SparkContext) a Global Spark Context, (H2OContext) a global H2O pysparkling context
    """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(env_vars['TASK_NAME']) \
        .config('spark.scheduler.mode', 'FAIR') \
        .config('spark.scheduler.allocation.file', f'{env_vars["SRC_HOME"]}/{SPARK_SCHEDULING_FILE}') \
        .getOrCreate()

    # Create pysparkling context for H2O model serialization/deserialization
    conf = H2OConf().setInternalClusterMode()
    hc = H2OContext.getOrCreate(conf)

    return spark, hc


SPARK_SESSION, HC = create_run_contexts()  # Global Spark Context


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
    KnownHandlers.register(HandlerNames.deploy_k8s, KubernetesDeploymentHandler)
    KnownHandlers.register(HandlerNames.deploy_database, DatabaseDeploymentHandler)


class Runner(ThreadedTask):
    """
    A Threaded Worker that will be
    scaled across a pool via threading
    """

    def __init__(self, spark_session: SparkSession, hc: H2OContext, task_id: int, handler_name: str) -> None:
        """
        :param task_id: (int) the job id to process.
            Unfortunately, one of the limitations
            of SQLAlchemy is that its Sessions aren't thread safe.
            So that means we have to retrieve the id in
            the main thread and then the actual object
            in the structures thread, rather than passing the
            object directly from the main thread.
            This conforms to SQLAlchemy's 'thread-local' architecture.
        """
        super().__init__()
        self.spark_context: SparkSession = spark_session
        self.hc: H2OContext = hc
        self.task_id: id = task_id

        self.handler_name = handler_name

    def run(self) -> None:
        """
        Execute the job
        """
        try:
            logger.info(f"Runner executing job id {self.task_id} --> {self.handler_name}")
            options: dict = dict(
                spark_context=self.spark_context
            ) if self.handler_name in RUN_HANDLERS else {}

            KnownHandlers.get_class(self.handler_name)(self.task_id, **options).handle()

        except Exception:  # uncaught exceptions should't break the runner
            logger.exception(
                f"Uncaught Exception Encountered while processing task #{self.task_id}")


def check_db_for_jobs() -> None:
    """
    Gets the currently pending jobs for Bobby to handle. This gets called both on Bobby startup (to populate the
    queue of jobs and on the api POST request /job for every new job submitted on the job-tracker/API code to mlflow.
    :return: str return code 200 or 500
    """
    try:
        jobs = SQLAlchemyClient.execute(DatabaseSQL.retrieve_jobs)
        for job_data in jobs:
            if job_data[0] not in LEDGER:
                job_id, handler_name = job_data
                logger.info(f"Found New Job with id #{job_id} --> {handler_name}")
                LEDGER.record(job_id)
                WORKER_POOL.put(Runner(SPARK_SESSION, HC, job_id, handler_name))
    except Exception:
        logger.exception("Error: Encountered Fatal Error while locating and executing jobs")
        raise


@APP.route('/job', methods=['POST'])
@HTTP.generate_json_response
def get_new_jobs():
    """
    Calls the function to get the new pending jobs via the API endpoint /job
    :return: HTTP response 200 or 500
    """
    check_db_for_jobs()
    return dict(data="Checked DB for Jobs")


def main():
    logger.info("Creating Splice Tables...")
    create_bobby_tables()
    logger.info('Registering handlers...')
    register_handlers()
    logger.info('Populating handlers...')
    populate_handlers(SQLAlchemyClient.SessionFactory)
    logger.info('Waiting for new jobs...')
    check_db_for_jobs()  # get initial set of jobs from the DB

    # APP.run(host='0.0.0.0', port=2375)


main()
