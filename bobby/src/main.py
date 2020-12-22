"""
This module has a Master, which polls Splice Machine
for new jobs and dispatches them to Workers for execution
(in threads). This execution happens in parallel.
"""
from os import environ as env_vars
from typing import List

from flask import Flask
from handlers.modifier_handlers import (DisableServiceHandler,
                                        EnableServiceHandler)
from handlers.run_handlers import (AzureDeploymentHandler,
                                   KubernetesDeploymentHandler,
                                   SageMakerDeploymentHandler,
                                   DatabaseDeploymentHandler,
                                   RetrainingDeploymentHandler)
from pyspark.sql import SparkSession
from pysparkling import H2OConf, H2OContext
from shared.api.responses import HTTP
from shared.environments.cloud_environment import (CloudEnvironment,
                                                   CloudEnvironments)
from shared.logger.logging_config import logger
from shared.models.feature_store_models import create_feature_store_tables
from shared.models.splice_models import create_bobby_tables, Job, RecurringJob
from shared.models.enums import RecurringJobStatuses
from shared.services.database import DatabaseSQL, SQLAlchemyClient
from shared.services.handlers import (HandlerNames, KnownHandlers,
                                      populate_handlers)
from shared.structures.ledger import JobLedger
from workerpool import Job as ThreadedTask
from workerpool import WorkerPool

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


def create_run_contexts():
    """
    Create a Global Spark Context that runs in the FAIR scheduling mode, and an H2O context. This means that
    it shares resources across threads. We need a Spark Context to create the directory structure from a
    deserialized PipelineModel (formerly a byte stream in the database.py)
    """
    SparkSession.builder \
        .master("local[*]") \
        .appName(env_vars.get('TASK_NAME', 'bobby-0')) \
        .config('spark.scheduler.mode', 'FAIR') \
        .config('spark.scheduler.allocation.file', f'{env_vars["SRC_HOME"]}/{SPARK_SCHEDULING_FILE}') \
        .config('spark.driver.extraClassPath', f'{env_vars["SRC_HOME"]}/lib/*') \
        .getOrCreate()

    # Create pysparkling context for H2O model serialization/deserialization
    conf = H2OConf().setInternalClusterMode()
    H2OContext.getOrCreate(conf)


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
    KnownHandlers.register(HandlerNames.schedule_retrain, RetrainingDeploymentHandler)


class Runner(ThreadedTask):
    """
    A Threaded Worker that will be
    scaled across a pool via threading
    """

    def __init__(self, task_id: int, handler_name: str) -> None:
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
        self.task_id: id = task_id

        self.handler_name = handler_name

    def run(self) -> None:
        """
        Execute the job
        """
        try:
            logger.info(f"Runner executing job id {self.task_id} --> {self.handler_name}")
            KnownHandlers.get_class(self.handler_name)(self.task_id).handle()

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
                WORKER_POOL.put(Runner(job_id, handler_name))
    except Exception:
        logger.exception("Error: Encountered Fatal Error while locating and executing jobs")
        raise


def check_for_k8s_deployments() -> None:
    """
    When the database pauses or Bobby restarts, all k8s deployed models will be removed as they are children deployments
    of Bobby. This function checks for k8s models that should be deployed and redeploys them.
    :return:
    """
    session = SQLAlchemyClient.SessionFactory()
    k8s_payloads = session.execute(DatabaseSQL.get_k8s_deployments_on_restart)
    for user, payload in k8s_payloads:
        # Create a new job to redeploy the model
        job: Job = Job(handler_name=HandlerNames.deploy_k8s,
                       user=user,
                       payload=payload)
        session.add(job)
    check_db_for_jobs()  # Add new jobs to the Job Ledger


def check_for_recurring_deployments():
    """
    When the database pauses of bobby restarts, all of the recurring deployments
    will stop. This function resumes them if they should be redepoloyed.
    :return:
    """
    session = SQLAlchemyClient.SessionFactory()
    recurring_jobs: List[RecurringJob] = session.query(RecurringJob).filter_by(status=RecurringJobStatuses.active).all()
    # Get all of the payloads in 1 query
    r_job_ids = [r.job_id for r in recurring_jobs]
    job_payloads = {j.id: j.payload for j in session.query(Job).filter(Job.id.in_(r_job_ids)).all()}
    for recurring_job in recurring_jobs:
        # Add a flag that bobby is recreating this job (after a db pause). This will ensure the job doesn't fail
        # Due to a PK constraint on the RecurringJobs table (since this job technically already exists there)
        p = job_payloads[recurring_job.job_id]
        p['skip_validation'] = True
        # Create a job to recreate the cron schedule
        job: Job = Job(
            handler_name=HandlerNames.schedule_retrain,
            user=recurring_job.user,
            payload=p
        )
        session.add(job)
        session.commit()
        check_db_for_jobs()  # Add new jobs to the Job Ledger


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
    logger.info("Creating Contexts...")
    create_run_contexts()
    logger.info("Creating Splice Tables...")
    create_bobby_tables()
    logger.info("Creating Feature Store Tables...")
    create_feature_store_tables()
    logger.info('Registering handlers...')
    register_handlers()
    logger.info('Populating handlers...')
    populate_handlers(SQLAlchemyClient.SessionFactory)
    logger.info('Checking for pre-existing k8s deployments')
    check_for_k8s_deployments()
    logger.info('Checking for pre-existing Scheduled Jobs')
    check_for_recurring_deployments()
    logger.info('Waiting for new jobs...')
    check_db_for_jobs()  # get initial set of jobs from the DB


main()
