"""
This module has a Master, which polls Splice Machine
for new jobs and dispatches them to Workers for execution
(in threads). This execution happens in parallel.
"""
from os import environ as env_vars
from time import sleep as wait

from handlers.modifier_handlers import EnableServiceHandler, DisableServiceHandler
from handlers.run_handlers import SageMakerDeploymentHandler
from mlmanager_lib.database.constants import HandlerNames
from mlmanager_lib.database.models import KnownHandlers, Job, SessionFactory, DBUtilities
from mlmanager_lib.logger.logging_config import logging
from mlmanager_lib.worker.ledger import JobLedger
from workerpool import Job as ThreadedTask, WorkerPool

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)

POLL_INTERVAL: int = 2  # check for new jobs every 2 seconds
LEDGER_MAX_SIZE: int = int(env_vars['WORKER_THREADS'] * 2)
# how many previous jobs to remember (to account for jobs in processing)

Session = SessionFactory()


def register_handlers() -> None:
    """
    Register all handlers
    to their associated
    handler classes
    """
    KnownHandlers.register(HandlerNames.deploy_aws, SageMakerDeploymentHandler)
    KnownHandlers.register(HandlerNames.deploy_azure, NotImplementedError)
    KnownHandlers.register(HandlerNames.enable_service, EnableServiceHandler)
    KnownHandlers.register(HandlerNames.disable_service, DisableServiceHandler)


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
            in the worker thread, rather than passing the
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
            LOGGER.info(f"Runner executing job id {self.task_id} --> {self.handler_name}")
            KnownHandlers.get_class(self.handler_name)(self.task_id).handle()
        except Exception:  # uncaught exceptions should't break the runner
            LOGGER.exception(
                f"Uncaught Exception Encountered while processing task #{self.task_id}")


class Master(object):
    """
    Master which checks for active
    jobs and dispatches them to workers
    """

    id_col: str = "id"  # column name for id in Jobs
    timestamp_col: str = "timestamp"
    status_col: str = "status"
    handler_col: str = "handler_name"
    job_table_name: str = Job.__table_schema_name__
    service_status: str = 'PENDING'

    poll_sql_query = \
        f"""
        SELECT TOP 1 {id_col}, {handler_col} FROM {job_table_name}
        WHERE {status_col}='{service_status}'
        ORDER BY "{timestamp_col}"
        """

    def __init__(self) -> None:
        """
        Initializes workerpool on construction of
        object
        """
        self.worker_pool: WorkerPool = WorkerPool(size=30)
        self.ledger: JobLedger = JobLedger(LEDGER_MAX_SIZE)

    @staticmethod
    def _get_first_pending_task_id_handler() -> list:
        """
        Returns the earliest task id and handler in the
        database with the status 'PENDING'
        from the database
        """
        # Since this is running a lot, we will
        # use SQL so that it is more efficient
        return DBUtilities.execute_sql(Master.poll_sql_query)
        # SELECT only the `id` + `handler_name` column from the first inserted pending task

    def poll(self) -> None:
        """
        Wait for new Jobs and dispatch
        them to the queue where they will be
        serviced by free workers
        """

        while True:
            job_data: list = Master._get_first_pending_task_id_handler()
            LOGGER.info(job_data)
            if job_data and job_data[0][0] not in self.ledger:
                job_id, handler_name = job_data[0]  # unpack arguments
                LOGGER.info(f"Found New Job with id #{job_id} --> {handler_name}")
                self.ledger.record(job_id)
                self.worker_pool.put(Runner(job_id, handler_name))

            wait(POLL_INTERVAL)


if __name__ == '__main__':
    register_handlers()
    DBUtilities.populate_handlers(Session)
    dispatcher: Master = Master()  # initialize worker pool
    dispatcher.poll()
