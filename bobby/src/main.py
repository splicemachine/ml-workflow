from logging_config import * # has to be run first so logger will propagate configuration
from collections import deque
from time import sleep as wait
from os import environ as env_vars
from logging import getLogger
from workerpool import Job as Task, WorkerPool

from handlers.definitions import Handler, Job, SessionFactory, execute_sql
from handlers.aws_deployment_handler import SageMakerDeploymentHandler
from handlers.access_handlers import EnableHandler, DisableHandler

POLL_INTERVAL: int = 2  # check for new jobs every 2 seconds
LEDGER_MAX_SIZE: int = int(env_vars['WORKER_THREADS'] * 2)
# how many previous jobs to remember (to account for jobs in processing)

HANDLERS: dict = {
    'DEPLOY_AWS': SageMakerDeploymentHandler,
    'DEPLOY_AZURE': NotImplementedError,
    'ENABLE_SERVICE': EnableHandler,
    'DISABLE_SERVICE': DisableHandler,
    'RETRAIN_AWS': NotImplementedError,
    'RETRAIN_AZURE': NotImplementedError
}

Session = SessionFactory()

logger = getLogger(__name__)


def populate_handlers(handlers: dict) -> None:
    """
    Populates the handlers table with
    handlers which have their names
    specified in the 'handlers_list'
    argument, if they don't exist already

    :param handlers: (dict[str, object]) mapping of handler names to check (and create)
    """

    db_handler_names: list = [db_handler.name for db_handler in Session.query(Handler).all()]

    for handler_name in handlers:
        if handler_name not in db_handler_names:
            logger.info("Encountered New Handler: " + handler_name + "! Adding...")
            Session.add(Handler(name=handler_name))

    Session.commit()


class JobLedger:
    """
    A data structure that fills up
    to a maximum size with elements
    and then starts deleting oldest elements
    to maintain this size.

    *We use this to provide a buffer for jobs
    that are taking a while (especially if
    Splice Machine DB is running slow). We
    do not want to EVER service the same job
    from multiple threads (as it is probable
    to produce a write-write conflict). Thus,
    if a Job hasn't been updated to RUNNING
    status by the thread in time for the next
    poll, the Job ledger will make sure that it
    isn't serviced again. Obviously, this task
    could be done with a list, but this data
    structure is better-- it doesn't require
    searching through hundreds of jobs to find
    if one has been serviced yet. We use a deque
    (implemented as a linked list) because it
    has efficient deleting at head: O(1)*
    """

    def __init__(self, max_size: int) -> None:
        """
        :param max_size: (int) the maximum size of the
            list
        """
        self.max_size: int = max_size

        self.current_size: int = 0  # so we don't need to
        # keep finding length to improve performance

        self._linked_list: deque = deque()

    def record(self, job_id: int) -> None:
        """
        Record a new job_id in the job ledger,
        maintaining the maximum size

        :param job_id: (int) the job id to
            record in the list

        """
        if self.current_size >= self.max_size:
            self._linked_list.popleft()
            self.current_size -= 1

        self._linked_list.append(job_id)
        self.current_size += 1

    def __contains__(self, job_id: int) -> bool:
        """
        Return whether or not the Job Ledger
        contains a given job_id

        :param job_id: (int) the job id to check
            for the existence of
        :return: (bool) whether or not the job ledger
            contains the specified job id
        """
        return job_id in self._linked_list


class Runner(Task):
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
            logger.info(f"Runner executing job id {self.task_id} --> {self.handler_name}")
            HANDLERS[self.handler_name](self.task_id).handle()
        except Exception:  # uncaught exceptions should't break the runner
            logger.exception(f"Uncaught Exception Encountered while processing task #{self.task_id}")


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
        self.worker_pool: WorkerPool = WorkerPool(size=int(env_vars['WORKER_THREADS']))
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
        return execute_sql(Master.poll_sql_query)
        # SELECT only the `id` + `handler_name` column from the first inserted pending task

    def poll(self) -> None:
        """
        Wait for new Jobs and dispatch
        them to the queue where they will be
        serviced by free workers
        """

        while True:
            job_data: list = Master._get_first_pending_task_id_handler()
            if job_data and job_data[0][0] not in self.ledger:
                job_id, handler_name = job_data[0]  # unpack arguments
                logger.info(f"Found New Job with id #{job_id} --> {handler_name}")
                self.ledger.record(job_id)
                self.worker_pool.put(Runner(job_id, handler_name))

            wait(POLL_INTERVAL)


if __name__ == '__main__':
    populate_handlers(HANDLERS)
    dispatcher: Master = Master()  # initialize worker pool
    dispatcher.poll()
