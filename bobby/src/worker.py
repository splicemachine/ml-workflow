from re import compile, sub
from os import environ as env_vars
from json import loads
import logging.config
from time import sleep as wait
import workerpool

from .handlers.definitions import Handler, Job, ManagedDBSession
from .handlers.aws_deployment_handler import SageMakerDeploymentHandler
from .handlers.access_handlers import EnableHandler, DisableHandler

ENV_CHARACTER: str = '`'
ENV_REGEX = compile('{chr}(.*?){chr}'.format(chr=ENV_CHARACTER))

POLL_INTERVAL: int = 2  # check for new jobs every 2 seconds

HANDLERS: dict = {
    'DEPLOY_AWS': SageMakerDeploymentHandler,
    'DEPLOY_AZURE': NotImplementedError,
    'ENABLE_SERVICE': EnableHandler,
    'DISABLE_SERVICE': DisableHandler,
    'RETRAIN_AWS': NotImplementedError,
    'RETRAIN_AZURE': NotImplementedError
}


def retrieve_logger_configuration() -> dict:
    """
    Retrieves and Formats the JSON Logger Configuration
    specified in config/logging_config.json.
    All values surrounded with `` (e.g. `FRAMEWORK_NAME`)
    will be replaced with their associated environment
    variable values.

    :return: (dict) logging configuration
    """

    with open('config/logging_config.json', 'r', encoding='utf-8') as config_file:
        json_data: str = config_file.read()

    # extract the corresponding environment variable for each string that the regex matches
    formatted_json_data: str = sub(ENV_REGEX, lambda match: env_vars[match.groups()[1:-1]], json_data)
    return loads(formatted_json_data)


logging.config.dictConfig(retrieve_logger_configuration())
logger = logging.getLogger(__name__)


def populate_handlers(handlers: dict) -> None:
    """
    Populates the handlers table with
    handlers which have their names
    specified in the 'handlers_list'
    argument, if they don't exist already

    :param handlers: (dict[str, object]) mapping of handler names to check (and create)
    """

    with ManagedDBSession() as Session:
        db_handler_names: list = [db_handler.name for db_handler in Session.query(Handler).all()]

        for handler_name in handlers:
            if handler_name not in db_handler_names:
                logger.info("Encountered New Handler: " + handler_name + "! Adding...")
                Session.add(Handler(name=handler_name))

        Session.commit()  # commit transaction at once is faster than individual


class Runner(workerpool.Job):
    """
    A Threaded Worker that will be
    scaled across a pool via threading
    """

    def __init__(self, task: Job) -> None:
        """
        :param task: (Job) the job to process
        """
        super().__init__()
        self.task: Job = task

    def run(self) -> None:
        """
        Execute the job
        """
        try:
            logger.info(f"Runner executing job {self.task}")
            HANDLERS[self.task.handler_name].handle()
            logger.debug(f"Completed Execution of job {self.task}")
        except Exception:  # uncaught exceptions should't break the runner
            logger.exception(f"Uncaught Exception Encountered while processing task {self.task}")


class Master(object):
    """
    Master which checks for active
    jobs and dispatches them to workers
    """

    def __init__(self) -> None:
        """
        Initializes workerpool on construction of
        object
        """
        self.worker_pool: workerpool.WorkerPool = workerpool.WorkerPool(size=env_vars['WORKER_THREADS'])

    @staticmethod
    def _get_first_pending_task() -> Job:
        """
        Returns the first task in the
        database with the status 'PENDING'
        from the database
        """
        with ManagedDBSession() as Session:
            return Session.query(Job).filter(Job.status == 'PENDING').order_by(Job.timestamp).first()

    def poll(self) -> None:
        """
        Wait for new Jobs and dispatch
        them to the queue where they will be
        serviced by free workers
        """
        while True:
            job: Job or None = Master._get_first_pending_task()
            if job:
                logger.info(f"Found New Job {job}")
                self.worker_pool.put(Runner(job))

            wait(POLL_INTERVAL)


if __name__ == '__main__':
    populate_handlers(HANDLERS)
    dispatcher: Master = Master()  # initialize worker pool
    dispatcher.poll()
