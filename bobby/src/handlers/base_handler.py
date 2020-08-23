"""
Module containing the base class for all handlers
(Handlers don't need to worry about creating
DB Sessions, checking for handler status, catching
exception etc.). That behavior is uniform. Each individual
handler MUST implement the *abstract method*
`def _handle(self)` to carry out their own handle.
This function will be called in the `def handle(self)`
function.
"""
from abc import abstractmethod
from typing import Optional

from sqlalchemy.orm import load_only

from shared.models.splice_models import Handler, Job
from shared.services.database import SQLAlchemyClient
from shared.logger.job_lifecycle_manager import JobLifecycleManager

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class BaseHandler(object):
    """
    Base Class for all Handlers
    """

    def __init__(self, task_id: int) -> None:
        """
        Construct a new instance
        of Base Handler (cannot actually
        be instantiated because abstract
        methods need to be implemented)

        :param task_id: (int) the job id of the pending
            task to handle
        """
        self.task_id: int = task_id
        self.task: Optional[Job] = None  # assigned later

        self.Session = SQLAlchemyClient.SessionFactory()

        # Lifecycle Management
        self.manager: JobLifecycleManager = JobLifecycleManager(task_id=task_id)
        self.logger = self.manager.get_logger()

    def is_handler_enabled(self) -> None:
        """
        Set the handler specified
        in handler_name as an instance variable
        """
        self.logger.info(f"Checking whether handler {self.task.handler_name} is enabled", send_db=True)
        return self.Session.query(Handler) \
            .options(load_only("enabled")) \
            .filter(Handler.name == self.task.handler_name).first().enabled

    @abstractmethod
    def _handle(self) -> None:
        """
        Subclass-specific job handler
        functionality
        """
        pass

    # noinspection PyBroadException
    def handle(self) -> None:
        """
        Handle the given task and update
        statuses/detailed info on error/success
        """
        try:
            self.task = self.manager.retrieve_task()
            self.logger.info("A service worker has found your request", send_db=True, update_status='RUNNING')
            if self.is_handler_enabled():
                self.logger.info("Handler is available", send_db=True)
                self.logger.info("Retrieved task: " + str(self.task.__dict__))

                self._handle()
                self.logger.info(f"Success! Target '{self.task.handler_name}' completed successfully.",
                                 update_status='SUCCESS', send_db=True)
            else:
                self.logger.error(f"Error: Target '{self.task.handler_name}' is disabled", send_db=True)
                raise Exception("Task is disabled")

        except Exception:
            self.Session.rollback()
            self.logger.exception(f"Task Failed", update_status='FAILURE', send_db=True)
        finally:
            self.logger.info("TASK_COMPLETED", send_db=True)
            self.manager.destroy_logger()  # release the logger
            self.Session.close()  # close the thread local session in all cases
