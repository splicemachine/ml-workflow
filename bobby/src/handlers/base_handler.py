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
from traceback import format_exc
from typing import Optional

from sqlalchemy.orm import load_only

from shared.models.splice_models import Handler, Job
from shared.services.database import SQLAlchemyClient
from shared.logger.job_logging_config import JobLoggingManager

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

        # Logging
        self.logging_manager: JobLoggingManager = JobLoggingManager(session=self.Session, task_id=task_id)
        self.logger = self.logging_manager.get_logger()

    def is_handler_enabled(self) -> None:
        """
        Set the handler specified
        in handler_name as an instance variable
        """
        self.logger.info(f"Checking whether handler {self.task.handler_name} is enabled", send_db=True)
        return self.Session.query(Handler) \
            .options(load_only("enabled")) \
            .filter(Handler.name == self.task.handler_name).first().enabled

    def retrieve_task(self) -> None:
        """
        Set the task specified
        in task_id as an instance variable
        """
        self.logger.info("Retrieving Task in Worker Thread", send_db=True)
        self.task = self.Session.query(Job).filter(Job.id == self.task_id).first()
        self.task.parse_payload()  # deserialize json

    @staticmethod
    def _format_html_exception(traceback: str) -> str:
        """
        Turn a Python string into HTML so that it can be rendered in the deployment GUI

        :param traceback: string to format for the GUI
        :returns: string in HTML pre-formatted code-block format

        """
        # what we need to change in order to get formatted HTML <pre>
        replacements: dict = {
            '\n': '<br>',
            "'": ""
        }
        for subject, target in replacements.items():
            traceback: str = traceback.replace(subject, target)

        return f'<br>{traceback}'

    def update_task_in_db(self, status: str = None, info: str = None) -> None:
        """
        Update the current task in the Database
        under a local context

        :param status: (str) the new status to update to
        :param info: (str) the new info to update to

        One or both of the arguments (status/info) can be
        specified, and this function will update
        the appropriate attributes of the task and commit it
        """
        self.logger.info(f"Updating Task Status to {status}", send_db=True)
        self.task.update(status=status)
        self.Session.add(self.task)
        self.Session.commit()

    def succeed_task_in_db(self):
        """
        Succeed the current task in the Database under
        a local session context

        :param success_message: (str) the message to update
            the info string to

        """
        self.logger.warning("Task succeeded!", send_db=True)
        self.task.succeed()
        self.Session.add(self.task)
        self.Session.commit()

    def fail_task_in_db(self) -> None:
        """
        Fail the current task in the database.py under
        a local session context
        """
        self.logger.error(f"Task Failed..", send_db=True)
        self.task.fail()
        self.Session.add(self.task)
        self.Session.commit()

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
            self.retrieve_task()
            self.logger.info("A service worker has found your request", send_db=True)
            self.update_task_in_db(status='RUNNING')
            if self.is_handler_enabled():
                self.logger.info("Handler is available", send_db=True)
                self.logger.info("Retrieved task: " + str(self.task.__dict__))

                self._handle()
                self.succeed_task_in_db()
                self.logger.info(f"Success! Target '{self.task.handler_name}' completed successfully.", send_db=True)
            else:
                self.logger.error(f"Error: Target '{self.task.handler_name}' is disabled", send_db=True)

            self.Session.commit()  # commit transaction to database.py

        except Exception:
            self.logger.error(f"Error: <br>{self._format_html_exception(format_exc())}",
                              send_db=True)
            self.Session.rollback()
            self.fail_task_in_db()
            self.Session.commit()
        finally:
            self.logging_manager.destroy_logger() # release the logger
            self.Session.close()  # close the thread local session in all cases
