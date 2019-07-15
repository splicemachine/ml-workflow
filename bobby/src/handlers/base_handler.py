import logging
from traceback import format_exc
from abc import abstractmethod

from .definitions import Job, Handler, ManagedDBSession

logger = logging.getLogger(__name__)


class BaseHandler(object):
    """
    Base Class for all Handlers
    """

    def __init__(self, task: Job, handler_name: str = None) -> None:
        """
        Construct a new instance
        of Base Handler (cannot actually
        be instantiated because abstract
        methods need to be implemented)

        :param task: (Job) the job object
            to handle
        """
        self.task: Job = task
        self.handler_name: str = handler_name

    def is_handler_enabled(self) -> bool:
        """
        Check whether the associated handler
        is enabled in the database

        :return: (boolean) whether or not the handler is enabled
        """
        with ManagedDBSession() as Session:
            handler: Handler = Session.query(Handler).filter_by(handler_name=self.handler_name).first()

        if handler:
            return handler.enabled

        raise Exception(f"Handler {self.handler_name} cannot be found in the database")

    @staticmethod
    def _format_html_exception(traceback: str) -> str:
        """
        Turn a Python string into HTML so that it can be rendered in the deployment GUI

        :param string: string to format for the GUI
        :returns: string in HTML pre-formatted code-block format

        """
        # what we need to change in order to get formatted HTML <code>
        replacements: dict = {
            '\n': '<br>',
            "'": ""
        }
        for subject, target in replacements.items():
            traceback: str = traceback.replace(subject, target)

        return f'<br><code>{traceback}</code>'

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
        self.task.update(status=status, info=info)

        with ManagedDBSession() as Session:
            Session.add(self.task)

    def succeed_task_in_db(self, success_message: str) -> None:
        """
        Succeed the current task in the Database under
        a local session context

        :param success_message: (str) the message to update
            the info string to

        """
        self.task.succeed(success_message)

        with ManagedDBSession() as Session:
            Session.add(self.task)

    def fail_task_in_db(self, failure_message: str) -> None:
        """
        Fail the current task in the database under
        a local session context

        :param failure_message: (str) the message to updatr
            the info string to

        """
        self.task.fail(failure_message)

        with ManagedDBSession() as Session:
            Session.add(self.task)

    @abstractmethod
    def _handle(self) -> None:
        """
        Subclass-specific job handler
        functionality
        """
        pass

    # noinspection PyBroadException
    def handle(self):
        """
        Handle the given task and update
        statuses/detailed info on error/success
        """

        try:
            if self.is_handler_enabled():
                self.update_task_in_db(status='RUNNING')
                self._handle()
                self.succeed_task_in_db(f"Task {self.task.handler_name} finished successfully")
            else:
                self.fail_task_in_db(f"Failure:<br>{self.task.handler_name} is disabled")
        except Exception:
            logger.exception("An unexpected error occurred while handling task")
            # this will print the message along with the stack trace
            self.fail_task_in_db(f"Failure:<br>{self._format_html_exception(format_exc())}")
            # format traceback as HTML for render in deployment GUI
