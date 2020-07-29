"""
Base class for modifier handlers:
Handlers that modify other handlers.
"""
import logging
from abc import abstractmethod

from shared.models.splice_models import Handler

from ..base_handler import BaseHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)


class BaseModifierHandler(BaseHandler):
    """
    Base Modifier Handler-- subclasses
    must implement their own 'modify' functionality
    """

    def __init__(self, task_id: int, action: str) -> None:
        """
        :param task_id: (int) ID of the task to execute
        :param action: (str) name used for exceptions and logging
            identifying the handler subclass
        """
        BaseHandler.__init__(self, task_id)
        self.action: str = action

    def _get_handler_by_name(self, handler_name: str) -> Handler:
        """
        Retrieve a Handler object from the database.py
        by its name

        :param handler_name: (str) the name of the
            handler to retrieve
        :return: (Handler) retrieved handler object (Python) from the database.py
        """
        return self.Session.query(Handler).filter_by(name=handler_name).first()

    @abstractmethod
    def modify(self) -> None:
        """
        Modifier handler specific
        functionality
        """
        pass

    def _handle(self) -> None:
        """
        Run modifications
        """
        LOGGER.debug(f"Running Modifier Handler: {self.action}")
        self.task.target_service = self.task.parsed_payload['service']
        self.target_handler_object: Handler = self._get_handler_by_name(
            self.task.target_service
        )
        if self.target_handler_object:
            if self.target_handler_object.modifiable:
                self.modify()
                self.Session.add(self.target_handler_object)
                self.Session.add(self.task)  # changes for GUI
                self.Session.commit()  # commit DB transaction
                LOGGER.info(f"Modified {self.target_handler_object} successfully")
            else:
                msg: str = f"Forbidden: Cannot execute {self.action}- Target handler " \
                    f"{self.target_handler_object} is not modifiable"
                LOGGER.error(msg)
                raise Exception(msg)
        else:
            msg: str = f"Error: Cannot execute {self.action}- Unable to find service " \
                f"{self.target_handler_object}"
            raise Exception(msg)
