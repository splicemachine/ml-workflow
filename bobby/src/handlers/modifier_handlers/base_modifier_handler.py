"""
Base class for modifier handlers:
Handlers that modify other handlers.
"""
from abc import abstractmethod

from shared.models.splice_models import Handler
from shared.services.database import SQLAlchemyClient
from ..base_handler import BaseHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


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
        self.logger.info(f"Running Modifier Handler: {self.action}", send_db=True)
        self.task.target_service = self.task.parsed_payload['service']
        self.target_handler_object: Handler = self._get_handler_by_name(
            self.task.target_service
        )
        if self.target_handler_object:
            if self.target_handler_object.modifiable:
                self.modify()
                self.Session.add(self.target_handler_object)
                self.Session.commit()  # commit DB transaction
                self.logger.info(f"Modified successfully", send_db=True)
            else:
                msg: str = f"Forbidden: Cannot execute {self.action}- Target handler " \
                           f"{self.target_handler_object} is not modifiable"
                self.logger.error(msg, send_db=True)
                raise Exception(msg)
        else:
            msg: str = f"Error: Cannot execute {self.action}- Unable to find service"
            raise Exception(msg)
