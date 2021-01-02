"""
Base Utility Handler
"""
from abc import abstractmethod

from ..base_handler import BaseHandler


class BaseUtilityHandler(BaseHandler):
    """
    Base Utility Handler
    """

    def __init__(self, task_id: int, logging_format: str = None):
        BaseHandler.__init__(self, task_id=task_id, logging_format=logging_format)

    @abstractmethod
    def execute(self):
        """
        Subclass Specific Functionality
        :return:
        """
        pass

    def _handle(self) -> None:
        """
        Run subclass functionality
        :return:
        """
        self.execute()
