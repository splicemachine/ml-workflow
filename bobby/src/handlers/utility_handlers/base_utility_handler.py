"""
Base Utility Handler
"""
from abc import abstractmethod

from ..base_handler import BaseHandler


class BaseUtilityHandler(BaseHandler):
    """
    Base Utility Handler
    """

    def __init__(self, task_id: int, **manager_kwargs):
        BaseHandler.__init__(self, task_id=task_id, **manager_kwargs)

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
