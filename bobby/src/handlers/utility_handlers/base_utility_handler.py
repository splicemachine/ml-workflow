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

    def populate_parent_run_id(self):
        """
        Add the parent run ID to the task
        """
        # Since we are manipulating the task (which is an object local to the main thread)
        # we need to use the manager's session to change the task's data
        self.logger.info("Setting parent run ID...")
        self.task.parent_job_id = self.task.parsed_payload['parent_job_id']
        self.manager.Session.add(self.task)
        self.manager.Session.commit()
        self.logger.info("Done.")

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
        self.populate_parent_run_id()
        self.execute()
