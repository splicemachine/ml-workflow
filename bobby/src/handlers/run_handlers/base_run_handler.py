"""
Definition of Base Run Handler
"""
from abc import abstractmethod

from ..base_handler import BaseHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class BaseRunHandler(BaseHandler):
    """
    Base class for run handlers--
    handlers that execute jobs (AWS Deployment etc.)
    """

    def __init__(self, task_id: int) -> None:
        """
        :param task_id: (int) id of the task to execute
        """
        BaseHandler.__init__(self, task_id)

    @abstractmethod
    def execute(self) -> None:
        """
        Subclass specific run functionality
        """
        pass

    def _handle(self) -> None:
        """
        We add the MLFlow Run URL as a parameter
        that can be displayed in a GUI for all
        of these Jobs.
        """
        mlflow_run_url: str = f"#/experiments/{self.task.parsed_payload['experiment_id']}/runs/" \
            f"{self.task.parsed_payload['run_id']}"
        self.task.mlflow_url: str = mlflow_run_url
        self.Session.add(self.task)
        self.Session.commit()
        self.execute()
