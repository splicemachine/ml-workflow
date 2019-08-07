"""
Definition of Base Run Handler
"""
from abc import abstractmethod
from os import environ as env_vars

from mlflow.tracking import MlflowClient

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
        self.mlflow_run: object = None

    @staticmethod
    def get_mlflow_service_uri() -> str:
        """
        Get the URL endpoint for MLFlow Tracking
        Server
        :return: (str) address to MLFlow Tracking Server
        """
        return f"{env_vars['MLFLOW_URL']}"

    def retrieve_run_from_mlflow(self) -> None:
        """
        Retrieve the current run from mlflow tracking server
        """
        client: MlflowClient = MlflowClient(tracking_uri=self.get_mlflow_service_uri())
        try:
            self.mlflow_run: object = client.get_run(self.task.parsed_payload['run_id'])
        except Exception:
            raise Exception(
                "Error: The Run associated with the ID specified could not be retrieved"
            )

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
        self.retrieve_run_from_mlflow()
        run_url: str = f"/#/experiments/{self.mlflow_run.info.experiment_id}/" \
            f"runs/{self.mlflow_run.info.run_uuid}"

        self.task.mlflow_url: str = f"<a href='{run_url}' target='_blank' onmouseover=" \
            f"'javascript:event.target.port={env_vars['MLFLOW_PORT']}'>Link to Mlflow Run</a>"
        # populates a link to the associated Mlflow run that opens in a new tab.
        self.Session.add(self.task)
        self.Session.commit()
        self.execute()
