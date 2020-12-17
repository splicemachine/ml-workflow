"""
Contains code pertaining to retraining deployment
"""
from .base_deployment_handler import BaseDeploymentHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class RetrainingDeploymentHandler(BaseDeploymentHandler):
    """
    Deployment handler for retraining
    """
    def __init__(self, task_id: int) -> None:
        """
        Initialize retraining handler constructor
        :param task_id: task id to process
        """
        BaseDeploymentHandler.__init__(self, task_id)

    def _build_template_parameters