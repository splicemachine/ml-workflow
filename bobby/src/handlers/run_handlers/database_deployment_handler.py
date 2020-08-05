"""
Contains handler and functions
pertaining to database model deployment
"""
from os import environ as env_vars
from subprocess import check_call
from tempfile import NamedTemporaryFile

from .base_deployment_handler import BaseDeploymentHandler


class DatabaseDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for processing deployment database
    """

    def __init__(self, task_id: int, spark_context: None):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) Id of job to process
        :param spark_context: (SparkContext) Global FAIR SparkContext for deserializing models
        """
        BaseDeploymentHandler.__init__(self, task_id, spark_context)
