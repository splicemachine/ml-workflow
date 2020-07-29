"""
Contains handler and functions
pertaining to Kubernetes Model Deployment
"""

from shared.logger.logging_config import logger

from .base_deployment_handler import BaseDeploymentHandler


class KubernetesDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for processing deployment to Kubernetes
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

    def execute(self):
        """
        Deploy Job to Kubernetes
        :return:
        """
        self._retrieve_model_binary_stream_from_db()
        self._deserialize_artifact_stream()
        logger.warning(self.task.__dict__)
        raise Exception(self.__dict__)
