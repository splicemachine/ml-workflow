"""
Definition of Database Deployment Handler
which handles db deployment jobs
"""

from .base_deployment_handler import BaseDeploymentHandler


class DatabaseDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for processing deployment to the database
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

        self.db_model = None

        self.raw_model # tuple for H2o, a singular for everything else

    def _add_database_model_to_artifact(self, run_id: str, db_model: bytearray, ):
        pass
