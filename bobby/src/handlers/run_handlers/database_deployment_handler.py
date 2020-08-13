"""
Definition of Database Deployment Handler
which handles db deployment jobs
"""
from typing import Optional

from shared.models.model_types import Representations
from shared.logger.logging_config import logger

from .base_deployment_handler import BaseDeploymentHandler
from .db_deploy_utils.entities.db_model import Model
from .db_deploy_utils.db_model_creator import DatabaseModelConverter
from .db_deploy_utils.db_model_preparer import DatabaseModelMetadataPreparer
from .db_deploy_utils.db_model_ddl import DatabaseModelDDL


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

        self.creator: Optional[DatabaseModelConverter] = None
        self.metadata_preparer: Optional[DatabaseModelMetadataPreparer] = None

        self.model: Optional[Model] = None

    def _retrieve_raw_model_representations(self):
        """
        Read the raw model from the MLModel on disk
        """
        self.creator = DatabaseModelConverter(file_ext=self.artifact.file_ext,
                                              java_jvm=self.spark_context,
                                              df_schema=self.task.parsed_payload['df_schema'])

        self.creator.create_raw_representations(from_dir=self.downloaded_model_path)

    def _get_model_schema(self):
        """
        Get the model schema for MLeap representation,
        and for the target table to deploy to
        """
        ...

        self.creator.create_alternate_representations()
        self.model = self.creator.model

    def _create_model_metadata(self):
        """
        Populate model metadata necessary for database deployment
        """
        self.metadata_preparer = DatabaseModelMetadataPreparer(file_ext=self.artifact.file_ext,
                                                               model=self.model,
                                                               classes=self.task.parsed_payload['classes'],
                                                               library_specific=self.task.parsed_payload[
                                                                   'library_specific'])
        self.metadata_preparer.add_metadata()

    def _update_artifact(self):
        """
        Update the artifact with the retrieved data
        """
        self.artifact.database_binary = self.model.get_representation(Representations.BYTES)
        self.artifact.library = self.task.parsed_payload['library']
        self.artifact.version = self.task.parsed_payload['version']
        logger.info("Updating Artifact with serialized representation")
        self.Session.add(self.artifact)
        logger.debug("Committing Artifact Update")
        self.Session.commit()

    def _create_ddl(self):
        payload = self.task.parsed_payload
        ddl_creator = DatabaseModelDDL(session=self.Session, model=self.model, run_id=payload['run_id'],
                                       primary_key=payload['primary_key'], schema_name=payload['db_schema'],
                                       table_name=payload['db_table'], model_columns=payload['model_cols'],
                                       library_specific_args=payload['library_specific'])
