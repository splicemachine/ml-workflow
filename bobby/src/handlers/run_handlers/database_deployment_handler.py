"""
Definition of Database Deployment Handler
which handles db deployment jobs
"""
from uuid import uuid4
import json
from typing import Optional

from pyspark.sql.types import StructType, StructField
from shared.models.model_types import Representations, Metadata
from shared.logger.logging_config import logger
from shared.services.database import Converters, SQLAlchemyClient

from sqlalchemy import inspect as peer_into_splicedb
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
        self.savepoint_name: Optional[str] = f'pre-deploy-{str(uuid4()).replace("-", "")}'

    def _set_savepoint(self):
        """
        Set a savepoint that we can restore to if necessary
        (if the deployment fails). We use a UUID for the savepoint
        name to guarantee uniqueness across simultaneous workerpool executions.
        """
        self.Session.execute(f"SAVEPOINT {self.savepoint_name}")
        self.Session.commit()

    def _retrieve_raw_model_representations(self):
        """
        Read the raw model from the MLModel on disk
        """
        self.creator = DatabaseModelConverter(file_ext=self.artifact.file_ext,
                                              java_jvm=self.jvm,
                                              df_schema=self.task.parsed_payload['df_schema'])

        self.creator.create_raw_representations(from_dir=self.downloaded_model_path)

    def _add_model_examples_from_df(self):
        """
        Add model examples from a dataframe
        """
        specified_df_schema = json.loads(self.task.parsed_payload['df_schema'])
        struct_schema = StructType.fromJson(json=specified_df_schema)

        self.model.add_metadata(Metadata.DATAFRAME_EXAMPLE, self.spark_session.createDataFrame(data=[],
                                                                                               schema=struct_schema))

        self.model.add_metadata(Metadata.SQL_SCHEMA, {field.name: Converters[str(field.datatype)]
                                                      for field in struct_schema})

    def _add_model_examples_from_db(self, table_name: str, schema_name: str):
        """
        Add model examples from the database table
        :param table_name: the table name to retrieve examples from
        :param schema_name: schema to retrieve examples from
        """
        import pyspark.sql.types as spark_types
        inspector = peer_into_splicedb(SQLAlchemyClient.engine)
        struct_type = StructType()
        schema_dict = {}

        columns = inspector.get_columns(table_name, schema_name=schema_name)

        if len(columns) == 0:
            raise Exception("Either the table has no columns, or the table cannot be found.")

        for field in columns:
            schema_dict[field['name']] = str(field['type'])
            # Remove length specification from datatype for backwards conversion
            spark_d_type = getattr(spark_types, Converters.DB_SPARK_CONVERSIONS[str(field['type'].split('(')[0])])
            struct_type.add(StructField(name=field['name'], dataType=spark_d_type))

        self.model.add_metadata(Metadata.DATAFRAME_EXAMPLE,
                                self.spark_session.createDataFrame(data=[], schema=struct_type))
        self.model.add_metadata(Metadata.SQL_SCHEMA, schema_dict)

    def _get_model_schema(self):
        """
        Get the model schema for MLeap representation,
        and for the target table to deploy to
        """
        specified_df_schema = self.task.parsed_payload['df_schema']
        reference_table = self.task.parsed_payload['reference_table']
        reference_schema = self.task.parsed_payload['reference_table']

        if self.task.parsed_payload['create_model_table']:
            logger.info("Creating Model Table...")
            if reference_table and reference_schema:
                if specified_df_schema:
                    raise Exception("Cannot create model table with both db schema and reference table")
                self._add_model_examples_from_db(table_name=reference_table, schema_name=reference_schema)
            elif specified_df_schema:
                self._add_model_examples_from_df()
            else:
                raise Exception("Either db schema or reference table+schema must be specified")
        else:
            self._add_model_examples_from_db(table_name=self.task.parsed_payload['db_table'],
                                             schema_name=self.task.parsed_payload['db_schema'])

    def _retrieve_alternate_model_representations(self):
        """
        Retrieve alternate model representations, including serialized
        and library specific (MOJO, MLeap) that are required for deployment
        """
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
        """
        Create DDL for Database Deployment inside SpliceDB
        """
        payload = self.task.parsed_payload
        ddl_creator = DatabaseModelDDL(session=self.Session, model=self.model, run_id=payload['run_id'],
                                       primary_key=payload['primary_key'], schema_name=payload['db_schema'],
                                       table_name=payload['db_table'], model_columns=payload['model_cols'],
                                       library_specific_args=payload['library_specific'])
        ddl_creator.create()

    def exception_handler(self, exc: Exception):
        """
        Callback for exceptions that are thrown
        during job execution
        :param exc: the exception object
        """
        super().exception_handler(exc=exc)  # rollback the session
        logger.error("Rolling Back to pre-deployment savepoint")
        self.Session.execute(f"ROLLBACK TO {self.savepoint_name}")
        self.Session.commit()
        logger.error("Releasing savepoint")
        self.Session.execute(f"RELEASE SAVEPOINT {self.savepoint_name}")
        self.Session.commit()

    def execute(self) -> None:
        """
        Execute the steps required to accomplish database deployment
        """
        steps: tuple = (
            self._set_savepoint,
            self._retrieve_model_binary_stream_from_db,
            self._deserialize_artifact_stream,
            self._retrieve_raw_model_representations,
            self._get_model_schema,
            self._retrieve_alternate_model_representations,
            self._update_artifact,
            self._create_model_metadata,
            self._create_ddl
        )

        for execute_step in steps:
            execute_step()
