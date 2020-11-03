"""
Definition of Database Deployment Handler
which handles db deployment jobs
"""
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType
from sqlalchemy import inspect as peer_into_splice_db, text

from shared.models.model_types import Metadata, Representations
from shared.services.database import Converters, SQLAlchemyClient, DatabaseSQL
from shared.services.database import SQLAlchemyClient

from .base_deployment_handler import BaseDeploymentHandler
from .db_deploy_utils.db_representation_creator import DatabaseRepresentationCreator
from .db_deploy_utils.db_model_ddl import DatabaseModelDDL
from .db_deploy_utils.db_metadata_preparer import DatabaseModelMetadataPreparer
from .db_deploy_utils.entities.db_model import Model


class DatabaseDeploymentHandler(BaseDeploymentHandler):
    """
    Handler for processing deployment to the database
    """

    def __init__(self, task_id: int):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) Id of job to process
        """
        BaseDeploymentHandler.__init__(self, task_id)

        self.creator: Optional[DatabaseRepresentationCreator] = None
        self.metadata_preparer: Optional[DatabaseModelMetadataPreparer] = None
        self.spark_session = SparkSession.builder.getOrCreate()
        self.jvm = self.spark_session._jvm
        self.model: Optional[Model] = None

    def _validate_primary_key(self):
        """
        Validates the primary key passed by the user conforms to SQL. If the user is deploying to an existing table
        This verifies that the table has a primary key
        """
        inspector = peer_into_splice_db(SQLAlchemyClient.engine)
        create_model_table = self.task.parsed_payload['create_model_table']
        primary_key = self.task.parsed_payload['primary_key']

        if create_model_table and not primary_key:
            raise Exception("A deployed model table must have primary_key parameter specified")

        if not create_model_table:
            primary_keys = inspector.get_primary_keys(self.task.parsed_payload['db_table'],
                                                      schema=self.task.parsed_payload['db_schema'])
            assert primary_keys, "No primary keys were found for the specified table"
            self.task.parsed_payload['primary_key'] = {pk: None for pk in primary_keys}

        if primary_key:
            for key in primary_key:
                assert primary_key[key].split('(')[0] in Converters.SQL_TYPES, f"Unsupported type {key}"

    def _retrieve_raw_model_representations(self):
        """
        Read the raw model from the MLModel on disk
        """
        self.logger.info("Creating raw model representation from MLModel", send_db=True)
        self.creator = DatabaseRepresentationCreator(file_ext=self.artifact.file_extension,
                                                     java_jvm=self.jvm,
                                                     logger=self.logger,
                                                     df_schema=self.task.parsed_payload['df_schema'])

        self.creator.get_library_representation(from_dir=self.downloaded_model_path)
        self.model = self.creator.model
        self.logger.info("Done.", send_db=True)

    def _add_model_examples_from_df(self):
        """
        Add model examples from a dataframe
        """
        self.logger.info("Generating Model Schema and DF Example from Dataframe")
        specified_df_schema = self.task.parsed_payload['df_schema']
        struct_schema = StructType.fromJson(json=specified_df_schema)

        self.model.add_metadata(Metadata.DATAFRAME_EXAMPLE, self.spark_session.createDataFrame(data=[],
                                                                                               schema=struct_schema))

        self.model.add_metadata(Metadata.SQL_SCHEMA,
                                {field.name: Converters.SPARK_DB_CONVERSIONS[str(field.dataType).split('(')[0]]
                                 for field in struct_schema})
        # The database
        self.model.add_metadata(Metadata.SCHEMA_STR,
                                ', '.join([f"{field.name.upper()} {Converters.SPARK_DB_CONVERSIONS[str(field.dataType)].upper()}"
                                 for field in struct_schema]) + ',')
        self.logger.info("Done.")

    def _add_model_examples_from_db(self, table_name: str, schema_name: str):
        """
        Add model examples from the database table
        :param table_name: the table name to retrieve examples from
        :param schema_name: schema to retrieve examples from
        """
        import pyspark.sql.types as spark_types
        inspector = peer_into_splice_db(SQLAlchemyClient.engine)
        struct_type = StructType()
        schema_dict = {}
        schema_str = []
        columns = inspector.get_columns(table_name, schema=schema_name)

        if len(columns) == 0:
            raise Exception("Either the table has no columns, or the table cannot be found.")

        for field in columns:
            # FIXME: Sqlalchemy assumes lowercase and Splice assumes uppercase. Quoted cols in DB don't translate
            schema_dict[field['name'].upper()] = str(field['type']).split('(')[0].upper()
            # Remove length specification from datatype for backwards conversion
            spark_d_type = getattr(spark_types,
                                   Converters.DB_SPARK_CONVERSIONS[str(field['type']).split('(')[0].upper()])
            # TODO @ben or @amrit: case-sensitity
            struct_type.add(StructField(name=field['name'].upper(), dataType=spark_d_type()))

            schema_str.append(f"{field['name'].upper()} {str(field['type']).upper()}")

        self.model.add_metadata(Metadata.DATAFRAME_EXAMPLE,
                                self.spark_session.createDataFrame(data=[], schema=struct_type))
        self.model.add_metadata(Metadata.SQL_SCHEMA, schema_dict)
        self.model.add_metadata(Metadata.SCHEMA_STR, ', '.join([i for i in schema_str]) + ',')

    def _get_model_schema(self):
        """
        Get the model schema for MLeap representation,
        and for the target table to deploy to
        """
        specified_df_schema = self.task.parsed_payload['df_schema']
        reference_table = self.task.parsed_payload['reference_table']
        reference_schema = self.task.parsed_payload['reference_schema']

        if self.task.parsed_payload['create_model_table']:
            self.logger.info("Adding Model Schema and DF...", send_db=True)
            if reference_table and reference_schema:
                if specified_df_schema:
                    raise Exception("Cannot pass both df schema and reference table")
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

    def _create_model_metadata(self):
        """
        Populate model metadata necessary for database deployment
        """
        self.metadata_preparer = DatabaseModelMetadataPreparer(file_ext=self.artifact.file_extension,
                                                               model=self.model,
                                                               logger=self.logger,
                                                               classes=self.task.parsed_payload['classes'],
                                                               library_specific=self.task.parsed_payload[
                                                                   'library_specific'])
        self.metadata_preparer.add_metadata()

    def _update_artifact(self):
        """
        Update the artifact with the retrieved data
        """
        self.Session.execute(
            text(DatabaseSQL.update_artifact_database_blob.format(
                run_uuid=self.artifact.run_uuid, name=self.artifact.name
            )), dict(binary=self.model.get_representation(Representations.BYTES))
        )

        self.logger.info("Updating Artifact with serialized representation", send_db=True)

    def _create_ddl(self):
        """
        Create DDL for Database Deployment inside SpliceDB
        """
        payload = self.task.parsed_payload
        ddl_creator = DatabaseModelDDL(session=self.Session, model=self.model, run_id=payload['run_id'],
                                       primary_key=payload['primary_key'], schema_name=payload['db_schema'],
                                       table_name=payload['db_table'], model_columns=payload['model_cols'],
                                       create_model_table=payload['create_model_table'],
                                       library_specific_args=payload['library_specific'], logger=self.logger,
                                       request_user=self.task.user)
        ddl_creator.create()
        self.logger.info("Flushing", send_db=True)
        self.Session.flush()
        self.logger.warning("Committing Transaction to Database", send_db=True)
        self.Session.commit()
        self.logger.info("Committed.", send_db=True)

    def execute(self) -> None:
        """
        Execute the steps required to accomplish database deployment
        """
        steps: tuple = (
            self._validate_primary_key,
            self._retrieve_model_binary_stream_from_db,
            self._deserialize_artifact_stream,
            self._retrieve_raw_model_representations,
            self._get_model_schema,
            self._retrieve_alternate_model_representations,
            self._update_artifact,
            self._create_model_metadata,
            self._create_ddl
        )

        for step_no, execute_step in enumerate(steps):
            self.logger.info(f"Running Step {step_no}...")
            execute_step()
