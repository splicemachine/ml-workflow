"""
Contains handler and functions
pertaining to Database Model Removal
"""
from os import environ as env_vars
from subprocess import check_output
from tempfile import NamedTemporaryFile

from yaml import dump as dump_yaml

from shared.services.kubernetes_api import KubernetesAPIService

from .base_deployment_handler import BaseDeploymentHandler
from shared.services.database import DatabaseSQL, DatabaseFunctions


class DatabaseUndeploymentHandler(BaseDeploymentHandler):
    """
    Handler for removing deployment of model from the Database
    """

    def __init__(self, task_id: int):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) Id of job to process
        """
        BaseDeploymentHandler.__init__(self, task_id)
        self.savepoint = self.Session.begin_nested() # Create a savepoint in case of errors
        # We may be dropping multiple schemas and tables
        self.schema_names = []
        self.table_names = []
        self.run_id = self.task.parsed_payload['run_id']

    def _get_db_tables(self):
        """
        Gets and saves the database table(s) associated to the run ID of the deployed model. If no schema/table was
        supplied this will get all schema/tables associated with the model.
        """
        payload = self.task.parsed_payload
        schema = payload['db_schema']
        table = payload['db_table']
        if (schema and not table) or (table and not schema):
            raise Exception("You cannot provide only a schema or only a table. Either provide both or neither "
                            "(neither will drop all tables associated to this model)")
        if schema and table:
            # Validate that this table is associated to this model
            table_exists = self.Session.execute(
                f"""
                SELECT * FROM mlmanager.live_model_status 
                WHERE run_id={self.run_id}
                AND UPPER(schema_name)={schema.upper()}
                AND UPPER(table_name)={table.upper()}
                AND action='DEPLOYED'
                """).fetchall()
            if not table_exists:
                raise Exception(f'Model {self.run_id} has not been deployed to table {schema}.{table}. You can see '
                                f'all current and past deployments via mlflow.get_deployed_models()')
            self.schema_names.append(schema)
            self.table_names.append(table)
        else:
            sql = DatabaseSQL.get_model_table_from_run.format(run_id=payload['run_id'])
            res = self.Session.execute(sql).fetchall()
            for schema, table in res:
                self.schema_names.append(schema)
                self.table_names.append(table)

    def _drop_triggers(self):
        """
        Removes the trigger(s) associated to the table(s) of the model
        """

        for schema, table in zip(self.schema_names, self.table_names):
            trigger_name = f'{schema}.runModel_{table}_{self.run_id}'
            DatabaseFunctions.drop_trigger_if_exists(trigger_name, self.Session)

    def remove_deployment_from_feature_store(self):
        """
        Checks to see if this model is associated to the feature store, and if so removes the deployment record (still
        keeps the history)
        """

    def _drop_tables(self):
        """
        Drops the model table(s) (if the user explicitly requests the table be dropped)
        """
        for schema, table in zip(self.schema_names, self.table_names):
            DatabaseFunctions.drop_table_if_exists(schema, table, self.Session.get_bind())

    def exception_handler(self, exc: Exception):
        self.logger.info("Rolling back...",send_db=True)
        self.savepoint.rollback()
        self.Session.rollback()
        self._cleanup()  # always run cleanup, regardless of success or failure
        raise exc

    def execute(self):
        """
        Undeploy model from database
        :return:
        """
        steps: tuple = (
            self._get_db_tables,
            self._drop_triggers
        )
        if self.task.parsed_payload['drop_tables']:
            steps += (self._drop_tables(),)

        for execute_step in steps:
            execute_step()
