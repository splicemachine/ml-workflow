"""
Contains handler and functions
pertaining to Database Model Removal
"""
from datetime import datetime

from pytz import utc
from sqlalchemy import and_, func

from shared.db.functions import DatabaseFunctions
from shared.db.sql import SQL
from shared.models.feature_store_models import Deployment
from shared.models.mlflow_models import DatabaseDeployedMetadata

from .base_deployment_handler import BaseDeploymentHandler


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

    def _get_db_tables(self):
        """
        Gets and saves the database table(s) associated to the run ID of the deployed model. If no schema/table was
        supplied this will get all schema/tables associated with the model.
        """
        payload = self.task.parsed_payload
        schema = payload.get('db_schema')
        table = payload.get('db_table')
        if (schema and not table) or (table and not schema):
            raise Exception("You cannot provide only a schema or only a table. Either provide both or neither "
                            "(neither will drop all tables associated to this model)")
        if schema and table:
            # Validate that this table is associated to this model
            # We need to execute raw SQL because this is a View, not a table
            sql = f"""
                SELECT * FROM mlmanager.live_model_status 
                WHERE UPPER(schema_name)='{schema.upper()}'
                AND UPPER(table_name)='{table.upper()}'
                AND action='DEPLOYED'
                AND run_uuid='{self.run_id}'
            """

            model_table_exists = self.Session.execute(sql).fetchall()
            if self.run_id and not model_table_exists:
                raise Exception(f'Model {self.run_id} has not been deployed to table {schema}.{table}. You can see '
                                f'all current and past deployments via mlflow.get_deployed_models()')
            elif not model_table_exists:
                raise Exception(f'Cannot find any deployments to table {schema}.{table}. You can see '
                                f'all current and past deployments via mlflow.get_deployed_models()')
            self.schema_names.append(schema)
            self.table_names.append(table)
        else:
            sql = SQL.get_model_table_from_run.format(run_id=self.run_id)
            res = self.Session.execute(sql).fetchall()
            for schema, table in res:
                self.schema_names.append(schema)
                self.table_names.append(table)
            self.logger.info(f'Found {len(self.table_names)} tables for model {self.run_id}', send_db=True)


    def _drop_triggers(self):
        """
        Removes the trigger(s) associated to the table(s) of the model
        """

        for schema, table in zip(self.schema_names, self.table_names):
            trigger_name = f'runModel_{table}_{self.run_id}'
            self.logger.info(f'TRIGGER EXISTS: {DatabaseFunctions.trigger_exists(schema, trigger_name, db=self.Session)}', send_db=True)
            self.logger.info(f'Dropping trigger for table {schema}.{table}', send_db=True)
            self.logger.info(f'Dropping trigger {schema}.{trigger_name}')
            DatabaseFunctions.drop_trigger_if_exists(schema, trigger_name, self.Session)

    def _remove_deployment_from_feature_store(self):
        """
        Checks to see if this model is associated to the feature store, and if so removes the deployment record (still
        keeps the history)
        """
        for schema, table in zip(self.schema_names, self.table_names):
            self.logger.info(f'Removing deployment table {schema}.{table} from Feature Store metadata', send_db=True)
            self.Session.query(Deployment).filter(
                and_(
                    func.upper(Deployment.model_schema_name) == schema.upper(),
                    func.upper(Deployment.model_table_name) == table.upper()
                )
            ).delete(synchronize_session='fetch')

    def _update_deployment_metadata(self):
        """
        Updates the DB_DEPLOYED_METADATA and sets the table(s) status to undeployed or deleted depending on if
        the user requested to drop the table
        """
        for schema, table in zip(self.schema_names, self.table_names):
            if self.task.parsed_payload['drop_table']:
                new_status = 'DELETED'
            else:
                new_status = 'UNDEPLOYED'

            self.logger.info(f'Updating deployment {schema}.{table} status to {new_status}', send_db=True)
            update_payload = dict(
                action=new_status,
                action_date=datetime.now(tz=utc),
                db_user=self.task.user
            )

            tableid = self.Session.execute(
                SQL.get_table_id.format(table_name=table, schema_name=schema)
            ).fetchone()[0]
            self.Session.query(DatabaseDeployedMetadata).filter(
                and_(
                    DatabaseDeployedMetadata.tableid == tableid,
                    func.upper(DatabaseDeployedMetadata.run_uuid) == self.run_id.upper()
                )
            ).update(update_payload, synchronize_session='fetch')

    def _drop_tables(self):
        """
        Drops the model table(s) (if the user explicitly requests the table be dropped)
        """
        for schema, table in zip(self.schema_names, self.table_names):
            self.logger.info(f'Dropping table {schema}.{table}', send_db=True)
            DatabaseFunctions.drop_table_if_exists(schema, table, self.Session)

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
        # We set the run_id here and not in the __init__ because the self.task hasn't been assigned at that point
        # It gets assigned during the handle() function (see BaseHandler.handle)
        self.run_id = self.task.parsed_payload.get('run_id')
        steps: tuple = (
            self._get_db_tables,
            self._drop_triggers,
            self._remove_deployment_from_feature_store,
            self._update_deployment_metadata
        )
        if self.task.parsed_payload['drop_table']:
            self.logger.info('Drop table flag was set to true. Table(s) will be dropped', send_db=True)
            steps += (self._drop_tables,)

        for execute_step in steps:
            execute_step()
        # Release the savepoint so we can commit changes
        self.savepoint.commit()
        self.Session.commit()
