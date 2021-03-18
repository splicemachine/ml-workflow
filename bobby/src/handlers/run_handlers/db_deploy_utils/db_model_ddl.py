"""
Class to prepare database models for deployment
to Splice Machine
"""
from typing import Dict, List, Optional, Tuple

from sqlalchemy import inspect as peer_into_splice_db, text, func
from sqlalchemy.orm import Session
from mlflow.store.tracking.dbmodels.models import SqlParam

from shared.logger.logging_config import log_operation_status, logger
from shared.models.model_types import (DeploymentModelType, H2OModelType,
                                       KerasModelType, Metadata,
                                       SklearnModelType, SparkModelType)
from shared.services.database import SQLAlchemyClient, Converters, DatabaseSQL, DatabaseFunctions
from shared.models.feature_store_models import (Deployment, TrainingView,
                                                TrainingSet, TrainingSetFeature, Feature)

from .entities.db_model import Model


class DatabaseModelDDL:
    """
    Create tables and triggers for DB deployment
    """

    def __init__(self,
                 session: Session,
                 run_id: str,
                 model: Model,
                 schema_name: str,
                 table_name: str,
                 request_user: str,
                 model_columns: List[str],
                 primary_key: Dict[str, str],
                 library_specific_args: Optional[Dict[str, str]] = None,
                 create_model_table: bool = False,
                 logger=logger):
        """
        Initialize the class

        :param session: The sqlalchemy session
        :param run_id: (str) the run id
        :param model: Model object containing representations and metadata
        :param schema_name: (str) the schema name to deploy the model table to
        :param table_name: (str) the table name to deploy the model table to
        :param request_user: (str) the user who submitted the task
        :param model_columns: (List[str]) the columns in the feature vector passed into the model/pipeline
        :param primary_key: (List[Tuple[str,str]]) column name, SQL datatype for the primary key(s) of the table
        :param library_specific_args: (Dict[str,str]) All library specific function arguments (sklearn_args,
        keras_pred_threshold etc)
        :param logger: logger override
        """
        self.session = session
        self.model = model
        self.run_id = run_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.model_columns = model_columns or self.model.get_metadata(Metadata.SQL_SCHEMA).keys()# The model_cols parameter
        self.request_user = request_user
        self.primary_key = primary_key
        self.create_model_table = create_model_table
        self.library_specific_args = library_specific_args
        self.logger = logger

        self.schema_table_name = f'{self.schema_name}.{self.table_name}'
        self.prediction_data = {}
        self._create_prediction_data()

        # Create the schema of the table (we use this a few times)
        self.logger.info("Adding Schema String to model metadata...", send_db=True)

        mcols = [m.upper() for m in self.model_columns]
        self.model.add_metadata( # We split on "(" because columns like DECIMAL(5,2) and VARCHAR(3000) we only want the type
            Metadata.MODEL_VECTOR_STR, ', '.join([f'{name} {col_type.split("(")[0]}' for name, col_type in self.model.get_metadata(
                Metadata.SQL_SCHEMA).items() if name.upper() in mcols]) + ','
        )


    def _create_prediction_data(self):
        """
        Get the prediction data
        :return: prediction data
        """
        model_generic_type = self.model.get_metadata(Metadata.GENERIC_TYPE)

        if model_generic_type == DeploymentModelType.MULTI_PRED_INT:
            self.prediction_data.update({
                'prediction_call': 'MLMANAGER.PREDICT_CLASSIFICATION',
                'column_vals': ['PREDICTION VARCHAR(5000)'] + [f'"{cls}" DOUBLE' for cls in
                                                               self.model.get_metadata(Metadata.CLASSES)]
            })
        elif model_generic_type == DeploymentModelType.SINGLE_PRED_DOUBLE:
            self.prediction_data.update({
                'prediction_call': 'MLMANAGER.PREDICT_REGRESSION',
                'column_vals': ['PREDICTION DOUBLE']
            })
        elif model_generic_type == DeploymentModelType.SINGLE_PRED_INT:
            self.prediction_data.update({
                'prediction_call': 'MLMANAGER.PREDICT_CLUSTER',
                'column_vals': ['PREDICTION INT']
            })
        elif model_generic_type == DeploymentModelType.MULTI_PRED_DOUBLE:
            self.prediction_data.update({
                'prediction_call': 'MLMANAGER.PREDICT_KEY_VALUE',
                'column_vals': [f'"{cls}" DOUBLE' for cls in self.model.get_metadata(Metadata.CLASSES)]
            })
        else:
            raise Exception("Unknown Model Deployment Type")

    @staticmethod
    def _get_feature_vector_sql(model_columns: List[str], schema_types: Dict[str,str]):
        model_cols = [i.upper() for i in model_columns]
        stypes = {i.upper():j.upper() for i,j in schema_types.items()}

        sql_vector = ''
        for index, col in enumerate(model_cols):
            sql_vector += '||' if index != 0 else ''
            if 'VARCHAR' in stypes[str(col)].upper() or 'CLOB' in stypes[str(col)].upper():
                sql_vector += f'NEWROW.{col}||\',\''
            else:
                inner_cast = f'CAST(NEWROW.{col} as DECIMAL(38,10))' if \
                    stypes[str(col)] in {'FLOAT', 'DOUBLE'} else f'NEWROW.{col}'
                sql_vector += f'TRIM(CAST({inner_cast} as CHAR(41)))||\',\''
        return sql_vector


    def create_model_deployment_table(self):
        """
        Creates the table that holds the columns of the feature vector as well as a unique MOMENT_ID
        """
        schema_str = self.model.get_metadata(Metadata.SCHEMA_STR)

        if DatabaseFunctions.table_exists(table_name=self.table_name, schema_name=self.schema_name, engine=SQLAlchemyClient.engine):
            raise Exception(
                f'The table {self.schema_table_name} already exists. To deploy to an existing table, do not pass in a'
                f' dataframe and/or set create_model_table parameter=False')

        table_create_sql = f"""CREATE TABLE {self.schema_table_name} (
                CUR_USER VARCHAR(50) DEFAULT CURRENT_USER,
                EVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                RUN_ID VARCHAR(50) DEFAULT '{self.run_id}',
                {schema_str}"""

        pk_cols = ''
        for key in self.primary_key:
            # If pk is already in the schema_string, don't add another column. PK may be an existing value
            if key.lower() not in schema_str.lower() and \
                    key.upper() not in self.model.get_metadata(Metadata.RESERVED_COLUMNS):
                table_create_sql += f'{key} {self.primary_key[key]},'
            pk_cols += f'{key},'

        for col in self.prediction_data['column_vals']:
            table_create_sql += f'{col},'

        table_create_sql += f'PRIMARY KEY({pk_cols.rstrip(",")}))'

        self.logger.info(f"Executing\n{table_create_sql}", send_db=True)
        self.session.execute(table_create_sql)

    def alter_model_table(self):
        """
        Alters the provided table for deployment. Adds columns for storing model results as well as metadata such as
        current user, eval time, run_id, and the prediction label columns
        """
        self.logger.info("Altering existing model...", send_db=True)
        # Table needs to exist
        if not DatabaseFunctions.table_exists(table_name=self.table_name, schema_name=self.schema_name, engine=SQLAlchemyClient.engine):
            raise Exception(
                f'The table {self.schema_table_name} does not exist. To create a new table for deployment, '
                f'pass in a dataframe and set the set create_model_table=True')

        # Currently we only support deploying 1 model to a table
        inspector = peer_into_splice_db(SQLAlchemyClient.engine)
        table_cols = [col['name'] for col in inspector.get_columns(self.table_name, schema=self.schema_name)]
        reserved_fields = set(
            self.model.get_metadata(Metadata.RESERVED_COLUMNS) + (self.model.get_metadata(Metadata.CLASSES) or [])
        )
        for col in table_cols:
            if col in reserved_fields:
                raise Exception(
                    f'The table {self.schema_table_name} looks like it already has values associated with '
                    f'a deployed model. Only 1 model can be deployed to a table currently.'
                    f'The table cannot have the following fields: {reserved_fields}')

        # Splice cannot currently add multiple columns in an alter statement so we need to make a bunch and execute
        # all of them
        alter_table_sql = []
        alter_table_syntax = f'ALTER TABLE {self.schema_table_name} ADD COLUMN'
        alter_table_sql.append(f'{alter_table_syntax} CUR_USER VARCHAR(50) DEFAULT CURRENT_USER')
        alter_table_sql.append(f'{alter_table_syntax} EVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        alter_table_sql.append(f'{alter_table_syntax} RUN_ID VARCHAR(50) DEFAULT \'{self.run_id}\'')

        # Add the correct prediction type
        for col in self.prediction_data['column_vals']:
            alter_table_sql.append(f'{alter_table_syntax} {col}')

        for sql in alter_table_sql:
            self.logger.info(f"Executing\n{sql})", send_db=True)
            self.session.execute(sql)

    def create_vti_prediction_trigger(self):
        """
        Create Trigger that uses VTI instead of parsing
        """
        self.logger.info("Creating VTI Prediction Trigger...", send_db=True)
        schema_types = self.model.get_metadata(Metadata.SQL_SCHEMA)
        model_type = self.model.get_metadata(Metadata.GENERIC_TYPE)
        classes = self.model.get_metadata(Metadata.CLASSES)
        model_vector_str = self.model.get_metadata(Metadata.MODEL_VECTOR_STR)
        specific_type = self.model.get_metadata(Metadata.TYPE)

        prediction_call = """new "com.splicemachine.mlrunner.MLRunner"('key_value', '{run_id}', {raw_data}, 
        '{model_vector_str}'"""

        if model_type == DeploymentModelType.MULTI_PRED_DOUBLE and isinstance(specific_type, SklearnModelType):
            self.logger.info('Managing Sklearn prediction args', send_db=True)
            if 'predict_call' not in self.library_specific_args and 'predict_args' not in self.library_specific_args:
                self.logger.info("Using transform call...", send_db=True)
                # This must be a .transform call
                predict_call = 'transform'
                predict_args = None
            else:
                predict_call = self.library_specific_args.get('predict_call', 'predict')
                predict_args = self.library_specific_args.get('predict_args')

            prediction_call += f", '{predict_call}', '{predict_args}'"

        elif model_type == DeploymentModelType.MULTI_PRED_DOUBLE and len(classes) == 2 and \
                self.library_specific_args.get('pred_threshold') and isinstance(specific_type,KerasModelType):
            self.logger.info('Managing Keras prediction args', send_db=True)
            prediction_call += f", '{self.library_specific_args.get('pred_threshold')}'"

        prediction_call += ')'  # Close the prediction call

        trigger_sql = f"""CREATE TRIGGER {self.schema_name}.runModel_{self.table_name}_{self.run_id} 
                        AFTER INSERT ON {self.schema_table_name} REFERENCING NEW AS NEWROW FOR EACH ROW
                        UPDATE {self.schema_table_name} SET ("""

        output_column_names = ''  # Names of the output columns from the model
        output_cols_vti_reference = ''  # Names references from the VTI (ie b.COL_NAME)
        output_cols_schema = ''  # Names with their datatypes (always DOUBLE for now)
        for cls in classes:
            output_column_names += f'"{cls}",'
            output_cols_vti_reference += f'b."{cls}",'
            output_cols_schema += f'"{cls}" DOUBLE,' if cls != 'prediction' else f'"{cls}" INT,'  # sk predict_proba

        raw_data = DatabaseModelDDL._get_feature_vector_sql(self.model_columns, schema_types)

        # Cleanup (remove concatenation SQL from last variable) + schema for PREDICT call
        raw_data = raw_data[:-5].lstrip('||')
        model_vector_str_pred_call = model_vector_str.replace('\t', '').replace('\n', '').rstrip(',')

        prediction_call = prediction_call.format(run_id=self.run_id, raw_data=raw_data, model_vector_str=model_vector_str_pred_call)

        trigger_sql += f'{output_column_names[:-1]}) = ('
        trigger_sql += f'SELECT {output_cols_vti_reference[:-1]} FROM {prediction_call}' \
                       f' as b ({output_cols_schema[:-1]}) WHERE 1=1) WHERE '
        # 1=1 because of a DB bug that requires a where clause

        trigger_sql += ' AND '.join([f'{index} = NEWROW.{index}' for index in self.primary_key])
        # TODO - use the above syntax for other queries
        self.logger.info(f"Executing\n{trigger_sql}", send_db=True)
        self.session.execute(trigger_sql)


    def create_prediction_trigger(self):
        """
        Create the actual prediction trigger for insertion
        """
        self.logger.info("Creating Prediction Trigger...", send_db=True)
        schema_types = self.model.get_metadata(Metadata.SQL_SCHEMA)
        # The database function call is dependent on the model type
        prediction_call = self.prediction_data['prediction_call']

        pred_trigger = f"""CREATE TRIGGER {self.schema_name}.runModel_{self.table_name}_{self.run_id}
                           BEFORE INSERT ON {self.schema_table_name} REFERENCING NEW AS NEWROW 
                           FOR EACH ROW SET NEWROW.PREDICTION={prediction_call}(\'{self.run_id}\',"""

        pred_trigger += DatabaseModelDDL._get_feature_vector_sql(self.model_columns, schema_types)

        # Cleanup + schema for PREDICT call
        pred_trigger = pred_trigger[:-5].lstrip('||') + ',\n\'' + self.model.get_metadata(Metadata.MODEL_VECTOR_STR).replace(
            '\t', '').replace('\n', '').rstrip(',') + '\')'

        self.logger.info(f"Executing\n{pred_trigger}", send_db=True)
        self.session.execute(pred_trigger)

    def create_parsing_trigger(self):
        """
        Creates the secondary trigger that parses the results of the first trigger and updates the prediction
        row populating the relevant columns.
        TO be removed when we move to VTI only
        """
        self.logger.info("Creating parsing trigger...", send_db=True)
        sql_parse_trigger = f"""CREATE TRIGGER {self.schema_name}.PARSERESULT_{self.table_name}_{self.run_id}
                                BEFORE INSERT ON {self.schema_table_name} REFERENCING NEW AS NEWROW
                                FOR EACH ROW set """
        set_prediction_case_str = 'NEWROW.PREDICTION=\n\t\tCASE\n'
        for i, c in enumerate(self.model.get_metadata(Metadata.CLASSES)):
            sql_parse_trigger += f'NEWROW."{c}"=MLMANAGER.PARSEPROBS(NEWROW.prediction,{i}),'
            set_prediction_case_str += f'\t\tWHEN MLMANAGER.GETPREDICTION(NEWROW.prediction)={i} then \'{c}\'\n'

        set_prediction_case_str += '\t\tEND;'
        if self.model.get_metadata(Metadata.GENERIC_TYPE) == DeploymentModelType.MULTI_PRED_DOUBLE:
            # These models don't have an actual prediction
            sql_parse_trigger = sql_parse_trigger[:-1]
        else:
            sql_parse_trigger += set_prediction_case_str

        formatted_sql_parse_trigger = sql_parse_trigger.replace('\n', ' ').replace('\t', ' ')

        self.logger.info(f"Executing\n{formatted_sql_parse_trigger}", send_db=True)
        self.session.execute(formatted_sql_parse_trigger)

    def add_model_to_metadata_table(self):
        """
        Add the model to the deployed model metadata table
        """
        self.logger.info("Adding Model to Metadata table", send_db=True)
        table_id = self.session.execute(f"""
            SELECT TABLEID FROM SYSVW.SYSTABLESVIEW WHERE TABLENAME='{self.table_name.upper()}' 
            AND SCHEMANAME='{self.schema_name.upper()}'""").fetchone()[0]

        trigger_suffix = f"{self.table_name}_{self.run_id}".upper()

        trigger_1_id, trigger_1_timestamp = self.session.execute(f"""
            SELECT TRIGGERID, CREATIONTIMESTAMP FROM SYS.SYSTRIGGERS
            WHERE TABLEID='{table_id}' AND TRIGGERNAME='RUNMODEL_{trigger_suffix}'
        """).fetchone()

        trigger_2_id = self.session.execute(f"""
            SELECT TRIGGERID FROM SYS.SYSTRIGGERS
            WHERE TABLEID='{table_id}' AND TRIGGERNAME='PARSERESULT_{trigger_suffix}'
        """).fetchone()
        trigger_2_id = trigger_2_id[0] if trigger_2_id else None

        self.logger.info("Executing SQL to insert Database Deployed Metadata", send_db=True)
        self.session.execute(
            DatabaseSQL.add_database_deployed_metadata.format(
                run_uuid=self.run_id, action='DEPLOYED', tableid=table_id,
                trigger_type='INSERT', triggerid=trigger_1_id, triggerid_2=trigger_2_id, db_env='PROD',
                db_user=self.request_user, action_date=trigger_1_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            )
        )
        self.logger.info("Done executing.", send_db=True)

    def _register_training_set(self, key_vals: Dict[str,str]):
        """
        Registers training set for the deployment
        :param key_vals: Dictionary containing the relevant keys for the training set
        :return: TrainingSet
        """

        self.logger.info(f"Dictionary of params {str(key_vals)}")

        view_id = key_vals['splice.feature_store.training_view_id']

        if eval(view_id):
            self.logger.info(f"Found training view with ID {view_id}")
            self.logger.info(f"Registering new training set for training view {key_vals['splice.feature_store.training_set']}", send_db=True)

            # Create training set
            ts = TrainingSet(
                view_id=view_id,
                name=key_vals['splice.feature_store.training_set'],
                last_update_username=self.request_user
            )
        # If there is no training view, this means the user called fs.get_feature_dataset, and created a dataframe
        # for training without using a TrainingView (think clustering where all features come from FeatureSets).
        # in this case, the view is null, but the TrainingSet is still a valid one, with features, a start
        # time and an end time
        else:
            ts = TrainingSet(
                view_id=None,
                name=None,
                last_update_username=self.request_user
            )
        self.session.add(ts)
        self.session.merge(ts) # Get the training_set_id
        return ts

    def _register_training_set_features(self, ts: TrainingSet, key_vals: Dict[str,str]):
        """
        Registers the features of a training set for a deployment
        :param ts: The TrainingSet
        :param key_vals: Dictionary containing the relevant keys for the training set
        :return:
        """

        # Create an entry for each feature
        training_set_features: List[SqlParam] = self.session.query(SqlParam).filter_by(run_uuid=self.run_id) \
            .filter(SqlParam.key.like('splice.feature_store.training_set_feature%')).all()
        self.logger.info("Done. Getting feature IDs for each feature...")
        features: List[Feature] =  self.session.query(Feature)\
            .filter(func.upper(Feature.name).in_([feat.value.upper() for feat in training_set_features])).all()

        # Validate that these features have not been deleted. If they have we cannot deploy
        if len(features) < len(training_set_features):
            feats = [f.name.lower() for f in features]
            tsf = [f.value.lower() for f in training_set_features]
            missing_features = set(tsf) - set(feats)
            raise Exception(f"Some of the features used in this model's training have been deleted: {missing_features}"
                            f" You cannot deploy a model that was trained on features that have been deleted.")

        self.logger.info(f"Done. Registering all {len(features)} features")
        for feat in features:
            self.session.add(
                TrainingSetFeature(
                    training_set_id=ts.training_set_id,
                    feature_id=feat.feature_id, # The mlflow param's value is the feature name
                    last_update_username=self.request_user,
                    is_label=(feat.name.lower() == key_vals['splice.feature_store.training_set_label'].lower())
                )
            )
    def _register_model_deployment(self, ts: TrainingSet, key_vals: Dict[str,str]):
        """
        Registers the model deployment with the TrainingSet
        :param ts: The TrainingSet
        :param key_vals: Dictionary containing the relevant keys for the training set
        :return:
        """
        # Add the model deployment
        # Splice PyODBC cannot take string representation of datetime, so we need to use raw SQL :(
        # Check if deployment exists (schema.table name)
        table_exists = self.session.query(Deployment)\
            .filter_by(model_schema_name=self.schema_name)\
            .filter_by(model_table_name=self.table_name)\
            .all()

        deployment = dict(
            model_schema_name=self.schema_name,
            model_table_name=self.table_name,
            training_set_id=ts.training_set_id,
            training_set_start_ts=key_vals['splice.feature_store.training_set_start_time'],
            training_set_end_ts=key_vals['splice.feature_store.training_set_end_time'],
            training_set_create_ts=key_vals['splice.feature_store.training_set_create_time'],
            run_id=self.run_id,
            last_update_username=self.request_user
        )
        # Can't do splice upsert because Splice Machine considers an an upsert as an insert,
        # so our update trigger won't ever fire.
        if table_exists: # Update
            self.logger.info("Updating deployment in Feature Store metadata", send_db=True)
            self.session.execute(
                DatabaseSQL.update_feature_store_deployment.format(**deployment)
            )
        else: # Insert
            self.logger.info("Adding new deployment to Feature Store metadata", send_db=True)
            self.session.execute(
                DatabaseSQL.add_feature_store_deployment.format(**deployment)
            )

    def _validate_training_view(self, view_id):
        """
        Validates that a particular training view exists. If a run in mlflow was training using a training set from
        the feature store, and that training set was taken from a training view that no longer exists, we cannot
        deploy this model (as we cannot recreate the training set used for training).

        :param view_id: The view ID
        """
        tvw: TrainingView = self.session.query(TrainingView)\
            .filter_by(view_id=view_id).one_or_none()
        if not tvw:
            raise Exception(f"The training view (id:{view_id}) used for this run has been deleted."
                            f" You cannot deploy a model that was trained using a training view that no longer exists.")

    def register_feature_store_deployment(self):
        """
        On deployment, if the model has an associated training set, we want to log this deployment in the
        FeatureStore.deployment table.
        First, we check if there is a training set.
        If there is, we check if a record of this deployment already exists in the deployment table (PK schema.table)
        If no deployment exists, we insert a new row.
        If one does, we UPDATE that row.
        We cannot use UPSERT because Splice treats upserts as inserts always, we there is an AFTER UPDATE trigger
        on the FeatureStore.Deployment table.
        :return:
        """
        self.logger.info("Checking if run was created with Feature Store training set", send_db=True)
        # Check if run has training set
        training_set_params = [f'splice.feature_store.{i}' for i in ['training_set','training_set_start_time',
                                                                    'training_set_end_time', 'training_set_create_time',
                                                                     'training_set_label', 'training_view_id']]
        params: List[SqlParam] = self.session.query(SqlParam)\
            .filter_by(run_uuid=self.run_id)\
            .filter(SqlParam.key.in_(training_set_params))\
            .all()

        if len(params)==len(training_set_params): # Run has associated training set
            key_vals = {param.key:param.value for param in params}
            self.logger.info("Training set found! Registering...", send_db=True)

            # We need to ensure this view hasn't been deleted since the time this run was created
            if eval(key_vals['splice.feature_store.training_view_id']): # returns 'None' not None so need eval
                self.logger.info('Validating that the Training View still exists')
                self._validate_training_view(key_vals['splice.feature_store.training_view_id'])

            ts: TrainingSet = self._register_training_set(key_vals)
            self.logger.info(f"Done. Gathering individual features...", send_db=True)
            self._register_training_set_features(ts, key_vals)
            self.logger.info("Done. Registering deployment with Feature Store")
            self._register_model_deployment(ts, key_vals)
            self.logger.info("Done!", send_db=True)

    def create(self):
        """
        Deploy the model to the database DDL
        """
        inspector = peer_into_splice_db(SQLAlchemyClient.engine)
        if self.create_model_table and not self.primary_key:
            raise Exception("A primary key must be specified if creating model table")
        #
        self.primary_key = self.primary_key or inspector.get_primary_keys(self.table_name, schema=self.schema_name)

        if self.create_model_table:
            with log_operation_status("creating model deployment table", logger_obj=self.logger):
                self.create_model_deployment_table()
        else:
            with log_operation_status("altering existing table", logger_obj=self.logger):
                self.alter_model_table()

        with log_operation_status("creating trigger", logger_obj=self.logger):
            if self.model.get_metadata(Metadata.TYPE) in {H2OModelType.MULTI_PRED_DOUBLE,
                                                          KerasModelType.MULTI_PRED_DOUBLE,
                                                          SklearnModelType.MULTI_PRED_DOUBLE}:
                self.create_vti_prediction_trigger()
            else:
                self.create_prediction_trigger()

        if self.model.get_metadata(Metadata.TYPE) in {SparkModelType.MULTI_PRED_INT, H2OModelType.MULTI_PRED_INT}:
            with log_operation_status("create parsing trigger", logger_obj=self.logger):
                self.create_parsing_trigger()

        with log_operation_status("add model to metadata table", logger_obj=self.logger):
            self.add_model_to_metadata_table()

        with log_operation_status("Managing Feature Store metadata", logger_obj=self.logger):
            self.register_feature_store_deployment()
