"""
Class to prepare database models for deployment
to Splice Machine
"""
from typing import List, Dict, Optional, Tuple

from shared.models.model_types import DeploymentModelType
from sqlalchemy import inspect as peer_into_splice_db
from shared.logger.logging_config import logger
from shared.models.model_types import Metadata
from sqlalchemy.orm import Session
from shared.shared.models.mlflow_models import DatabaseDeployedMetadata, SysTables, SysTriggers

from shared.services.database import SQLAlchemyClient

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
                 model_columns: List[str],
                 primary_key: List[Tuple[str, str]],
                 library_specific_args: Optional[Dict[str, str]] = None):
        """
        Initialize the class

        :param session: The sqlalchemy session
        :param run_id: (str) the run id
        :param model: Model object containing representations and metadata
        :param schema_name: (str) the schema name to deploy the model table to
        :param table_name: (str) the table name to deploy the model table to
        :param model_columns: (List[str]) the columns in the feature vector passed into the model/pipeline
        :param primary_key: (List[Tuple[str,str]]) column name, SQL datatype for the primary key(s) of the table
        :param library_specific_args: (Dict[str,str]) All library specific function arguments (sklearn_args, keras_pred_threshold etc)
        """
        self.session = session
        self.model = model
        self.run_id = run_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.model_columns = model_columns  # The model_cols parameter
        self.primary_key = primary_key
        self.library_specific_args = library_specific_args

        self.prediction_data = {
            DeploymentModelType.MULTI_PRED_INT: {
                'prediction_call': 'MLMANAGER.PREDICT_CLASSIFICATION',
                'column_vals': ['PREDICTION VARCHAR(5000)'] + [f'"{cls}" DOUBLE' for cls in
                                                               self.model.get_metadata(Metadata.CLASSES)]
            },
            DeploymentModelType.SINGLE_PRED_DOUBLE: {
                'prediction_call': 'MLMANAGER.PREDICT_REGRESSION',
                'column_vals': ['PREDICTION DOUBLE']
            },
            DeploymentModelType.SINGLE_PRED_INT: {
                'prediction_call': 'MLMANAGER.PREDICT_CLUSTER',
                'column_vals': ['PREDICTION INT']
            },
            DeploymentModelType.MULTI_PRED_DOUBLE: {
                'prediction_call': 'MLMANAGER.PREDICT_KEY_VALUE',
                'column_vals': [f'"{cls}" DOUBLE' for cls in self.model.get_metadata(Metadata.CLASSES)]
            }
        }

        # Create the schema of the table (we use this a few times)
        schema_str = ''
        for i in df.columns:
            spark_data_type = schema_types[str(i)]
            assert spark_data_type in CONVERSIONS, f'Type {spark_data_type} not supported for table creation. Remove column and try again'
            schema_str += f'\t{i} {CONVERSIONS[spark_data_type]},'

    def create_model_deployment_table(self):
        """
        Creates the table that holds the columns of the feature vector as well as a unique MOMENT_ID
        """
        schema_str = self.model.get_metadata(Metadata.SCHEMA_STR)
        schema_table_name = f'{self.table_name}.{self.schema_name}'

        inspector = peer_into_splice_db(SQLAlchemyClient.engine)
        if self.table_name in set(inspector.get_table_names(schema=self.schema_name)):
            raise Exception(
                f'The table {schema_table_name} already exists. To deploy to an existing table, do not pass in a'
                f' dataframe and/or set create_model_table parameter=False')

        table_create_sql = f"""
                    CREATE TABLE {schema_table_name} (\
                    \tCUR_USER VARCHAR(50) DEFAULT CURRENT_USER,
                    \tEVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    \tRUN_ID VARCHAR(50) DEFAULT '{self.run_id}',
                    \n {self.schema_str}
                    """

        pk_cols = ''
        for key in self.primary_key:
            # If pk is already in the schema_string, don't add another column. PK may be an existing value
            if key[0] not in self.schema_str:
                table_create_sql += f'\t{key[0]} {key[1]},\n'
            pk_cols += f'{key[0]},'

        for col in self.prediction_data[self.model.get_metadata(Metadata.GENERIC_TYPE)]['column_vals']:
            table_create_sql += f'\t{col},\n'

        table_create_sql += f'\tPRIMARY KEY({pk_cols.rstrip(",")})\n)'

        logger.info(table_create_sql)
        self.session.execute(table_create_sql)
        self.session.commit()

    def alter_model_table(self):
        """
        Alters the provided table for deployment. Adds columns for storing model results as well as metadata such as
        current user, eval time, run_id, and the prediction label columns
        """

        # Table needs to exist
        schema_table_name = f'{self.table_name}.{self.schema_name}'
        inspector = peer_into_splice_db(SQLAlchemyClient.engine)
        if self.table_name not in set(inspector.get_table_names(schema=self.schema_name)):
            raise Exception(
                f'The table {schema_table_name} does not exist. To create a new table for deployment, '
                f'pass in a dataframe and set the set create_model_table=True')

        # Currently we only support deploying 1 model to a table
        table_cols = [col['name'] for col in inspector.get_columns(self.table_name, schema=self.schema_name)]
        reserved_fields = set(
            ['CUR_USER', 'EVAL_TIME', 'RUN_ID', 'PREDICTION'] + self.model.get_metadata(Metadata.CLASSES)
        )
        for col in table_cols:
            if col in reserved_fields:
                raise Exception(
                    f'The table {schema_table_name} looks like it already has values associated with '
                    f'a deployed model. Only 1 model can be deployed to a table currently.'
                    f'The table cannot have the following fields: {reserved_fields}')

        # Splice cannot currently add multiple columns in an alter statement so we need to make a bunch and execute
        # all of them
        alter_table_sql = []
        alter_table_syntax = f'ALTER TABLE {schema_table_name} ADD COLUMN'
        alter_table_sql.append(f'{alter_table_syntax} CUR_USER VARCHAR(50) DEFAULT CURRENT_USER')
        alter_table_sql.append(f'{alter_table_syntax} EVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        alter_table_sql.append(f'{alter_table_syntax} RUN_ID VARCHAR(50) DEFAULT \'{self.run_id}\'')

        # Add the correct prediction type
        for col in self.prediction_data[self.model.get_metadata(Metadata.GENERIC_TYPE)]['column_vals']:
            alter_table_sql += f'{alter_table_syntax} {col}'

        for sql in alter_table_sql:
            logger.info(sql)
            self.session.execute(sql)
        self.session.commit()

    def create_vti_prediction_trigger(self):
        """
        Create Trigger that uses VTI instead of parsing
        """
        model_type = self.model.get_metadata(Metadata.GENERIC_TYPE)
        classes = self.model.get_metadata(Metadata.CLASSES)
        prediction_call = "new com.splicemachine.mlrunner.MLRunner('key_value', '{run_id}', {raw_data}, '{schema_str}'"

        if model_type == DeploymentModelType.MULTI_PRED_DOUBLE:
            if 'predict_call' not in self.library_specific_args and 'predict_args' not in self.library_specific_args:
                # This must be a .transform call
                predict_call = 'transform'
                predict_args = None
            else:
                predict_call = self.library_specific_args.get('predict_call', 'predict')
                predict_args = self.library_specific_args.get('predict_args')

            prediction_call += f", '{predict_call}', '{predict_args}'"

        elif model_type == DeploymentModelType.MULTI_PRED_DOUBLE and len(classes) == 2 and \
                self.library_specific_args.get('pred_threshold'):
            prediction_call += f", '{self.library_specific_args.get('pred_threshold')}'"

        prediction_call += ')'  # Close the prediction call

        schema_table_name = f'{self.table_name}.{self.schema_name}'
        trigger_sql = f'CREATE TRIGGER {self.schema_name}.runModel_{self.table_name}_{self.run_id}\n\tAFTER INSERT\n ' \
                      f'\tON {schema_table_name}\n \tREFERENCING NEW AS NEWROW\n \tFOR EACH ROW\n \t\tUPDATE ' \
                      f'{schema_table_name} SET ('

        output_column_names = ''  # Names of the output columns from the model
        output_cols_vti_reference = ''  # Names references from the VTI (ie b.COL_NAME)
        output_cols_schema = ''  # Names with their datatypes (always DOUBLE for now)
        for cls in classes:
            output_column_names += f'"{cls}",'
            output_cols_vti_reference += f'b."{cls}",'
            output_cols_schema += f'"{cls}" DOUBLE,' if cls != 'prediction' else f'"{cls}" INT,'  # sk predict_proba

        raw_data = ''
        for index, col in enumerate(self.model_columns):
            raw_data += '||' if index != 0 else ''
            if self.schema_types[str(col)] == 'VARCHAR(5000)':
                raw_data += f'NEWROW.{col}||\',\''
            else:
                inner_cast = f'CAST(NEWROW.{col} as DECIMAL(38,10))' if \
                    self.schema_types[str(col)] in {'FLOAT', 'DOUBLE'} else f'NEWROW.{col}'
                raw_data += f'TRIM(CAST({inner_cast} as CHAR(41)))||\',\''

        # Cleanup (remove concatenation SQL from last variable) + schema for PREDICT call
        raw_data = raw_data[:-5].lstrip('||')
        schema_str_pred_call = self.schema_str.replace('\t', '').replace('\n', '').rstrip(',')

        prediction_call = prediction_call.format(run_id=self.run_id, raw_data=raw_data, schema_str=schema_str_pred_call)

        trigger_sql += f'{output_column_names[:-1]}) = ('
        trigger_sql += f'SELECT {output_cols_vti_reference[:-1]} FROM {prediction_call}' \
                       f' as b ({output_cols_schema[:-1]}) WHERE 1=1) WHERE '
        # 1=1 because of a DB bug that requires a where clause

        trigger_sql += ' AND '.join([f'{index[0]} = NEWROW.{index[0]}' for index in self.primary_key])
        # TODO - use the above syntax for other queries
        logger.info(trigger_sql)
        self.session.execute(trigger_sql)
        self.session.commit()

    def add_model_to_metadata_table(self):
        """
        Add the model to the deployed model metadata table
        """
        schema_table_name = f'{self.schema_name}.{self.table_name}'
        table_id = self.session.execute(f"""
            SELECT TABLEID FROM SYSVW.SYSTABLESVIEW WHERE TABLENAME='{self.table_name}' AND '{self.schema_name}'
        """).fetchone()[0]
        trigger_suffix = f"{schema_table_name.replace('.', '_')}_{self.run_id}".upper()

        trigger_1 = self.session.query(SysTriggers) \
            .filter_by(tableid=table_id, triggername=f"RUNMODEL_{trigger_suffix}") \
            .scalar()

        trigger_2 = self.session.query(SysTriggers) \
            .filter_by(tableid=table_id, triggername=f"PARSERESULT_{trigger_suffix}") \
            .scalar()

        # TODO Hardcoded to mlmanager user
        metadata = DatabaseDeployedMetadata(run_uuid=self.run_id, action='DEPLOYED', tableid=table_id,
                                            trigger_type='INSERT', triggerid=trigger_1.TRIGGERID,
                                            triggerid_2=trigger_2.TRIGGERID or 'NULL', db_env='PROD',
                                            db_user='mlmanager', action_date=str(trigger_1.CREATIONTIMESTAMP))
        self.session.add(metadata)
        self.session.commit()

    def create_prediction_trigger(self):
        """
        Create the actual prediction trigger for insertion
        """
        # The database function call is dependent on the model type
        prediction_call = self.prediction_data[self.model.get_metadata(Metadata.CLASSES)]['prediction_call']

        schema_table_name = f'{self.table_name}.{self.schema_name}'
        pred_trigger = f'CREATE TRIGGER {self.schema_name}.runModel_{self.table_name}_{self.run_id}\n \tBEFORE INSERT\n' \
                       f'\tON {schema_table_name}\n \tREFERENCING NEW AS NEWROW\n \tFOR EACH ROW\n \tBEGIN ATOMIC \t\t' \
                       f'SET NEWROW.PREDICTION={prediction_call}(\'{self.run_id}\','

        for index, col in enumerate(self.model_columns):
            pred_trigger += '||' if index != 0 else ''
            if self.schema_types[str(col)] == 'VARCHAR(5000)':
                pred_trigger += f'NEWROW.{col}||\',\''
            else:
                inner_cast = f'CAST(NEWROW.{col} as DECIMAL(38,10))' if \
                    self.schema_types[str(col)] in {'FLOAT', 'DOUBLE'} else f'NEWROW.{col}'
                pred_trigger += f'TRIM(CAST({inner_cast} as CHAR(41)))||\',\''

        # Cleanup + schema for PREDICT call
        pred_trigger = pred_trigger[:-5].lstrip('||') + ',\n\'' + self.schema_str.replace('\t', '').replace('\n', '') \
            .rstrip(',') + '\');END'

        logger.info(pred_trigger)
        self.session.execute(pred_trigger)
        self.session.commit()

    def create_parsing_trigger(self):
        """
        Creates the secondary trigger that parses the results of the first trigger and updates the prediction
        row populating the relevant columns.
        TO be removed when we move to VTI only
        """

        schema_table_name = f'{self.table_name}.{self.schema_name}'
        sql_parse_trigger = f'CREATE TRIGGER {self.schema_name}.PARSERESULT_{self.table_name}_{self.run_id}' \
                            f'\n \tBEFORE INSERT\n \tON {schema_table_name}\n \tREFERENCING NEW AS NEWROW\n' \
                            f' \tFOR EACH ROW\n \t\tBEGIN ATOMIC\n\t set '
        set_prediction_case_str = 'NEWROW.PREDICTION=\n\t\tCASE\n'
        for i, c in enumerate(self.model.get_metadata(Metadata.CLASSES)):
            sql_parse_trigger += f'NEWROW."{c}"=MLMANAGER.PARSEPROBS(NEWROW.prediction,{i}),'
            set_prediction_case_str += f'\t\tWHEN MLMANAGER.GETPREDICTION(NEWROW.prediction)={i} then \'{c}\'\n'

        set_prediction_case_str += '\t\tEND;'
        if self.model.get_metadata(Metadata.GENERIC_TYPE) == DeploymentModelType.MULTI_PRED_DOUBLE:
            # These models don't have an actual prediction
            sql_parse_trigger = sql_parse_trigger[:-1] + 'END'
        else:
            sql_parse_trigger += set_prediction_case_str + 'END'

        logger.info(sql_parse_trigger.replace('\n', ' ').replace('\t', ' '))
        self.session.execute(sql_parse_trigger.replace('\n', ' ').replace('\t', ' '))
        self.session.commit()

    def create(self):
        """
        Create
        :return:
        """
