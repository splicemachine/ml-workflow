"""
Class to prepare database models for deployment
to Splice Machine
"""
from typing import List, Dict
from collections import namedtuple

from shared.models.enums import FileExtensions
from shared.models.model_types import SparkModelType, KerasModelType, SklearnModelType, H2OModelType
from shared.logger.logging_config import logger
from enum import Enum

from .preparation.spark_utils import SparkUtils
from .preparation.keras_utils import KerasUtils
from .preparation.sklearn_utils import ScikitUtils

class DatabaseModelDDL:
    """
    Create tables and triggers for DB deployment
    """
    def __init__(self):
        self.model_type: Enum = None
        self.run_id = None
        self.schema_name = None
        self.table_name = None
        self.model_columns: List[str] = [] # The model_cols parameter
        self.schema_types: Dict[str,str] = {} # The mapping of model column to data type
        self.schema_str: str = '' # Schema_types represented as a string (col_name TYPE,) may not be necessary because of schema_types
        self.primary_key = None
        self.classes: List[str] = []
        self.sklearn_args: Dict[str,str] = None
        self.keras_pred_threshold: float = None # The optional keras prediction threshold for predictions

        self.prediction_data = {
            SparkModelType.CLASSIFICATION: {
                'prediction_call':'MLMANAGER.PREDICT_CLASSIFICATION',
                'column_vals': ['PREDICTION VARCHAR(5000)'] + [f'"{i}" DOUBLE' for i in self.classes]
            },
            H2OModelType.CLASSIFICATION: {
                'predict_call':'MLMANAGER.PREDICT_CLASSIFICATION',
                'column_vals': ['PREDICTION VARCHAR(5000)'] + [f'"{i}" DOUBLE' for i in self.classes]
            },
            SparkModelType.CLUSTERING_WITH_PROB: {
                'prediction_call':'MLMANAGER.PREDICT_CLUSTER_PROBABILITIES',
                'column_vals': ['PREDICTION VARCHAR(5000)'] + [f'"{i}" DOUBLE' for i in self.classes]
            },
            SparkModelType.REGRESSION: {
                'prediction_call':'MLMANAGER.PREDICT_REGRESSION',
                'column_vals': ['PREDICTION DOUBLE']
            },
            H2OModelType.REGRESSION: {
                'prediction_call':'MLMANAGER.PREDICT_REGRESSION',
                'column_vals': ['PREDICTION DOUBLE']
            },
            SklearnModelType.REGRESSION: {
                'prediction_call':'MLMANAGER.PREDICT_REGRESSION',
                'column_vals': ['PREDICTION DOUBLE']
            },
            KerasModelType.REGRESSION: {
                'prediction_call':'MLMANAGER.PREDICT_REGRESSION',
                'column_vals': ['PREDICTION DOUBLE']
            },
            SparkModelType.CLUSTERING_WO_PROB: {
                'prediction_call':'MLMANAGER.PREDICT_CLUSTER',
                'column_vals': ['PREDICTION INT']
            },
            H2OModelType.SINGULAR: {
                'prediction_call':'MLMANAGER.PREDICT_CLUSTER',
                'column_vals': ['PREDICTION INT']
            },
            SklearnModelType.POINT_PREDICTION_CLF: {
                'prediction_call':'MLMANAGER.PREDICT_CLUSTER',
                'column_vals': ['PREDICTION INT']
            },
            H2OModelType.KEY_VALUE: {
                'prediction_call':'MLMANAGER.PREDICT_KEY_VALUE',
                'column_vals': [f'"{i}" DOUBLE' for i in self.classes]
            },
            SklearnModelType.KEY_VALUE: {
                'prediction_call':'MLMANAGER.PREDICT_KEY_VALUE',
                'column_vals': [f'"{i}" DOUBLE' for i in self.classes]
            },
            KerasModelType.KEY_VALUE: {
                'prediction_call':'MLMANAGER.PREDICT_KEY_VALUE',
                'column_vals': [f'"{i}" DOUBLE' for i in self.classes]
            }
        }

    def create_model_deployment_table(self):
        """
        Creates the table that holds the columns of the feature vector as well as a unique MOMENT_ID
        """
        schema_table_name = f'{self.table_name}.{self.schema_name}'
        if splice_context.tableExists(schema_table_name): #FIXME: Check if table exists without NSDS
            raise Exception( #FIXME: Not sure how we're doing errors
                f'The table {self.schema_table_name} already exists. To deploy to an existing table, do not pass in a dataframe '
                f'and/or set create_model_table parameter=False')

        SQL_TABLE = f"""
                    CREATE TABLE {schema_table_name} (\
                    \tCUR_USER VARCHAR(50) DEFAULT CURRENT_USER,
                    \tEVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    \tRUN_ID VARCHAR(50) DEFAULT '{self.run_id}',
                    \n {self.schema_str}
                    """

        pk_cols = ''
        for i in self.primary_key:
            # If pk is already in the schema_string, don't add another column. PK may be an existing value
            if i[0] not in self.schema_str:
                SQL_TABLE += f'\t{i[0]} {i[1]},\n'
            pk_cols += f'{i[0]},'

        for col in self.prediction_data[self.model_type]['column_vals']:
            SQL_TABLE += f'\t{col},\n'

        SQL_TABLE += f'\tPRIMARY KEY({pk_cols.rstrip(",")})\n)'

        ##TODO: Execute SQL_TABLE DDL


def alter_model_table(self):
    """
    Alters the provided table for deployment. Adds columns for storing model results as well as metadata such as
    current user, eval time, run_id, and the prediction label columns
    """

    # Table needs to exist
    schema_table_name = f'{self.table_name}.{self.schema_name}'
    if not splice_context.tableExists(schema_table_name): #FIXME: Check if table exists without NSDS
        raise Exception( #FIXME: Not sure how we're handling errors
            f'The table {schema_table_name} does not exist. To create a new table for deployment, pass in a dataframe and '
            f'The set create_model_table=True')

    # Currently we only support deploying 1 model to a table
    table_cols = []#TODO: Get column names of table
    reserved_fields = set(['CUR_USER', 'EVAL_TIME', 'RUN_ID', 'PREDICTION'] + self.classes)
    for col in table_cols:
        if col in reserved_fields:
            raise Exception( #FIXME: Not sure how we're handling errors
                f'The table {schema_table_name} looks like it already has values associated with '
                f'a deployed model. Only 1 model can be deployed to a table currently.'
                f'The table cannot have the following fields: {reserved_fields}')

    # Splice cannot currently add multiple columns in an alter statement so we need to make a bunch and execute all of them
    SQL_ALTER_TABLE = []
    alter_table_syntax = f'ALTER TABLE {schema_table_name} ADD COLUMN'
    SQL_ALTER_TABLE.append(f'{alter_table_syntax} CUR_USER VARCHAR(50) DEFAULT CURRENT_USER')
    SQL_ALTER_TABLE.append(f'{alter_table_syntax} EVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
    SQL_ALTER_TABLE.append(f'{alter_table_syntax} RUN_ID VARCHAR(50) DEFAULT \'{self.run_id}\'')

    # Add the correct prediction type
    for col in self.prediction_data[self.model_type]['column_vals']:
        SQL_ALTER_TABLE += f'{alter_table_syntax} {col}'

    for sql in SQL_ALTER_TABLE:
        splice_context.execute(sql) #FIXME: execute with sqlalchemy


    def create_vti_prediction_trigger(self):
        prediction_call = "new com.splicemachine.mlrunner.MLRunner('key_value', '{run_id}', {raw_data}, '{schema_str}'"

        if self.model_type == SklearnModelType.KEY_VALUE:
            if not self.sklearn_args:  # This must be a .transform call
                predict_call, predict_args = 'transform', None
            else:
                predict_call = self.sklearn_args.get('predict_call','predict')
                predict_args = self.sklearn_args.get('predict_args')

            prediction_call += f", '{predict_call}', '{predict_args}'"

        elif self.model_type == KerasModelType.KEY_VALUE and len(self.classes) == 2 and self.keras_pred_threshold:
            prediction_call += f", '{self.keras_pred_threshold}'"

        prediction_call += ')' # Close the prediction call

        schema_table_name = f'{self.table_name}.{self.schema_name}'
        SQL_PRED_TRIGGER = f'CREATE TRIGGER {self.schema}.runModel_{self.table_name}_{self.run_id}\n \tAFTER INSERT\n ' \
                           f'\tON {schema_table_name}\n \tREFERENCING NEW AS NEWROW\n \tFOR EACH ROW\n \t\tUPDATE ' \
                           f'{schema_table_name} SET ('

        output_column_names = ''  # Names of the output columns from the model
        output_cols_VTI_reference = ''  # Names references from the VTI (ie b.COL_NAME)
        output_cols_schema = ''  # Names with their datatypes (always DOUBLE for now)
        for i in self.classes:
            output_column_names += f'"{i}",'
            output_cols_VTI_reference += f'b."{i}",'
            output_cols_schema += f'"{i}" DOUBLE,' if i != 'prediction' else f'"{i}" INT,'  # for sklearn predict_proba

        raw_data = ''
        for i, col in enumerate(self.model_columns):
            raw_data += '||' if i != 0 else ''
            if self.schema_types[str(col)] == 'StringType':
                raw_data += f'NEWROW.{col}||\',\''
            else:
                inner_cast = f'CAST(NEWROW.{col} as DECIMAL(38,10))' if self.schema_types[str(col)] in {'FloatType',
                                                                                                   'DoubleType',
                                                                                                   'DecimalType'} else f'NEWROW.{col}'
                raw_data += f'TRIM(CAST({inner_cast} as CHAR(41)))||\',\''

        # Cleanup (remove concatenation SQL from last variable) + schema for PREDICT call
        raw_data = raw_data[:-5].lstrip('||')
        schema_str_pred_call = self.schema_str.replace('\t', '').replace('\n', '').rstrip(',')

        prediction_call = prediction_call.format(run_id=self.run_id, raw_data=raw_data, schema_str=schema_str_pred_call)

        SQL_PRED_TRIGGER += f'{output_column_names[:-1]}) = ('
        SQL_PRED_TRIGGER += f'SELECT {output_cols_VTI_reference[:-1]} FROM {prediction_call}' \
                        f' as b ({output_cols_schema[:-1]}) WHERE 1=1) WHERE ' # 1=1 because of a DB bug that requires a where clause

        # Set the outer where clause
        for i in self.primary_key:
            SQL_PRED_TRIGGER += f'{i[0]} = NEWROW.{i[0]} AND'
        # Remove last AND
        SQL_PRED_TRIGGER = SQL_PRED_TRIGGER[:-3]

        # TODO: sqlalchemy execute the SQL_PRED_TRIGGER DDL

    def create_prediction_trigger(self):
        # The database function call is dependent on the model type
        prediction_call = self.prediction_data[self.model_type]['prediction_call']

        schema_table_name = f'{self.table_name}.{self.schema_name}'
        SQL_PRED_TRIGGER = f'CREATE TRIGGER {self.schema_name}.runModel_{self.table_name}_{self.run_id}\n \tBEFORE INSERT\n ' \
                       f'\tON {schema_table_name}\n \tREFERENCING NEW AS NEWROW\n \tFOR EACH ROW\n \tBEGIN ATOMIC \t\t' \
                       f'SET NEWROW.PREDICTION={prediction_call}(\'{self.run_id}\','

        for i, col in enumerate(self.model_columns):
            SQL_PRED_TRIGGER += '||' if i != 0 else ''
            if self.schema_types[str(col)] == 'StringType':
                SQL_PRED_TRIGGER += f'NEWROW.{col}||\',\''
            else:
                inner_cast = f'CAST(NEWROW.{col} as DECIMAL(38,10))' if self.schema_types[str(col)] in {'FloatType',
                                                                                                   'DoubleType',
                                                                                                   'DecimalType'} else f'NEWROW.{col}'
                SQL_PRED_TRIGGER += f'TRIM(CAST({inner_cast} as CHAR(41)))||\',\''

        # Cleanup + schema for PREDICT call
        SQL_PRED_TRIGGER = SQL_PRED_TRIGGER[:-5].lstrip('||') + ',\n\'' + \
                           self.schema_str.replace('\t', '').replace('\n','').rstrip(',') + '\');END'

        #TODO: Execute SQL_PRED_TRIGGER DDL
