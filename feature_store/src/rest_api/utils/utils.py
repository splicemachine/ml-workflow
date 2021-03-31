from fastapi import status
from ..schemas import FeatureSetBase, FeatureSet, DataType
from ..constants import Columns, SQLALCHEMY_TYPES
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from typing import Dict, List
import shared.models.feature_store_models as models
from .. import schemas
from sqlalchemy import Column, VARCHAR, DECIMAL
import json
import re

def __validate_feature_data_type(feature_data_type: DataType):
    """
    Validates that the provided feature data type is a valid SQL data type
    :param feature_data_type: the feature data type
    :return: None
    """
    from ..constants import SQL_TYPES
    if not feature_data_type.data_type in SQL_TYPES:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f"The datatype you've passed in, {feature_data_type} is not a valid SQL type. "
                                     f"Valid types are {SQL_TYPES}")
    if feature_data_type.data_type.upper() == 'VARCHAR' and not feature_data_type.length:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message='The VARCHAR provided does not have a length. Varchars MUST include a length!')

def __validate_primary_keys(pks: Dict[str, DataType]):
    """
    Validates that the provided feature data type is a valid SQL data type for each primary key
    :param feature_data_type: the feature data type
    :return: None
    """
    for pk in pks:
        if not re.match('^[A-Za-z][A-Za-z0-9_]*$', pk, re.IGNORECASE):
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.INVALID_FORMAT,
                                     message=f'PK Column {pk} does not conform. Must start with an alphabetic character, '
                                     'and can only contains letters, numbers and underscores')
        try:
            __validate_feature_data_type(pks[pk])
        except SpliceMachineException as e:
            raise SpliceMachineException(status_code=e.status_code, code=e.code,
                                         message=f'Error with Primary Key {pk}: {e.message}')


def get_pk_column_str(fset: FeatureSet, history=False):
    if history:
        return ','.join(__get_pk_columns(fset) + Columns.history_table_pk)
    return ','.join(__get_pk_columns(fset))

def __get_pk_columns(fset: FeatureSetBase):
    return list(fset.primary_keys.keys())

def datatype_to_sql(typ: DataType) -> str:
    """
    Processes a DataType into the SQL compatible string representation
    Ex:
        datatype_to_sql(DataType('VARCHAR',length=20)) -> VARCHAR(20)
        datatype_to_sql(DataType('DECIMAL',precision=20, scale=3)) -> DECIMAL(20,3)

    :param typ: The Data Type
    :return: str the qualified SQL type
    """
    sql_type = typ.data_type
    if typ.length:
        sql_type += f'({typ.length})'
    elif typ.precision:
        sql_type += f'({typ.precision}'
        if typ.scale:
            sql_type += f',{typ.scale}'
        sql_type += ')'
    return sql_type

def sql_to_datatype(typ: str) -> DataType:
    """
    Converts a SQL datatype to a DataType object
    ex:
        sql_to_datatype('VARCHAR(50)') -> DataType(data_type= 'VARCHAR',length=50)
        sql_to_datatype('DECIMAL(10,2)') -> DataType(data_type= 'DECIMAL',precision=10,scale=2)
    :param typ: the SQL data type
    :return: DataType
    """
    # If it's a type that has params and those params have been set
    if typ in ('DECIMAL', 'FLOAT','NUMERIC') and len(typ.split('(')) == 2:
        dtype, params = typ.split('(')
        if ',' in params:
            prec, scale = params.strip(')').split(',')
        else:
            prec, scale = params.strip(')'), None
        data_type = DataType(data_type=typ, precision=prec, scale=scale)
    # If it's a type VARCHAR that has a length
    elif typ == 'VARCHAR' and len(typ.split('(')) == 2:
        dtype, length = typ.split('(')
        data_type = DataType(data_type=typ, length=length.strip(')'))
    else:
        data_type = DataType(data_type=typ)
    return data_type

def _sql_to_sqlalchemy_columns(sql_cols: Dict[str,schemas.DataType], pk: bool=False) -> List[Column]:
    """
    Takes a dictionary of column_name, column_type and returns a list of SQLAlchemy Columns with the proper SQLAlchemy
    types
    :param sql_types: List of SQL data types
    :param pk: If the list of columns are primary keys
    :return: List of SQLAlchemy Columns
    """
    cols = []
    for k in sql_cols:
        sql_type = sql_cols[k]
        if sql_type.data_type.upper() == 'VARCHAR': # Extract the length (eg VARCHAR(20))
            cols.append(Column(k.lower(), VARCHAR(sql_type.length), primary_key=pk))
        elif sql_type.data_type.upper() == 'DECIMAL': # Extract precision and scale (eg DECIMAL(10,2))
            cols.append(Column(k.lower(), DECIMAL(sql_type.precision, sql_type.scale), primary_key=pk))
        else:
            cols.append(Column(k.lower(), SQLALCHEMY_TYPES._get(sql_cols[k]), primary_key=pk))
    return cols

def model_to_schema_feature(feat: models.Feature) -> schemas.Feature:
    """
    A function that converts a models.Feature into a schemas.Feature through simple manipulations.
    Splice Machine does not support complex data types like JSON or Arrays, so we stringify them and store them as
    Strings in the database, so they need some manipulation when we retrieve them.
        * Turns tags into a list
        * Turns attributes into a Dict
        * Turns feature_data_type into a DataType object (dict)
    :param feat: The feature from the database
    :return: The schemas.Feature representation
    """
    f = feat.__dict__
    f['tags'] = f['tags'].split(',') if f.get('tags') else None
    f['attributes'] = json.loads(f['attributes']) if f.get('attributes') else None
    f['feature_data_type'] = sql_to_datatype(f['feature_data_type'])
    return schemas.Feature(**f)
