from fastapi import status
from .schemas import FeatureSetBase, FeatureSet
from .constants import Columns
from shared.api.exceptions import SpliceMachineException, ExceptionCodes

def __validate_feature_data_type(feature_data_type: str):
    """
    Validated that the provided feature data type is a valid SQL data type
    :param feature_data_type: the feature data type
    :return: None
    """
    from .constants import SQL_TYPES
    if not feature_data_type.split('(')[0] in SQL_TYPES:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, codee=ExceptionCodes.INVALID_TYPE,
                                     message=f"The datatype you've passed in, {feature_data_type} is not a valid SQL type. "
                                     f"Valid types are {SQL_TYPES}")

def get_pk_schema_str(fset: FeatureSet):
    return ','.join([f'\n\t{k} {fset.primary_keys[k]}' for k in fset.primary_keys])

def get_pk_column_str(fset: FeatureSet, history=False):
    if history:
        return ','.join(__get_pk_columns(fset) + Columns.history_table_pk)
    return ','.join(__get_pk_columns(fset))

def __get_pk_columns(fset: FeatureSetBase):
    return list(fset.primary_keys.keys())