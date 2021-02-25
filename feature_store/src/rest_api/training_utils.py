from fastapi import HTTPException, status
from typing import List, Union, Optional, Dict
from .schemas import Feature, FeatureSet, TrainingView, TrainingSet, TrainingSetMetadata
from . import crud
from sqlalchemy.orm import Session
from datetime import datetime
from .utils import __get_pk_columns
from shared.api.exceptions import SpliceMachineException, ExceptionCodes

"""
A set of utility functions for creating Training Set SQL 
"""

def dict_to_lower(dict):
    """
    Converts a dictionary to all lowercase keys

    :param dict: The dictionary
    :return: The lowercased dictionary
    """
    return {i.lower(): dict[i] for i in dict}


def _get_anchor_feature_set(features: List[Feature], feature_sets: List[FeatureSet]) -> FeatureSet:
    """
    From a dataframe of feature set rows, where each row has columns feature_set_id, schema_name, table_name
    and pk_cols where pk_cols is a pipe delimited string of Primary Key column names,
    this function finds which row has the superset of all primary key columns, raising an exception if none exist

    :param fset_keys: Pandas Dataframe containing FEATURE_SET_ID, SCHEMA_NAME, TABLE_NAME, and PK_COLUMNS, which
    is a | delimited string of column names
    :return: FeatureSet
    :raise: SpliceMachineException
    """
    # Get the Feature Set with the maximum number of primary key columns as the anchor
    anchor_fset = feature_sets[0]

    for fset in feature_sets:
        if len(__get_pk_columns(fset)) > len(__get_pk_columns(anchor_fset)):
            anchor_fset = fset

    # If Features are requested that come from Feature Sets that cannot be joined to our anchor, we will raise an
    # Exception and let the user know
    bad_features = []
    all_pk_cols = set(__get_pk_columns(anchor_fset))
    for fset in feature_sets:
        if not set(__get_pk_columns(fset)).issubset(all_pk_cols):
            bad_features += [f.name for f in features if f.feature_set_id == fset.feature_set_id]

    if bad_features:
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                    message=f"The provided features do not have a common join key."
                                     f"Remove features {bad_features} from your request")

    return anchor_fset


def _generate_training_set_history_sql(tvw: TrainingView, features: List[Feature],
                                       feature_sets: List[FeatureSet], start_time=None, end_time=None, create_time=None) -> str:
    """
    Generates the SQL query for creating a training set from a TrainingView and a List of Features.
    This performs the coalesces necessary to aggregate Features over time in a point-in-time consistent way

    :param tvw: The TrainingView
    :param features: List[Feature] The group of Features desired to be returned
    :param feature_sets: List[FeatureSets] the group of all Feature Sets of which Features are being selected
    :return: str the SQL necessary to execute
    """
    # SELECT clause
    sql = 'SELECT '
    for pkcol in tvw.pk_columns:  # Select primary key column(s)
        sql += f'\n\tctx.{pkcol},'

    sql += f'\n\tctx.{tvw.ts_column}, '  # Select timestamp column

    # TODO: ensure these features exist and fail gracefully if not
    for feature in features:
        sql += f'\n\tfset{feature.feature_set_id}h.{feature.name},'  # Collect all features over time

    # Select the optional label col
    if tvw.label_column:
        sql += f'\n\tctx.{tvw.label_column}'
    else:
        sql = sql.rstrip(',')

    # FROM clause
    sql += f'\nFROM ({tvw.view_sql}) ctx '

    # JOIN clause
    for fset in feature_sets:
        # Join Feature Set History
        sql += f'''\nLEFT OUTER JOIN ( 
                        SELECT h.*,coalesce(min(h.asof_ts) over (partition by h.customerid order by h.ASOF_TS ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), timestamp('{str(create_time)}')) until_ts 
                        FROM {fset.schema_name}.{fset.table_name}_history h
                        WHERE INGEST_TS <= timestamp('{str(create_time)}')     
                    ) fset{fset.feature_set_id}h
                        ON
                '''
        for pkcol in __get_pk_columns(fset):
            sql += f' fset{fset.feature_set_id}h.{pkcol}=ctx.{pkcol} AND '
        sql += f' ctx.{tvw.ts_column} >= fset{fset.feature_set_id}h.ASOF_TS AND ctx.{tvw.ts_column} < fset{fset.feature_set_id}h.UNTIL_TS'

    # WHERE clause on optional start and end times
    if start_time or end_time:
        sql += '\nWHERE '
        if start_time:
            sql += f"\n\tctx.{tvw.ts_column} >= '{str(start_time)}' AND"
        if end_time:
            sql += f"\n\tctx.{tvw.ts_column} <= '{str(end_time)}'"
        sql = sql.rstrip('AND')
    return sql


def _generate_training_set_sql(features: List[Feature], feature_sets: List[FeatureSet]) -> str:
    """
    Generates the SQL query for creating a training set from a List of Features (NO TrainingView).

    :param features: List[Feature] The group of Features desired to be returned
    :param feature_sets: List of Feature Sets
    :return: str the SQL necessary to execute
    """
    anchor_fset: FeatureSet = _get_anchor_feature_set(features, feature_sets)
    alias = f'fset{anchor_fset.feature_set_id}'  # We use this a lot for joins
    anchor_fset_schema = f'{anchor_fset.schema_name}.{anchor_fset.table_name} {alias} '
    remaining_fsets = [fset for fset in feature_sets if fset != anchor_fset]

    # SELECT clause
    feature_names = ','.join([f'fset{feature.feature_set_id}.{feature.name}' for feature in features])
    # Include the pk columns of the anchor feature set
    pk_cols = ','.join([f'{alias}.{pk}' for pk in __get_pk_columns(anchor_fset)])
    all_feature_columns = feature_names + ',' + pk_cols

    sql = f'SELECT {all_feature_columns} \nFROM {anchor_fset_schema}'

    # JOIN clause
    for fset in remaining_fsets:
        # Join Feature Set
        sql += f'\nLEFT OUTER JOIN {fset.schema_name}.{fset.table_name} fset{fset.feature_set_id} \n\tON '
        for ind, pkcol in enumerate(__get_pk_columns(fset)):
            if ind > 0: sql += ' AND '  # In case of multiple columns
            sql += f'fset{fset.feature_set_id}.{pkcol}={alias}.{pkcol}'
    return sql


def _create_temp_training_view(features: List[Feature], feature_sets: List[FeatureSet]) -> TrainingView:
    """
    Internal function to create a temporary Training View for training set retrieval using a Feature Set. When
    a user created

    :param fsets: List[FeatureSet]
    :param features: List[Feature]
    :return: Generated Training View
    """
    anchor_fset = _get_anchor_feature_set(features, feature_sets)
    anchor_pk_column_sql = ','.join(__get_pk_columns(anchor_fset))
    ts_col = 'LAST_UPDATE_TS'
    schema_table_name = f'{anchor_fset.schema_name}.{anchor_fset.table_name}_history'
    view_sql = f'SELECT {anchor_pk_column_sql}, ASOF_TS as {ts_col} FROM {schema_table_name}'
    return TrainingView(view_id=0, pk_columns=__get_pk_columns(anchor_fset), ts_column=ts_col, view_sql=view_sql,
                        description=None, name=None, label_column=None)

def _get_training_view_by_name(db: Session, name: str) -> List[TrainingView]:
    tvs = crud.get_training_views(db, {'name': name})
    if not tvs:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f'Could not find training view with name "{name}"')
    return tvs

def _get_training_set(db: Session, features: Union[List[Feature], List[str]], create_time: datetime, start_time: datetime = None, 
                            end_time: datetime = None, current: bool = False) -> TrainingSet:
    # Get List[Feature]
    features = crud.process_features(db, features)

    # Get the Feature Sets
    fsets = crud.get_feature_sets(db, list({f.feature_set_id for f in features}))

    if current:
        sql = _generate_training_set_sql(features, fsets)
    else:
        temp_vw = _create_temp_training_view(features, fsets)
        sql = _generate_training_set_history_sql(temp_vw, features, fsets, start_time=start_time, end_time=end_time, create_time=create_time)
    
    metadata = TrainingSetMetadata(training_set_start_ts=start_time, training_set_end_ts=end_time, training_set_create_ts=create_time)
    return TrainingSet(sql=sql, features=features, metadata=metadata)

def _get_training_set_from_view(db: Session, view: str, create_time: datetime, features: Union[List[Feature], List[str]] = None, 
                                start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> TrainingSet:
    # Get features as list of Features
    features = crud.process_features(db, features) if features else crud.get_training_view_features(db, view)

    # Get List of necessary Feature Sets
    feature_set_ids = list({f.feature_set_id for f in features})  # Distinct set of IDs
    feature_sets = crud.get_feature_sets(db, feature_set_ids)

    # Get training view information (view primary key column(s), inference ts column, )
    tvw = _get_training_view_by_name(db, view)[0]

    # Generate the SQL needed to create the dataset
    sql = _generate_training_set_history_sql(tvw, features, feature_sets, start_time=start_time, end_time=end_time, create_time=create_time)
    metadata = TrainingSetMetadata(training_set_start_ts=start_time, training_set_end_ts=end_time, training_set_create_ts=create_time)
    return TrainingSet(sql=sql, training_view=tvw, features=features, metadata=metadata)
