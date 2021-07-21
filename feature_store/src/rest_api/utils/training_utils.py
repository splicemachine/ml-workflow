from datetime import datetime
from json import dumps
from typing import Any, List, Optional, Tuple, Union

from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import crud
from schemas import (Feature, FeatureSet, FeatureSetDetail, 
                        TrainingSet, TrainingSetMetadata, TrainingViewDetail)
from sqlalchemy.orm import Session
from utils.utils import __get_pk_columns
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.logger.logging_config import logger

"""
A set of utility functions for creating Training Set SQL 
"""

TRAINING_SET_MAX_SIZE = 5e7 # 50MB

def dict_to_lower(dict):
    """
    Converts a dictionary to all lowercase keys

    :param dict: The dictionary
    :return: The lowercased dictionary
    """
    return {i.lower(): dict[i] for i in dict}


def _get_anchor_feature_set(features: List[Feature], feature_sets: List[FeatureSetDetail], label: Feature = None) -> FeatureSetDetail:
    """
    From a list of features and corresponding feature sets, this function finds which feature set 
    has the superset of all primary key columns, raising an exception if none exist

    :param features: List[Features]
    :param feature_sets: List[FeatureSet]
    :param label: (Optional) label for the training set
    :return: FeatureSet
    :raise: SpliceMachineException
    """
    if label:
        anchor_fset = next(filter(lambda fs: fs.feature_set_id == label.feature_set_id, feature_sets))
    else:
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
        explainer = f' with label {label.name}' if label else ''
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                    message=f"The provided features do not have a common join key{explainer}."
                                     f"Remove features {bad_features} from your request")

    return anchor_fset


def _generate_training_set_history_sql(tvw: TrainingViewDetail, features: List[Feature], feature_sets: List[FeatureSetDetail], 
                                        start_time=None, end_time=None, create_time=None, 
                                        return_pk_cols: bool = False, return_ts_col: bool = False) -> str:
    """
    Generates the SQL query for creating a training set from a TrainingView and a List of Features.
    This performs the coalesces necessary to aggregate Features over time in a point-in-time consistent way

    :param tvw: The TrainingView
    :param features: List[Feature] The group of Features desired to be returned
    :param feature_sets: List[FeatureSets] the group of all Feature Sets of which Features are being selected
    :param start_time: datetime The start time for the Training Set
    :param end_time: datetime The end time for the Training Set
    :param create_time: datetime The creation time for the Training Set
    :param return_pk_cols: bool Whether or not the returned sql should include the primary key column(s)
    :param return_ts_cols: bool Whether or not the returned sql should include the timestamp column
    :return: str the SQL necessary to execute
    """
    # SELECT clause
    sql = 'SELECT '
    
    if return_pk_cols:
        for pkcol in tvw.pk_columns:  # Select primary key column(s)
            sql += f'\n\tctx.{pkcol},'

    if return_ts_col:
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
    sql += f'\nFROM ({tvw.sql_text}) ctx '

    # JOIN clause
    for fset in feature_sets:
        pkcols = __get_pk_columns(fset)
        # Join Feature Set History
        sql += ("\nLEFT OUTER JOIN ("
                f"\n\tSELECT h.*,coalesce(min(h.asof_ts) over (partition by {','.join(pkcols)} order by h.ASOF_TS ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), timestamp('{str(create_time)}')) until_ts "
                f"\n\tFROM {fset.schema_name}.{fset.versioned_table}_history h "
                f"\n\tWHERE INGEST_TS <= timestamp('{str(create_time)}')"
                f"\n) fset{fset.feature_set_id}h "
                "\nON"
                )
        for pkcol in pkcols:
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


def _generate_training_set_sql(features: List[Feature], feature_sets: List[FeatureSetDetail], label: Feature = None, return_pk_cols = False) -> str:
    """
    Generates the SQL query for creating a training set from a List of Features (NO TrainingView).

    :param features: List[Feature] The group of Features desired to be returned
    :param feature_sets: List of Feature Sets
    :param label: str (Optional) Label for the training set
    :param return_pk_cols: bool Whether or not the returned sql should include the primary key column(s)
    :return: str the SQL necessary to execute
    """
    anchor_fset: FeatureSetDetail = _get_anchor_feature_set(features, feature_sets, label)
    alias = f'fset{anchor_fset.feature_set_id}'  # We use this a lot for joins
    anchor_fset_schema = f'{anchor_fset.schema_name}.{anchor_fset.versioned_table} {alias} '
    remaining_fsets = [fset for fset in feature_sets if fset != anchor_fset]

    # SELECT clause
    feature_names = ','.join([f'fset{feature.feature_set_id}.{feature.name}' for feature in features])
    # Include the pk columns of the anchor feature set
    pk_cols = ','.join([f'{alias}.{pk}' for pk in __get_pk_columns(anchor_fset)])
    all_feature_columns = feature_names
    if return_pk_cols:
        all_feature_columns += ',' + pk_cols

    sql = f'SELECT {all_feature_columns} \nFROM {anchor_fset_schema}'

    # JOIN clause
    for fset in remaining_fsets:
        # Join Feature Set
        sql += f'\nLEFT OUTER JOIN {fset.schema_name}.{fset.versioned_table} fset{fset.feature_set_id} \n\tON '
        for ind, pkcol in enumerate(__get_pk_columns(fset)):
            if ind > 0: sql += ' AND '  # In case of multiple columns
            sql += f'fset{fset.feature_set_id}.{pkcol}={alias}.{pkcol}'
    return sql


def _create_temp_training_view(features: List[Feature], feature_sets: List[FeatureSet], create_time: datetime, label: Feature = None) -> TrainingViewDetail:
    """
    Internal function to create a temporary Training View for training set retrieval using a Feature Set. When
    a user created

    :param fsets: List[FeatureSet]
    :param features: List[Feature]
    :param create_time: datetime The creation time for the Training Set
    :param label: str (Optional) Label for the training view
    :return: Generated Training View
    """
    anchor_fset = _get_anchor_feature_set(features, feature_sets, label)
    anchor_columns = __get_pk_columns(anchor_fset)
    if label:
        anchor_columns.append(label.name)
    anchor_column_sql = ', '.join(anchor_columns)
    ts_col = 'LAST_UPDATE_TS'
    schema_table_name = f'{anchor_fset.schema_name}.{anchor_fset.versioned_table}_history'
    sql_text = f"SELECT {anchor_column_sql}, ASOF_TS as {ts_col} FROM {schema_table_name} WHERE INGEST_TS <= timestamp('{str(create_time)}')"
    return TrainingViewDetail(view_id=None, pk_columns=__get_pk_columns(anchor_fset), ts_column=ts_col, sql_text=sql_text,
                        description=None, name=None, label_column=(label.name if label else None))

def _get_training_view_by_name(db: Session, name: str, version: Optional[Union[str, int]] = 'latest') -> List[TrainingViewDetail]:
    """
    Internal function to retrieve a training view from the db by name
    :param db: Session The database connection
    :param name: str The Training View name
    :param version: str | int The Training View version
    :return: Training View
    :raise: SpliceMachineException
    """
    tvs = crud.get_training_views(db, {'name': name, 'view_version': version})
    if not tvs:
        v = f' and version {version}' if version != 'latest' else ''
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f'Could not find training view with name "{name}"{v}')
    return tvs

def _get_training_set(db: Session, features: Union[List[Feature], List[str]], create_time: datetime,
                      start_time: datetime = None, end_time: datetime = None, current: bool = False, label: str = None,
                      return_pk_cols: bool = False, return_ts_col: bool = False, return_type: Optional[str] = None
                      ) -> TrainingSet:
    """
    Creates a training set without a training view from a list of features
    :param db: Session The database connection
    :param features: List[Feature] The group of Features desired to be returned
    :param create_time: datetime The creation time for the Training Set
    :param start_time: datetime The start time for the Training Set
    :param end_time: datetime The end time for the Training Set
    :param current: bool If you only want the most recent values of the features
    :param label: str (Optional) Label for the training set
    :param return_pk_cols: bool Whether or not the returned sql should include the primary key column(s)
    :param return_ts_cols: bool Whether or not the returned sql should include the timestamp column
    :param return_type: A particular return type for the training set. Currently available options are 'json' and 'sql'
    :return: Training Set or a JsonResponse (if the user requested the data in json format).
        We do this because jsonifying the pydantic schema takes many minutes, and returning the raw JSON is much faster
    :raise: SpliceMachineException
    """
    if label:
        if any([(f if isinstance(f, str) else f.name).lower() == label.lower() for f in features]):
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                        message=f'Label column cannot be in the provided Feature list')
        features.append(label)
    
    # Get List[Feature]
    features = crud.process_features(db, features)

    # Get the Feature Sets
    fsets = crud.get_feature_sets(db, list({f.feature_set_id for f in features}))

    # Get the label Feature
    if label:
        ind = next((i for i, f in enumerate(features) if f.name.lower() == label.lower()), None)
        if not ind:
            raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Unable to find label feature '{label}'")
        label = features.pop(ind)

    if current:
        temp_vw = None
        sql = _generate_training_set_sql(features, fsets, label, return_pk_cols)
    else:
        temp_vw = _create_temp_training_view(features, fsets, create_time, label)
        sql = _generate_training_set_history_sql(temp_vw, features, fsets, start_time=start_time, end_time=end_time, create_time=create_time,
            return_pk_cols=return_pk_cols, return_ts_col=return_ts_col)
    
    metadata = TrainingSetMetadata(training_set_start_ts=start_time or datetime(year=1900,month=1,day=1),
                                   training_set_end_ts=end_time or datetime.today(), training_set_create_ts=create_time)
    if label:
        features.append(label)

    ts = TrainingSet(sql=sql, training_view=temp_vw, features=features, metadata=metadata)

    # We will add more return types in the future so we leave it as a string not a bool
    if return_type == 'json':
        d = get_training_set_data(db, ts, return_type)
        ts.data = dumps(d, default=str)

    return ts

def _get_training_set_from_view(db: Session, view: str, create_time: datetime, features: Union[List[Feature], 
                                List[str]] = None, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None, 
                                return_pk_cols: bool = False, return_ts_col: bool = False, return_type: Optional[str] = None) -> TrainingSet:
    """
    Creates a training set from a training view
    :param db: Session The database connection
    :param view: str The name of the Training View
    :param create_time: datetime The creation time for the Training Set
    :param features: List[Feature] The group of Features desired to be returned
    :param start_time: datetime The start time for the Training Set
    :param end_time: datetime The end time for the Training Set
    :param return_pk_cols: bool Whether or not the returned sql should include the primary key column(s)
    :param return_ts_cols: bool Whether or not the returned sql should include the timestamp column
    :param return_type: A particular return type for the training set. Currently available options are 'json' and 'sql'
    :return: Training Set
    :raise: SpliceMachineException
    """
    # Get training view information (view primary key column(s), inference ts column, )
    tvw = _get_training_view_by_name(db, view)[0]

    # Get features as list of Features
    features = crud.process_features(db, features) if features else crud.get_training_view_features(db, tvw)

    # Get List of necessary Feature Sets
    feature_set_ids = list({f.feature_set_id for f in features})  # Distinct set of IDs
    feature_sets = crud.get_feature_sets(db, feature_set_ids)

    # Generate the SQL needed to create the dataset
    sql = _generate_training_set_history_sql(tvw, features, feature_sets, start_time=start_time, end_time=end_time, create_time=create_time,
        return_pk_cols=return_pk_cols, return_ts_col=return_ts_col)
    metadata = TrainingSetMetadata(training_set_start_ts=start_time or datetime(year=1900,month=1,day=1),
                                   training_set_end_ts=end_time or datetime.today(), training_set_create_ts=create_time,
                                   view_id=tvw.view_id, view_version=tvw.view_version)

    ts = TrainingSet(sql=sql, training_view=tvw, features=features, metadata=metadata)
    # We will add more return types in the future so we leave it as a string not a bool
    if return_type == 'json':
        d = get_training_set_data(db, ts, return_type)
        logger.info('Serializing resultset')
        ts.data = dumps(d, default=str)

    return ts


def register_training_set(db: Session, ts: TrainingSet, save_as: str) -> TrainingSet:
    """
    Validates and then Saves a training set metadata and TrainingSetInstance.

    *   First we need to see if this name already exists.
    *   If it does not, then we create it at version 1.
    *   If it does, validate that it has a matching schema to this one (same features). If so, create a new version.
    *   If the schema doesn't match, throw an error

    :param db: Session
    :param ts: TrainingSet
    :param save_as: The name we wish to save the training set with
    :return: Training Set with updated metadata
    """

    tsm: TrainingSetMetadata = crud.get_training_set_instance_by_name(db, save_as)
    if tsm:
        # Validate that it's using the same View (if it has a view at all)
        if tsm.view_id != ts.metadata.view_id:
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.ALREADY_EXISTS,
                                         message=f'Training Set {save_as} already exists and was created with a different '
                                                 f'view ({ts.metadata.view_id}) than the one provided ({tsm.view_id}). '
                                                 f'To create a new version of Training Set {save_as}, you must use '
                                                 f'the same view. Otherwise, save this Training Set with a different name')

        tsm_feats = sorted([int(i) for i in tsm.features.split(',')])
        ts_feats = sorted([f.feature_id for f in ts.features])
        if tsm_feats != ts_feats:
            raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.ALREADY_EXISTS,
                                         message=f'Training Set {save_as} already exists and has a different set '
                                                 f'of features than the ones provided. To create a new version of '
                                                 f'Training Set {save_as}, you must use the same set of features. '
                                                 f'Training Set {save_as} has features {tsm_feats} but you requested'
                                                 f' features {ts_feats}')
        # Training Set Instance exists and this is a new one. Register a new instance with version + 1
        new_instance = TrainingSetMetadata(
            training_set_start_ts = ts.metadata.training_set_start_ts,
            training_set_end_ts = ts.metadata.training_set_end_ts,
            training_set_create_ts = ts.metadata.training_set_create_ts,
            training_set_version = tsm.training_set_version + 1,
            training_set_id = tsm.training_set_id,
            view_id = tsm.view_id,
            view_version = tsm.view_version
        )
        crud.register_training_set_instance(db, new_instance)
    else:
        # No Training Set Instance of this name yet. Create the first instance
        logger.info(f"Creating training set {save_as}")
        tset_id = crud.create_training_set(db, ts, save_as)

        crud.register_training_set_features(db, tset_id, ts.features, ts.metadata.label)
        new_instance = TrainingSetMetadata(
            training_set_start_ts = ts.metadata.training_set_start_ts,
            training_set_end_ts = ts.metadata.training_set_end_ts,
            training_set_create_ts = ts.metadata.training_set_create_ts,
            training_set_version = 1,
            training_set_id = tset_id,
            view_id = ts.metadata.view_id,
            view_version = ts.metadata.view_version
        )
        crud.register_training_set_instance(db, new_instance)
    ts.metadata = new_instance
    return ts



def get_training_set_data(db: Session, ts: TrainingSet, return_type: str) -> List[Tuple[str, List[Any]]]:
    """
    Runs the SQL for a training set and returns the materialized data (assuming it is smaller than the limit)

    :param db: SqlAlchemy Session
    :param ts: Training Set (for metadata)
    :param return_type: The requested return type
    :return: The data formatted in Columnar format (column_name: [column datum]
    """
    # 50MB max size, assuming each column is a float64 (8 bytes)
    max_rows = int(TRAINING_SET_MAX_SIZE / len(ts.features) * 8)

    logger.info(f'Executing SQL')
    sql = f'select top {max_rows} * from ({ts.sql})'
    res = db.execute(sql)

    logger.info(f'Gathering result set')
    cols = [i[0] for i in res.cursor.description]
    rows = res.fetchall()

    if len(rows) >= max_rows:
        raise SpliceMachineException(code=ExceptionCodes.BAD_ARGUMENTS, status_code=status.HTTP_403_FORBIDDEN,
                                     message=f'This training set is too large to return in the request '
                                             f'format ({return_type}). Remove the return type to have this rendered '
                                             f'on the client as a Spark DF, or set return_type to sql to get the '
                                             f'SQL statement that creates this DF. Alternatively, set a smaller '
                                             f'range for start_time and end_time to get less data.')

    logger.info(f'Formatting data')

    data = [(col,[row[i] for row in rows]) for i,col in enumerate(cols)]
    # data = [cols] + [list(row) for row in rows] Simple row storage
    return data

def training_set_to_json(ts: TrainingSet) -> JSONResponse:
    """
    Converts a TrainingSet object to raw JSON so it can be returned directly in a JSONResponse.
    We use this function in the event that a user requests their training set in pandas / JSON form,
    so it can be send via the REST response.

    :param ts: TrainingSet
    :return: JSON
    """
    data = ts.__dict__.pop('data')
    json_data = jsonable_encoder(ts)
    json_data['data'] = data
    logger.info(f'JSON encoding complete')
    return JSONResponse(content=json_data)
