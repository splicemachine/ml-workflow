import shared.models.feature_store_models as models
from ... import schemas, crud
from . import helpers, constants
from sqlalchemy.orm import Session
from typing import List, Union
import json
from datetime import datetime
import base64
from ...constants import SQL
from ..utils import sql_to_datatype
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from fastapi import status
from shared.logger.logging_config import logger

def _get_source(name: str, db: Session):
    """
    The implementation of the get_source route with logic here so it can be called by other functions directly
    :param name: Name of Source
    :param db: SQLAlchemy Session
    :return: Source
    """
    s = crud.get_source(db, name)
    if not s:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                    message=f"Source {name} does not exist. Please provide a valid source")
    return s

def create_pipeline_entities(db: Session, sf: schemas.SourceFeatureSetAgg, source: schemas.Source, fset_id: int):
    """
    Creates the necessary pipeline entities and adds the metadata to the feature store
    This will create the pipeline aggregations and resulting features (from those aggregations)
    that come from a unique source+pipeline+feature_set combination.

    :param db: SqlAlchemy Session
    :param sf: SourceFeatureSetAgg
    :param source: The SQL source of the pipeline
    :param fset_id: Feature Set ID for these Pipeline aggregations
    """
    # Create feature aggregations
    pipeline_aggs: List[models.PipelineAgg] = []
    agg_features: List[schemas.FeatureCreate] = []
    for agg in sf.aggregations:
        # User provided prefix or generated unique by schema/table/column/agg
        feat_prefix = agg.feature_name_prefix or f'{sf.schema_name}_{sf.table_name}_{source.name}_{agg.column_name}'

        # Splice cant store arrays so stringify them
        json_func = json.dumps(agg.agg_functions)
        json_windows = json.dumps(agg.agg_windows)

        pipeline_aggs.append(
            models.PipelineAgg(
                feature_set_id=fset_id,
                feature_name_prefix=feat_prefix,
                column_name=agg.column_name,
                agg_functions=json_func,
                agg_windows=json_windows,
                agg_default_value=agg.agg_default_value
            )
        )

        # Create the features that the aggregations produce
        for f in agg.agg_functions:
            for w in agg.agg_windows: # For each agg function, there are n windows that make n features
                # Validate that the feature name is valid (in case they provided an already used prefix)
                fname = helpers.build_agg_feature_name(feat_prefix,f,w)
                crud.validate_feature(db, fname, sql_to_datatype('DOUBLE'))
                agg_features.append(
                    schemas.FeatureCreate(
                        feature_set_id=fset_id,
                        name=fname,
                        description=f'{f} of {agg.column_name} over last {w}',
                        feature_data_type=sql_to_datatype('DOUBLE'),
                        feature_type='C',  # 'C'ontinuous
                        tags=['sql','aggregation','auto'],
                        attributes={'source_col':agg.column_name,'agg_function':
                            f,'agg_type': 'windowed agg', 'agg_window': w},
                    )
                )
    crud.create_pipeline_aggregations(db, pipeline_aggs)
    crud.bulk_register_feature_metadata(db, agg_features)


def generate_backfill_sql(schema: str, table: str, source: schemas.Source, feature_aggs: List[schemas.FeatureAggregation]):
    """
    Generates the necessary backfill SQL for a feature set from a source with a set of feature aggregations

    :param schema: The schema of the feature set
    :param table: The table of the feature set
    :param source: The Source created by the user
    :param feature_aggs: The list of FeatureAggregations requested by the user
    :return: the backfill SQL
    """
    ins_column_list = ''
    expression_list = ''

    full_sql = f"INSERT INTO {schema}.{table}_HISTORY ("
    # pks
    ins_column_list += ",".join( source.pk_columns )
    expression_list += ",".join([ f' x.{f}' for f in source.pk_columns ])
    source_list = ",".join([ f' src.{f}' for f in source.pk_columns ])
    innersource_list = ",".join([ f' innersrc.{f}' for f in source.pk_columns ])
    # asof_ts
    ins_column_list += ', ASOF_TS, INGEST_TS'
    expression_list += ", '{backfill_asof_ts}', CURRENT_TIMESTAMP"
    source_list += f", {source.event_ts_column}"
    source_group_by = source_list

    # build timestamps for backfill
    all_windows = []

    for f in feature_aggs:
        source_list += f",SUM({f.column_name}) AS {f.column_name}"
        innersource_list += f", {f.column_name}"
        for func in f.agg_functions:
            all_windows = all_windows + f.agg_windows
            for window in f.agg_windows:
                agg_feature = helpers.build_agg_feature_name(f.feature_name_prefix, func, window )
                expressions = helpers.build_agg_expression(func, window, f.column_name, source.event_ts_column,
                                                           "'{backfill_asof_ts}'", default_value = f.agg_default_value)
                ins_column_list += f',{agg_feature}'
                expression_list += f',{expressions}'

    # find largest window to create lower bound of time for source table scan
    largest_window_sql = helpers.get_largest_window_sql(all_windows)

    # Get the SQL integer value of the smallest window and it's length (see function description)
    min_window_length, min_window_value = helpers.get_min_window_sql(all_windows)

    innersource_list += f", FeatureStore.TimestampSnapToInterval(timestamp({source.event_ts_column}), {min_window_value},{min_window_length}) {source.event_ts_column}"


    pk_col_list = ",".join(source.pk_columns)
    full_sql = f"""{full_sql} {ins_column_list} ) --splice-properties useSpark=True
                   SELECT {expression_list} 
                   FROM (
                           SELECT {source_list}
                           FROM ( 
                                SELECT {innersource_list} 
                                FROM ( 
                                    {source.sql_text} 
                                ) innersrc 
                                WHERE {source.event_ts_column} <= '{{backfill_asof_ts}}' 
                                AND {source.event_ts_column} >= ( TIMESTAMP('{{backfill_asof_ts}}') - {largest_window_sql} ) 
                            )src
                           GROUP BY {source_group_by}
                        ) x 
                   GROUP BY {pk_col_list}
               """
    return full_sql


def generate_backfill_intervals(db: Session, pipeline: schemas.Pipeline, ) -> List[datetime]:
    """
    Gets a list of timestamps to format the backfill SQL. We cannot run the entire backfill SQL at once for a feature set
    because it may be far too large for a single query, and it may crash the executors. So we break it into many timestamps
    and run those in a partially parallelized way.  The backfill_sql is always the same, and the intervals returned
    from this function are passed into that SQL as the `backfill_asof_ts` parameter (the only one in the SQL).
    for each timestamp returned from this function, you run the backfill SQL once (at that timestamp).
    :param db: SQLAlchemy Session
    :param pipeline: The pipeline to run on
    :return: The list of timestamp intervals to execute the Pipeline SQL with
    """
    window_type, window_length = helpers.parse_time_window(pipeline.backfill_interval)
    window_value = constants.tsi_window_values.get(window_type) # TODO: tsi_windows or tsi_window_values??
    sql = SQL.backfill_timestamps.format(backfill_start_time=pipeline.backfill_start_ts, pipeline_start_time=pipeline.pipeline_start_ts,
                     window_value=window_value, window_length=window_length)
    res = db.execute(sql).fetchall()
    return [i for (i,) in res] # Unpack the list of tuples

def generate_pipeline_sql(db,  source: schemas.Source, pipeline: schemas.Pipeline,
                          feature_aggs: List[schemas.FeatureAggregation] ):
    """
    Generates the incremental pipeline SQL for a Feature Set pipeline to run in Airflow
    :param db: SQLAlchemy Session
    :param source: The source of the Pipeline (the SELECT in the SQL statement)
    :param pipeline: The metadata about the pipeline
    :param feature_aggs: The specific feature aggregations to run
    :return: 
    """
    # find last completed extract timestamp
    ts_limit = crud.get_last_pipeline_run(db, pipeline.feature_set_id) or pipeline.pipeline_start_ts

    window_type, window_length = helpers.parse_time_window( pipeline.pipeline_interval )
    window_value = constants.tsi_window_values.get(window_type)

    pk_col_list = ",".join(source.pk_columns)
    extract_scope_sql = f'''
        SELECT 
            DISTINCT {pk_col_list}, 
            FeatureStore.TimestampSnapToInterval(timestamp({source.event_ts_column}), {window_value}, {window_length}) asof_ts 
        FROM ({source.sql_text}) isrc 
        WHERE isrc.{source.update_ts_column} > timestamp('{ts_limit}')
    '''

    ins_column_list = ''
    expression_list = ''

    # pks
    qualified_pks = ",".join([ f' x.{f}' for f in source.pk_columns ])
    expression_list += qualified_pks
    join_on_clause = " AND ".join ([f"x.{c} = t.{c}" for c in source.pk_columns ])
    # asof_ts
    expression_list += f', t.ASOF_TS, CURRENT_TIMESTAMP AS INGEST_TS, MAX(x.{source.update_ts_column}) MAX_UPDATE_TS'
    as_of_expr = 't.ASOF_TS'
    all_windows=[]
    for f in feature_aggs:
        for func in f.agg_functions:
            all_windows = all_windows + f.agg_windows
            for window in f.agg_windows:
                # The SQL aggregation to perform on the column (SUM, MAX, COUNT etc)
                agg = helpers.build_agg_expression(func, window, f.column_name, source.event_ts_column,
                                                   as_of_expr, f.agg_default_value)
                # The alias which will be the Feature name
                feature_name = helpers.build_agg_feature_name(f.feature_name_prefix, func, window)
                expression_list += f", {agg} AS {feature_name} "
    # find largest window to create lower bound of time for source table scan
    largest_window_sql = helpers.get_largest_window_sql(all_windows)

    full_sql = f'''SELECT {expression_list} 
                   FROM ({source.sql_text}) x
                    INNER JOIN
                        ({extract_scope_sql}) t 
                    ON {join_on_clause} 
                    AND x.{source.event_ts_column} <= {as_of_expr} 
                    AND x.{source.event_ts_column} >= {as_of_expr} - {largest_window_sql}
                   GROUP BY {qualified_pks}, {as_of_expr}
               '''

    return full_sql

def _create_pipe(pipe: schemas.PipeCreate, db: Session) -> schemas.PipeDetail:
    """
    The implementation of the create_pipe route with logic here so other functions can call it
    :param pipe: The pipe to create
    :param db: The database session
    :return: The created Pipe
    """
    crud.validate_pipe(db, pipe)
    logger.info(f'Registering pipe {pipe.name} in Feature Store')
    pipe_metadata = crud.register_pipe_metadata(db, pipe)
    pipe_version = crud.create_pipe_version(db, pipe_metadata)
    pd = pipe_metadata.__dict__
    pd.update(pipe_version.__dict__)
    return schemas.PipeDetail(**pd)

def _update_pipe(update: schemas.PipeUpdate, name: str, db: Session):
    """
    The implementation of the update_pipe route with logic here so other functions can call it
    :param update: The pipe version to create
    :param db: The database session
    :return: The created Pipe version
    """

    pipes: List[schemas.PipeDetail] = crud.get_pipes(db, _filter={'name': name, 'pipe_version': 'latest'})
    if not pipes:
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipe {name} does not exist. Please enter "
                                        f"a valid pipe, or create this pipe using fs.create_pipe()")
    pipe = pipes[0]

    if update.description:
        logger.info(f'Updating description for {name}')
        crud.update_pipe_description(db, pipe.pipe_id, update.description)

    crud.validate_pipe_function(update, pipe.ptype)
    pipe.__dict__.update(update.__dict__)
    pipe_version = crud.create_pipe_version(db, pipe, pipe.pipe_version + 1)
    pipe.pipe_version = pipe_version.pipe_version

    return pipe

def _alter_pipe(alter: schemas.PipeAlter, name: str, version: Union[str, int], db: Session):
    """
    The implementation of the update_pipe route with logic here so other functions can call it
    :param alter: The pipe to alter
    :param name: The pipe name
    :param version: The version to alter
    :param db: The database session
    :return: The updated Pipe
    """
    pipes: List[schemas.PipeDetail] = crud.get_pipes(db, _filter={'name': name, 'pipe_version': version})
    if not pipes:
        v = f'with version {version} ' if isinstance(version, int) else ''
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, code=ExceptionCodes.DOES_NOT_EXIST,
                                        message=f"Pipe {name} {v}does not exist. Please enter a valid pipe.")
    pipe = pipes[0]

    if alter.function:
        #if pipe has dependent pipeline, throw error
        #else
        crud.validate_pipe_function(alter, pipe.ptype)
        crud.alter_pipe_function(db, pipe, alter.func, alter.code)
        pipe.func = alter.func
        pipe.code = alter.code

    if alter.description:
        logger.info(f'Updating description for {name}')
        crud.update_pipe_description(db, pipe.pipe_id, alter.description)
        pipe.description = alter.description

    return pipe

def stringify_function(func: bytes) -> str:
    return base64.encodebytes(func).decode('ascii').strip()

def destringify_function(func: str) -> bytes:
    return base64.decodebytes(func.strip().encode('ascii'))
