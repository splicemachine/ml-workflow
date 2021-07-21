from sqlalchemy.types import (BIGINT, BLOB, BOOLEAN, CHAR, CLOB, DATE, DECIMAL,
                              FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, TEXT,
                              TIME, TIMESTAMP, VARCHAR)


class SQL:
    FEATURE_STORE_SCHEMA = 'FeatureStore'

    feature_set_trigger = f'''
    CREATE TRIGGER {{schema}}.{{table}}_history_{{action}} 
    AFTER {{action}} ON {{schema}}.{{table}}
    REFERENCING NEW_TABLE AS NEWW
    FOR EACH STATEMENT
        INSERT INTO {{schema}}.{{table}}_history (ASOF_TS, INGEST_TS, {{pk_list}}, {{feature_list}}) 
        SELECT NEWW.LAST_UPDATE_TS, CURRENT_TIMESTAMP, {{new_pk_cols}}, {{new_feature_cols}} FROM NEWW
    '''

    training_set_feature_stats = f"""
    INSERT INTO {FEATURE_STORE_SCHEMA}.training_set_feature_stats ( training_set_id, training_set_start_ts, training_set_end_ts, feature_id, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev) 
    VALUES 
    ({{training_set_id}}, {{training_set_start_ts}}, {{training_set_end_ts}}, {{feature_id}}, {{feature_cardinality}}, {{feature_histogram}}, {{feature_mean}}, {{feature_median}}, {{feature_count}}, {{feature_stddev}}) 
    """

    deployment_feature_stats = f"""
    INSERT INTO {FEATURE_STORE_SCHEMA}.deployment_feature_stats ( model_schema_name, model_table_name, model_start_ts, model_end_ts, feature_id, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev) 
    VALUES 
    ({{model_schema_name}}, {{model_table_name}}, {{model_start_ts}}, {{model_end_ts}}, {{feature_id}}, {{feature_cardinality}}, {{feature_histogram}}, {{feature_mean}}, {{feature_median}}, {{feature_count}}, {{feature_stddev}}) 
    """

    backfill_timestamps = """
    SELECT ASOF_TS
    FROM new "com.splicemachine.fs_functions.TimestampGeneratorVTI"('{backfill_start_time}','{pipeline_start_time}',{window_value},{window_length})
    t (asof_ts TIMESTAMP, until_ts TIMESTAMP)
    """

    pipeline = f"""
    INSERT INTO {FEATURE_STORE_SCHEMA}.pipeline(feature_set_id, source_id, pipeline_start_ts, pipeline_interval, backfill_start_ts, backfill_interval, pipeline_url) 
    VALUES ({{feature_set_id}}, {{source_id}}, '{{pipeline_start_ts}}', '{{pipeline_interval}}', '{{backfill_start_ts}}', '{{backfill_interval}}', '{{pipeline_url}}') 
    """

class Columns:
    feature = ['feature_id', 'feature_set_id', 'name', 'description', 'feature_data_type', 'feature_type',
               'tags', 'compliance_level', 'last_update_ts', 'last_update_username']
    training_view = ['view_id','name','description','sql_text','pk_columns','ts_column','label_column','join_columns']
    feature_set = ['feature_set_id', 'table_name', 'schema_name', 'description', 'pk_columns', 'pk_types', 'deployed']
    history_table_pk = ['ASOF_TS','UNTIL_TS']

SQL_TYPES = ['CHAR', 'LONG VARCHAR', 'VARCHAR', 'DATE', 'TIME', 'TIMESTAMP', 'BLOB', 'CLOB', 'TEXT', 'BIGINT',
             'DECIMAL', 'DOUBLE', 'DOUBLE PRECISION', 'FLOAT', 'INTEGER', 'NUMERIC', 'REAL', 'SMALLINT', 'TINYINT', 'BOOLEAN',
             'INT']

SQLALCHEMY_TYPES = [CHAR, VARCHAR, VARCHAR, DATE, TIME, TIMESTAMP, BLOB, CLOB, TEXT, BIGINT, DECIMAL, FLOAT, FLOAT,
                    FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, SMALLINT, BOOLEAN, INTEGER]

# class SQLALCHEMY_TYPES:
#     mapping = dict(zip(SQL_TYPES, [CHAR, VARCHAR, VARCHAR, DATE, TIME, TIMESTAMP, BLOB, CLOB, TEXT, BIGINT,
#                         DECIMAL, FLOAT, FLOAT, FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, SMALLINT, BOOLEAN, INTEGER]))
#     @staticmethod
#     def _get(item: str):
#         return SQLALCHEMY_TYPES.mapping[item.split('(')[0]]

SQL_TO_SQLALCHEMY = dict(
    zip(SQL_TYPES,SQLALCHEMY_TYPES)
)

CRON_PRESETS = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
