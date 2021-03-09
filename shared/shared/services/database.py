from os import environ as env_vars

from sqlalchemy import create_engine, inspect as peer_into_splice_db, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.schema import MetaData

from mlflow.store.db.base_sql_model import Base as MLFlowBase
from shared.logger.logging_config import logger


class DatabaseConnectionConfig:
    """
    Database Connection Config
    """
    database_name: str = env_vars.get("DB_DATABASE_NAME", "splicedb")
    database_schema: str = env_vars['DB_USER'].upper()
    database_user: str = env_vars['DB_USER']
    database_port: str = env_vars.get('SPLICEDB_HREGION_SERVICE_PORT_JDBC', '1527')
    database_password: str = env_vars['DB_PASSWORD']
    database_host: str = env_vars['DB_HOST']

    @staticmethod
    def connection_string():
        """
        Get the SQLAlchemy Connection String
        :return: cnxn string
        """
        return f"splicemachinesa://{DatabaseConnectionConfig.database_user}:" \
               f"{DatabaseConnectionConfig.database_password}@{DatabaseConnectionConfig.database_host}:" \
               f"{DatabaseConnectionConfig.database_port}/{DatabaseConnectionConfig.database_name}"


class DatabaseEngineConfig:
    """
    Database Engine Connection Configuration
    """
    pool_size: int = 20
    max_overflow: int = env_vars.get('MAX_OVERFLOW',-1)
    echo: bool = env_vars['MODE'] == 'development'
    pool_pre_ping: bool = True

    @staticmethod
    def as_dict():
        """
        Serialize the Engine Configuration to a dictionary
        :return: serialized version
        """
        return {
            'pool_size': DatabaseEngineConfig.pool_size,
            'max_overflow': DatabaseEngineConfig.max_overflow,
            'echo': DatabaseEngineConfig.echo,
            'pool_pre_ping': DatabaseEngineConfig.pool_pre_ping
        }

class SQLAlchemyClient:
    """
    Database configuration constants
    """
    engine = None  # SQLAlchemy Engine

    logging_connection = None  # Logging Connection the database

    reflector = None  # Inspector for DB reflection

    SessionMaker = None  # Session Maker
    SessionFactory = None  # Thread-safe session factory issuer

    LoggingSessionMaker = None  # Logging Session Maker
    LoggingSessionFactory = None  # Thread-safe session factory issuer

    # We have two bases because there are two different types of tables to create.
    # There are MLFlow tables (created by their code, such as Experiment
    SpliceBase = declarative_base()
    MlflowBase = MLFlowBase

    _created = False
    _job_manager_created = False

    @staticmethod
    def create_job_manager():
        """
        Create Job Manager Connection (only runs if Job Manager is used)
        """
        logger.info("Creating Job Manager Database Connection")
        SQLAlchemyClient.logging_connection = create_engine(
                DatabaseConnectionConfig.connection_string(),
                **DatabaseEngineConfig.as_dict()
            )
        logger.info("Creating Job Manager Session Maker")
        SQLAlchemyClient.LoggingSessionMaker = sessionmaker(bind=SQLAlchemyClient.logging_connection)
        logger.info("Creating Logging Factory")
        SQLAlchemyClient.LoggingSessionFactory = scoped_session(SQLAlchemyClient.LoggingSessionMaker)
        logger.info("Done.")
        SQLAlchemyClient._job_manager_created = True

    @staticmethod
    def create():
        """
        Connect to the Splice Machine Database
        :return: sqlalchemy engine
        """
        # if there are multiple writers, setting var before
        # will ensure that only one engine is created
        if not SQLAlchemyClient._created:
            logger.info("SQLAlchemy Engine has not been created... creating SQLAlchemy Client...")
            SQLAlchemyClient.engine = create_engine(
                DatabaseConnectionConfig.connection_string(),
                **DatabaseEngineConfig.as_dict()
            )
            SQLAlchemyClient.SpliceBase.metadata.bind = SQLAlchemyClient.engine
            logger.debug("Created engine...")
            SQLAlchemyClient.SessionMaker = sessionmaker(bind=SQLAlchemyClient.engine,
                                                         expire_on_commit=False, autoflush=False)
            logger.debug("Created session maker")
            SQLAlchemyClient.SessionFactory = scoped_session(SQLAlchemyClient.SessionMaker)
            logger.debug("created session factory")
            SQLAlchemyClient._created = True
            logger.debug("Created SQLAlchemy Client...")
        else:
            logger.debug("Using existing SQLAlchemy Client...")
        return SQLAlchemyClient.engine

    @staticmethod
    def execute(sql: str) -> list:
        """
        Directly Execute SQL on the
        SQLAlchemy ENGINE without
        using the ORM (more performant).

        *WARNING: Is NOT Thread Safe-- Use SessionFactory for thread-safe
        SQLAlchemy Sessions*

        :param sql: (str) the SQL to execute
        :return: (list) returned result set
        """
        return SQLAlchemyClient.engine.execute(sql)


class DatabaseSQL:
    """
    Namespace for SQL Commands
    """
    feature_update_check = \
    """
    CREATE TRIGGER <schema_name>.<feature_set_tablename>_update_check
    BEFORE UPDATE 
    ON <schema_name>.<feature_set_tablename>
    REFERENCING OLD AS OLDW NEW AS NEWW
    FOR EACH ROW
    WHEN (OLDW.LAST_UPDATE_TS > NEWW.LAST_UPDATE_TS) SIGNAL SQLSTATE '2201H' SET MESSAGE_TEXT = 'LAST_UPDATE_TS must be greater than the current row.';
    """

    deployment_feature_historian = \
    """
    CREATE TRIGGER FeatureStore.deployment_historian
    AFTER UPDATE 
    ON FeatureStore.deployment
    REFERENCING OLD AS od
    FOR EACH ROW 
    INSERT INTO FeatureStore.deployment_history ( model_schema_name, model_table_name, asof_ts, training_set_id, training_set_start_ts, training_set_end_ts, training_set_create_ts, run_id, last_update_ts, last_update_username)
    VALUES ( od.model_schema_name, od.model_table_name, CURRENT_TIMESTAMP, od.training_set_id, od.training_set_start_ts, od.training_set_end_ts, od.training_set_create_ts, od.run_id, od.last_update_ts, od.last_update_username)
    """

    live_status_view_selector: str = \
        """
       SELECT mm.run_uuid,
           mm.action,
           CASE
               WHEN ((sta.tableid IS NULL
                      OR st.triggerid IS NULL
                      OR (mm.triggerid_2 IS NOT NULL
                          AND st2.triggerid IS NULL))
                     AND mm.action = 'DEPLOYED') THEN 'Table or Trigger Missing'
               ELSE mm.action
           END AS deployment_status,
           mm.tableid,
           mm.trigger_type,
           mm.triggerid,
           mm.triggerid_2,
           mm.db_env,
           mm.db_user,
           mm.action_date
    FROM DATABASE_DEPLOYED_METADATA mm
    LEFT OUTER JOIN sys.systables sta USING (tableid)
    LEFT OUTER JOIN sys.systriggers st ON (mm.triggerid = st.triggerid)
    LEFT OUTER JOIN sys.systriggers st2 ON (mm.triggerid_2 = st2.triggerid)
        """

    retrieve_jobs: str = \
        """
        SELECT id, handler_name FROM JOBS
        WHERE status='PENDING'
        ORDER BY "timestamp"
        """

    get_monthly_aggregated_jobs = \
        """
        SELECT MONTH(INNER_TABLE.parsed_date) AS month_1, COUNT(*) AS count_1, user_1
        FROM (
            SELECT "timestamp" AS parsed_date, "user" as user_1
            FROM JOBS
        ) AS INNER_TABLE
        WHERE YEAR(INNER_TABLE.parsed_date) = YEAR(CURRENT_TIMESTAMP)
        GROUP BY 1, 3
        """

    update_job_log = \
        """
        UPDATE JOBS SET LOGS=LOGS||:message 
        WHERE id=:task_id
        """
    add_database_deployed_metadata = \
        """
        INSERT INTO DATABASE_DEPLOYED_METADATA (
            run_uuid,action,tableid,trigger_type,triggerid,triggerid_2,db_env,db_user,action_date
        ) VALUES ('{run_uuid}','{action}','{tableid}','{trigger_type}','{triggerid}','{triggerid_2}','{db_env}',
            '{db_user}','{action_date}')
        """

    update_artifact_database_blob = \
        """
        UPDATE ARTIFACTS SET database_binary=:binary
        WHERE run_uuid='{run_uuid}' AND name='{name}'
        """

    add_feature_store_deployment = \
    """
    INSERT INTO FEATURESTORE.DEPLOYMENT(
        model_schema_name, model_table_name, training_set_id, 
        training_set_start_ts, training_set_end_ts, training_set_create_ts, run_id, last_update_username
    ) VALUES (
        '{model_schema_name}', '{model_table_name}', {training_set_id}, 
        '{training_set_start_ts}', '{training_set_end_ts}', '{training_set_create_ts}', '{run_id}', '{last_update_username}')
    """

    update_feature_store_deployment = \
    """
    UPDATE FEATURESTORE.DEPLOYMENT
        SET 
            training_set_id={training_set_id},
            training_set_start_ts='{training_set_start_ts}',
            training_set_end_ts='{training_set_end_ts}',
            training_set_create_ts='{training_set_create_ts}',
            last_update_username='{last_update_username}',
            run_id='{run_id}'
        WHERE
            model_schema_name='{model_schema_name}' 
        AND
            model_table_name='{model_table_name}'
    """

    get_k8s_deployments_on_restart = \
    """
    SELECT "user",payload FROM MLManager.Jobs
    INNER JOIN 
       ( SELECT MLFlow_URL, max("timestamp") "timestamp" 
         FROM MLManager.Jobs 
         where HANDLER_NAME in ('DEPLOY_KUBERNETES','UNDEPLOY_KUBERNETES')  
         group by 1 ) LatestEvent using ("timestamp",MLFLOW_URL)
    where HANDLER_NAME='DEPLOY_KUBERNETES'
    """

class Converters:
    """
    Converters for the database
    """

    @staticmethod
    def remove_spec_detail(sql_name: str) -> str:
        """
        Remove the length/precision/size etc specification
        from a SQL Type. Example: VARCHAR(2000) -> VARCHAR
        :param sql_name:  sql identifier
        :return: cleaned sql identifier
        """
        return sql_name.split('(')[0] if '(' in sql_name else sql_name

    SQL_TYPES = ['CHAR', 'LONG VARCHAR', 'VARCHAR', 'DATE', 'TIME', 'TIMESTAMP', 'BLOB', 'CLOB', 'TEXT', 'BIGINT',
                 'DECIMAL', 'DOUBLE', 'DOUBLE PRECISION', 'INTEGER', 'NUMERIC', 'REAL', 'SMALLINT', 'TINYINT',
                 'BOOLEAN', 'INT']

    SPARK_DB_CONVERSIONS = {
        'BinaryType': 'BLOB',
        'BooleanType': 'BOOLEAN',
        'ByteType': 'TINYINT',
        'DateType': 'DATE',
        'DoubleType': 'FLOAT',
        'DecimalType': 'DECIMAL',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'ShortType': 'SMALLINT',
        'StringType': 'VARCHAR(20000)',
        'TimestampType': 'TIMESTAMP',
        'UnknownType': 'BLOB',
        'FloatType': 'FLOAT'
    }

    DB_SPARK_CONVERSIONS = {
        'FLOAT': 'FloatType',
        'DOUBLE': 'FloatType',
        'BLOB': 'BinaryType',
        'BIGINT': 'LongType',
        'DATE': 'DateType',
        'INTEGER': 'IntegerType',
        'TIMESTAMP': 'TimestampType',
        'VARCHAR': 'StringType',
        'DECIMAL': 'DecimalType',
        'TINYINT': 'ByteType',
        'BOOLEAN': 'BooleanType',
        'SMALLINT': 'ShortType',
        'REAL': 'DoubleType',
        'NUMERIC': 'DecimalType',
        'DOUBLE PRECISION': 'DoubleType',
        'TEXT': 'StringType',
        'CLOB': 'StringType',
        'LONG VARCHAR': 'StringType',
        'CHAR': 'StringType'
    }

class DatabaseFunctions:
    """
    Helper functions for the database
    """

    @staticmethod
    def table_exists(schema_name: str, table_name: str, engine) -> bool:
        """
        Check whether or not a given table exists
        :param schema_name: schema name
        :param table_name: the table name
        :param engine: the SQLAlchemy Engine
        :return: whether exists or not
        """
        inspector = peer_into_splice_db(engine)
        return table_name.lower() in [value.lower() for value in inspector.get_table_names(schema=schema_name)]

    @staticmethod
    def drop_table_if_exists(schema_name: str, table_name: str, engine):
        """
        Drops table if exists
        :param schema_name: schema name
        :param table_name: the table name
        :param engine: the SQLAlchemy Engine
        """
        if DatabaseFunctions.table_exists(schema_name, table_name, engine):
            t = Table(table_name.upper(), MetaData(engine), schema=schema_name.upper(), autoload=True)
            t.drop(engine)


SQLAlchemyClient.create()
SQLAlchemyClient.create_job_manager()
