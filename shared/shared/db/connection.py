from os import environ as env_vars

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from shared.logger.logging_config import logger


class DatabaseConnectionConfig:
    """
    Splice Machine DB connection configuration for use with SQLAlchemy
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
    max_overflow: int = env_vars.get('MAX_OVERFLOW', -1)
    echo: bool = env_vars['MODE'] == 'development'
    pool_pre_ping: bool = True
    pool_recycle = 500

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
            'pool_pre_ping': DatabaseEngineConfig.pool_pre_ping,
            'pool_recycle': DatabaseEngineConfig.pool_recycle
        }


class DBLoggingClient:
    """
    Configuration and Connections for DB Logging
    """
    logging_engine = None  # Logging Connection the database

    LoggingSessionMaker = None  # Logging Session Maker
    LoggingSessionFactory = None  # Thread-safe session factory issuer

    _job_manager_created = False

    def __init__(self):
        self.create_job_manager()

    @staticmethod
    def create_job_manager():
        """
        Create Job Manager Connection (only runs if Job Manager is used)
        """
        logger.info("Creating Job Manager Database Connection")
        DBLoggingClient.logging_connection = create_engine(
            DatabaseConnectionConfig.connection_string(),
            **DatabaseEngineConfig.as_dict()
        )
        logger.info("Creating Job Manager Session Maker")
        DBLoggingClient.LoggingSessionMaker = sessionmaker(bind=DBLoggingClient.logging_connection)
        logger.info("Creating Logging Factory")
        DBLoggingClient.LoggingSessionFactory = scoped_session(DBLoggingClient.LoggingSessionMaker)
        logger.info("Done.")
        DBLoggingClient._job_manager_created = True


class SQLAlchemyClient:
    """
    Database configuration constants
    """
    engine = None  # SQLAlchemy Engine

    reflector = None  # Inspector for DB reflection

    SessionMaker = None  # Session Maker
    SessionFactory = None  # Thread-safe session factory issuer

    # Non MLFlow Tables that are created with our own SQLAlchemy Engine/Sessions
    SpliceBase = declarative_base()

    _created = False

    def __init__(self, reconnect: bool = False):
        """
        :param reconnect: Whether or not to force DB reconnect
        """
        SQLAlchemyClient.connect(reconnect=reconnect)

    @property
    def MlflowBase(self):
        """
        Get the Mlflow SQLAlchemy Base. We use a property so that way
        this class can be used in software where mlflow is not installed
        """
        from mlflow.store.db.base_sql_model import Base as MLFlowBase
        return MLFlowBase

    @staticmethod
    def connect(reconnect: bool = False):
        """
        Connect to the Splice Machine Database
        :param reconnect: whether or not to force a reconnection and recreation of database resources
        :return: sqlalchemy engine
        """
        # if there are multiple writers, setting var before
        # will ensure that only one engine is created
        if reconnect or not SQLAlchemyClient._created:
            logger.info("SQLAlchemy Engine has not been created... creating SQLAlchemy Client...")
            SQLAlchemyClient.engine = create_engine(
                DatabaseConnectionConfig.connection_string(),
                **DatabaseEngineConfig.as_dict()
            )
            SQLAlchemyClient.SpliceBase.metadata.bind = SQLAlchemyClient.engine
            logger.debug("Created engine...")
            SQLAlchemyClient.SessionMaker = sessionmaker(bind=SQLAlchemyClient.engine,
                                                         expire_on_commit=False)
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

    @staticmethod
    def managed_session():
        """
        Managed Session with Rollback and Reconnection on Error
        """
        db = SQLAlchemyClient().SessionMaker()
        try:
            yield db
        except Exception:
            logger.error("Encountered Exception within Managed Transaction... Rolling Back and Resetting Connection")
            db.rollback()
            SQLAlchemyClient._created = False  # force reconnection on next operation
        finally:
            logger.info("Closing Session")
            db.close()

    @staticmethod
    def get_session():
        """
        Get a Session for DB Operations
        """
        db = SQLAlchemyClient().SessionMaker()
        try:
            yield db
        finally:
            logger.info("Closing session")
            db.close()
