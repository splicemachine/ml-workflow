"""
Static constants and definitions
used throughout MLManager Codebase
"""

from re import compile as regular_expression
from pyodbc import Connection as ODBCCnxn, connect as odbc_connect, Error as pyodbcError
from functools import partial
from os import environ as env_vars

from .. import Definition

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commerical"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


class Extraction:
    """
    Utilities for extraction
    """
    ARTIFACT_PATH_REGEX = regular_expression(f"^.*?:\/\/(.*?)\/(.*?)\/\w+$")


class Database:
    """
    Database configuration constants
    """
    database_name: str = "splicedb"
    database_schema: str = "MLMANAGER"
    db_port: str = env_vars.get('SPLICEDB_HREGION_SERVICE_PORT_JDBC') or '1527'
    connection_string: str = f"splicemachinesa://{env_vars['DB_USER']}:{env_vars['DB_PASSWORD']}@" \
                             f"{env_vars['DB_HOST']}:{db_port}/{database_name}"

    connection_pool_size: int = 3  # three active connections in pool
    sqlalchemy_echo: bool = (env_vars['MODE'] == 'development')

    engine_options: dict = {
        'pool_size': connection_pool_size,
        'max_overflow': 0,
        'echo': sqlalchemy_echo,
        'pool_pre_ping': True
    }

    creator_name: str = 'bobby-0'  # we encounter write-write conflicts
    # if we are concurrently doing create table statements from the
    # threaded Gunicorn App
    am_i_creator: str = env_vars['TASK_NAME'] == creator_name

    datetime_format: str = "%Y-%m-%d %H:%M:%S"  # YYYY-MM-DD HOUR:MIN:SEC (splice can parse this)


class JobStatuses(Definition):
    """
    Class containing names
    for valid Job statuses
    """
    pending: str = 'PENDING'
    success: str = 'SUCCESS'
    running: str = 'RUNNING'
    failure: str = 'FAILURE'

    @staticmethod
    def get_valid() -> tuple:
        """
        Return a tuple of the valid statuses
        for Jobs in Database
        :return: (tuple) valid statuses
        """
        return (
            JobStatuses.pending, JobStatuses.success, JobStatuses.running, JobStatuses.failure
        )


class FileExtensions(Definition):
    """
    Class containing names for
    valid File Extensions
    """
    spark: str = "spark"
    keras: str = "h5"
    h2o: str = "h2o"
    sklearn: str = "pkl"

    @staticmethod
    def get_valid() -> tuple:
        """
        Return a tuple of the valid file extensions
        in Database
        :return: (tuple) valid statuses
        """
        return (
            FileExtensions.spark, FileExtensions.keras, FileExtensions.h2o, FileExtensions.sklearn
        )
