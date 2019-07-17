from os import environ as env_vars
import logging
from json import loads as parse_dict
from time import time as timestamp

from sqlalchemy import create_engine, Column, ForeignKey, String, Integer, Boolean, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, relationship, sessionmaker

"""
This module contains SQLAlchemy Models
used for the Queue
"""

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commerical"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

Base = declarative_base()

# Constants
CONNECTION_POOL_SIZE: int = 5  # five active connections in pool

SCHEMA: str = "MLMANAGER"

SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_size': CONNECTION_POOL_SIZE,
    'max_overflow': 0,
    'echo': True,
    'pool_pre_ping': True
}

# Logging
logger = logging.getLogger('root')


def get_odbc_url() -> str:
    """
    Create an SQLAlchemy ODBC URL from environment
    variables specified at container runtime
    :return: (str) valid ODBC URL
    """
    return f"splicemachinesa://{env_vars['DB_USER']}:{env_vars['DB_PASSWORD']}@{env_vars['DB_HOST']}:" \
        f"{env_vars['DB_PORT']}/splicedb"


odbc_url: str = get_odbc_url()
logger.info(f"ODBC URL for Splice Machine DB is {odbc_url} ")

engine = create_engine(odbc_url, **SQLALCHEMY_ENGINE_OPTIONS)


class Statuses:
    """
    Class containing names
    for valid statuses
    """
    pending: str = 'PENDING'
    success: str = 'SUCCESS'
    running: str = 'RUNNING'
    failure: str = 'FAILURE'

    valid_statuses: tuple = (
        pending, success, running, failure
    )


# Define SQLAlchemy Models

# noinspection PyTypeChecker
class Handler(Base):
    """
    A Service e.g. Deployment, Start/Stop Service etc.
    """
    # Table Configuration
    __tablename__: str = "HANDLERS"

    __table_schema_name__: str = SCHEMA + "." + __tablename__

    # Sizes
    SHORT_VARCHAR_SIZE: int = 100

    # Columns Definition
    name: Column = Column(String(SHORT_VARCHAR_SIZE), primary_key=True)
    enabled: Column = Column(Boolean, default=True)  # whether or not this service is enabled

    # Table Option Configuration
    __table_args__: tuple = (
        {
            'schema': SCHEMA
        },
    )

    def __repr__(self) -> None:
        """
        :return: String representation of Handler
        """
        return f"<Handler: {self.name}>"

    def enable(self) -> None:
        """
        Enable Jobs with the current
        Handler
        """
        self.enabled = True

    def disable(self) -> None:
        """
        Disable Jobs with the current
        Handler
        """
        self.enabled = False


# noinspection PyTypeChecker
class Job(Base):
    # Table Configuration
    __tablename__: str = "JOBS"

    __table_schema_name__: str = SCHEMA + "." + __tablename__

    # Sizes for Truncation
    SHORT_VARCHAR_SIZE: int = 100
    LONG_VARCHAR_SIZE: int = 5000

    # TBA (To-Be-Assigned later)
    parsed_payload: dict or None = None

    # Columns Definition
    id: Column = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: Column = Column(Integer, default=int(timestamp()))
    handler_name: Column = Column(String(SHORT_VARCHAR_SIZE), ForeignKey(Handler.name))
    status: Column = Column(String(SHORT_VARCHAR_SIZE), default='PENDING')
    info: Column = Column(String(LONG_VARCHAR_SIZE))
    payload: Column = Column(String(LONG_VARCHAR_SIZE))

    handler: relationship = relationship('Handler', foreign_keys='Job.handler_name')

    # Table Options Configuration
    __table_args__: tuple = (
        CheckConstraint(
            status.in_(Statuses.valid_statuses)  # no funny business allowed!
        ),
        {
            'schema': SCHEMA
        }
    )

    def __repr__(self) -> str:
        """
        :return: (str) String Representation of Job
        """
        return f"<Job: {self.handler_name} ({self.status}) | Data={self.payload}>"

    def update(self, status: str = None, info: str = None) -> None:
        """
        Update the info of a job or the
        status of a job (or both) to new values

        * Note: there is a
        check constraint on this field for status*

        We truncate the inputted values so that way
        we don't get truncation errors when doing an insert
        with a large traceback

        :param status: (str) the new status to change to
        :param info: (str) the new info to change to
        """
        if status:
            self.status: str = status[:self.SHORT_VARCHAR_SIZE]

        if info:
            self.info: str = info[:self.LONG_VARCHAR_SIZE]

    def fail(self, error_message: str) -> None:
        """
        Fail the current task instance
        (update the info and status)

        :param error_message: (str) the formatted HTML string
        """
        self.update(status=Statuses.failure, info=error_message)

    def succeed(self, success_message: str) -> None:
        """
        Succeed the current task instance
        (update the info and status)

        :param success_message: (str) formatted HTML string
        """
        self.update(status=Statuses.success, info=success_message)

    def parse_payload(self) -> None:
        """
        Convert the serialized JSON payload
        string into a dictionary
        """
        self.parsed_payload = parse_dict(self.payload)


def execute_sql(sql: str) -> list:
    """
    Directly Execute SQL on the
    SQLAlchemy engine without
    using the ORM (more performant)

    :param sql: (str) the SQL to execute
    :return: (list) returned result set
    """
    return list(engine.execute(sql))


Base.metadata.bind = engine
Base.metadata.create_all(checkfirst=True)  # check if each table exists, if it doesn't, create it
SessionFactory = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

