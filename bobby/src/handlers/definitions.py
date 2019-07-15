import os
import logging
from json import loads as parse_dict
from time import time as timestamp

from sqlalchemy import create_engine, Column, ForeignKey, String, Integer, Boolean, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, relationship, sessionmaker
from contextlib import contextmanager

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

SCHEMA: str = "ML_MANAGER"

# Logging
logger = logging.getLogger(__name__)

# Database Session Setup
engine = create_engine(os.environ['ODBC_URL'], pool_size=CONNECTION_POOL_SIZE, max_overflow=0, echo=True)

Base.metadata.bind = engine
Base.metadata.create_all(checkfirst=True)  # check if each table exists, if it doesn't, create it

thread_safe_session_factory = None


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
    __tablename__ = "HANDLERS"

    id: Column = Column(Integer, primary_key=True, autoincrement=True)
    name: Column = Column(String(100))
    enabled: Column = Column(Boolean, default=True)  # whether or not this service is enabled

    __table_args__: tuple = (
        ('schema', SCHEMA),
    )

    def __repr__(self) -> None:
        """
        :return: String representation of Handler
        """
        return "<Handler: " + self.name + ">"

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
    __tablename__: str = "JOBS"

    id: Column = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: Column = Column(Integer, default=int(timestamp()))
    handler_name: Column = Column(String(100), ForeignKey(Handler.name))
    status: Column = Column(String(100), default='PENDING')
    info: Column = Column(String(1000))
    payload: Column = Column(String(5000))

    handler: relationship = relationship('Handler', backref='JOBS')

    __table_args__: tuple = (
        ('schema', SCHEMA),
        CheckConstraint(
            status.in_(Statuses.valid_statuses)  # no funny business allowed!
        )
    )

    def __repr__(self) -> str:
        """
        :return: (str) String Representation of Job
        """
        return "<Job: " + self.name + " | " + self.handler_name + " :: " + str(self.payload) + ">"

    def update(self, status: str = None, info: str = None) -> None:
        """
        Update the info of a job or the
        status of a job (or both) to new values

        * Note: there is a
        check constraint on this field for status*

        :param status: (str) the new status to change to
        :param info: (str) the new info to change to
        """
        self.status: str = status
        self.info: str = info

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
        self.payload = parse_dict(self.payload)


def init_session_factory():
    """
    Initialize the thread_safe_session_factory.

    :returns: (scoped_session) returns a factory
        for generating scoped, local, database sessions
    """
    global engine, thread_safe_session_factory

    if thread_safe_session_factory is None:
        thread_safe_session_factory = scoped_session(sessionmaker(bind=engine))
    return thread_safe_session_factory


@contextmanager
def ManagedDBSession():
    """
    Returns a thread-safe, exception-safe, local
    Database session via a context manager.

    Usage:
    ```
    with ManagedSession() as sess:
        results = sess.query(table).all()
        print(results)
        session.add(object)
        # automatically committed
    ```

    :returns: (Session) local, scoped session

    """
    global thread_safe_session_factory

    if thread_safe_session_factory is None:
        raise ValueError("There is no current session factory.")

    session = thread_safe_session_factory()

    try:
        yield session
        session.commit()
        session.flush()
        logger.debug("Committed DB Session...")
    except Exception:
        session.rollback()
        logger.exception("An unexpected error occurred while processing the DB Session")

        raise
    finally:
        thread_safe_session_factory.remove()


init_session_factory()
