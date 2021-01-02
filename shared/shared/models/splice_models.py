"""
This module contains SQLAlchemy Models
used for the Queue
"""
import re
from datetime import datetime
from json import loads as parse_dict
from time import sleep
from typing import Optional

from shared.logger.logging_config import logger
from shared.models.enums import JobStatuses, RecurringJobStatuses
from shared.services.database import SQLAlchemyClient
from sqlalchemy import (Boolean, CheckConstraint, Column, ForeignKey, Integer,
                        String, Text, DateTime)
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.sql.elements import TextClause

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

SHORT_VARCHAR_SIZE: int = 100
LONG_VARCHAR_SIZE: int = 5000
EXTRA_LONG_VARCHAR_SIZE: int = 30000
# example = mlflow/#/experiments/1/runs/9e87e9d68923
MLFLOW_URL_PARSER = re.compile("/experiments/(?P<exp_id>[0-9].*?)/runs/(?P<run_id>\w+)")


def format_timestamp() -> str:
    """
    Get a string representation
    of the current timestamp
    which can be parsed by the database.py
    :return:
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


############################
# Define SQLAlchemy Models #
############################

# noinspection PyTypeChecker
class Handler(SQLAlchemyClient.SpliceBase):
    """
    A Service e.g. Deployment, Start/Stop Service etc.
    """
    # Table Configuration
    __tablename__: str = "HANDLERS"

    # Sizes
    SHORT_VARCHAR_SIZE: int = 100

    # Columns Definition
    name: Column = Column(String(SHORT_VARCHAR_SIZE), primary_key=True)
    url: Column = Column(String(SHORT_VARCHAR_SIZE))
    modifiable: Column = Column(Boolean, default=True)
    enabled: Column = Column(Boolean, default=True)

    def __init__(self, payload_args: list, *args,
                 **kwargs) -> None:
        """
        :param payload_args: list of fields for the API
        """
        super().__init__(*args, **kwargs)
        # these attributes are used for the API, not persisted in the database
        self.payload_args = payload_args
        self.handler_class: object = None

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

    def assign_handler(self, handler: object) -> None:
        """
        Assign a handler class to a given
        handler object

        :param handler: (object) handler to assign
        """
        self.handler_class = handler


# noinspection PyTypeChecker
class Job(SQLAlchemyClient.SpliceBase):
    """
    A Job, e.g. Deploy this model, stop this service etc.
    """
    # Table Configuration
    __tablename__: str = "JOBS"

    # TBA (To-Be-Assigned later) when JSON is parsed by structures
    parsed_payload: Optional[dict] = None

    # Columns Definition
    id: Column = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: Column = Column(DateTime, server_default=TextClause("CURRENT_TIMESTAMP"), nullable=False)
    handler_name: Column = Column(String(SHORT_VARCHAR_SIZE), ForeignKey(Handler.name),
                                  nullable=False)

    parent_job_id = Column(Integer, nullable=True)

    status: Column = Column(String(SHORT_VARCHAR_SIZE), default='PENDING')
    logs: Column = deferred(Column(Text, default='---Job Logs---\n'))

    payload: Column = Column(Text, nullable=False)
    user: Column = Column(String(SHORT_VARCHAR_SIZE), nullable=False)

    mlflow_url: Column = Column(String(LONG_VARCHAR_SIZE), default="N/A")  # TODO change to run id and exp id

    # mlflow_url is only applicable to deployment jobs (and maybe retraining in the future)
    target_service: Column = Column(String(SHORT_VARCHAR_SIZE), default="N/A")
    # target_service is only applicable to access modifiers

    # Foreign Key Relationships
    handler: relationship = relationship('Handler', foreign_keys='Job.handler_name')
    # Table Options Configuration
    __table_args__: tuple = (
        CheckConstraint(
            status.in_(JobStatuses.get_valid())  # no funny business allowed!
        ),
    )

    def __repr__(self) -> str:
        """
        :return: (str) String Representation of Job
        """
        return f"<Job: {self.handler_name} ({self.status}) | Data={self.payload}>"

    def update(self, status: str) -> None:
        """
        Update the status of a job

        :param status: (str) the new status to change to
        """
        self.status: str = status[:SHORT_VARCHAR_SIZE]

    def parse_url(self):
        """
        Parse the MLFlow URL into Run ID and Experiment ID
        :return: dictionary containing run id and experiment di
        """
        if not self.mlflow_url:
            return
        match = MLFLOW_URL_PARSER.search(self.mlflow_url)
        if match:
            return dict(experiment_id=match.group('exp_id'), run_id=match.group('run_id'))
        else:
            raise Exception("Could not parse Job MLFlow URL")

    def parse_payload(self) -> None:
        """
        Convert the serialized JSON payload
        string into a dictionary
        """
        self.parsed_payload = parse_dict(self.payload)


class RecurringJob(SQLAlchemyClient.SpliceBase):
    """
    A Recurring job, e.g. retraining/feature store statistics in real time
    """
    __tablename__: str = "RECURRING_JOBS"

    name: Column = Column(String, primary_key=True, nullable=False)
    creation_timestamp: Column = Column(DateTime, server_default=TextClause("CURRENT_TIMESTAMP"), nullable=False)
    status: Column = Column(String(SHORT_VARCHAR_SIZE))
    job_id: Column = Column(Integer, ForeignKey(Job.id), nullable=False)
    user: Column = Column(String(SHORT_VARCHAR_SIZE), nullable=False)
    # This is the entity that we are scheduling a retrain for. This could be an mlflow run ID or a Feature store
    # Feature Set Name, or a Training Set ID, or a new entity created in the future
    entity_id: Column = Column(String(SHORT_VARCHAR_SIZE), nullable=False, primary_key=True)
    job: relationship = relationship('Job', foreign_keys='RecurringJob.job_id')

    __table_args__ = (
        CheckConstraint(
            status.in_(RecurringJobStatuses.get_valid())
        ),
    )


def create_bobby_tables(_sleep_secs=1) -> None:
    """
    Function that create's all of the tables in a retry loop in case the database.py doesn't exist
    Tries to create the necessary tables, retrying every 30 seconds, max 10 times
    Will gracefully fail after that if no DB exists
    """
    if _sleep_secs > 500:
        raise Exception("Could not connect to database ")

    # noinspection PyBroadException
    try:
        logger.warning("Creating Splice Tables inside Splice DB...")
        SQLAlchemyClient.SpliceBase.metadata.create_all(checkfirst=True)
        logger.info("Created Tables")
    except Exception:
        logger.exception(f"Encountered Error while initializing")  # logger might have failed
        logger.error(f"Retrying after {_sleep_secs} seconds...")
        sleep(_sleep_secs)
        create_bobby_tables(_sleep_secs=_sleep_secs * 2)
