"""
This module contains SQLAlchemy Models
used for the Queue
"""
from datetime import datetime
from json import loads as parse_dict
from time import sleep
from typing import Optional

from sqlalchemy import (Boolean, CheckConstraint, Column, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship

from shared.logger.logging_config import logger
from shared.models.enums import JobStatuses
from shared.services.database import DatabaseSQL, SQLAlchemyClient

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


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

    def __init__(self, required_payload_args: tuple, optional_payload_args: dict, *args,
                 **kwargs) -> None:
        """
        :param required_payload_args: (tuple) tuple of required keys in the payload for this handler
            to execute

        :param optional_payload_args: (dict) dictionary of optional key/values in the payload
            for this handler (if arguments are not specified, it uses the defaults specified
            as values in the dictionary)
        """
        super().__init__(*args, **kwargs)
        # these attributes are used for the API, not persisted in the database.py
        self.required_payload_args: Optional[tuple] = required_payload_args
        self.optional_payload_args: Optional[dict] = optional_payload_args
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

    # Sizes for Truncation
    SHORT_VARCHAR_SIZE: int = 100
    LONG_VARCHAR_SIZE: int = 5000
    EXTRA_LONG_VARCHAR_SIZE: int = 24000

    # TBA (To-Be-Assigned later) when JSON is parsed by structures
    parsed_payload: dict or None = None

    # Columns Definition
    id: Column = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: Column = Column(String(SHORT_VARCHAR_SIZE), default=format_timestamp)
    handler_name: Column = Column(String(SHORT_VARCHAR_SIZE), ForeignKey(Handler.name),
                                  nullable=False)

    status: Column = Column(String(SHORT_VARCHAR_SIZE), default='PENDING')
    logs: Column = Column(String(24000), default='---Job Logs---\n')

    payload: Column = Column(String(LONG_VARCHAR_SIZE), nullable=False)
    user: Column = Column(String(SHORT_VARCHAR_SIZE), nullable=False)

    mlflow_url: Column = Column(String(LONG_VARCHAR_SIZE), default="N/A")
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

    def update(self, status: str = None) -> None:
        """
        Update the status of a job

        :param status: (str) the new status to change to
        """
        self.status: str = status[:self.SHORT_VARCHAR_SIZE]

    def fail(self) -> None:
        """
        Fail the current task instance
        """
        self.update(status=JobStatuses.failure)

    def succeed(self) -> None:
        """
        Succeed the current task instance
        (update the info and status)
        """
        self.update(status=JobStatuses.success)

    def parse_payload(self) -> None:
        """
        Convert the serialized JSON payload
        string into a dictionary
        """
        self.parsed_payload = parse_dict(self.payload)


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
