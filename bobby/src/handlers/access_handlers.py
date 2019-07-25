"""
Contains handlers pertaining to access- e.g.
enabling/disabling other handlers
"""
import logging

from mlmanager_lib.database.models import Job, Handler

from .base_handler import BaseHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)


def _get_handler_by_name(session, handler_name: str) -> Handler:
    """
    Retrieve a Handler object from the database
    by its name

    :param session: (Session) the scoped
        database session to execute under
    :param handler_name: (str) the name of the
        handler to retrieve
    :return: (Handler) retrieved handler object (Python) from the database
    """
    return session.query(Handler).filter_by(name=handler_name).first()


class EnableHandler(BaseHandler):
    """
    Handles ENABLE_HANDLER Jobs
    """

    def __init__(self, task: Job) -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the enable
            handler job to process

        """
        BaseHandler.__init__(self, task)

    def _handle(self) -> None:
        """
        Enable a specific service
        """
        service_to_start: Handler = _get_handler_by_name(
            self.Session,
            self.task.parsed_payload['service']
        )

        LOGGER.info(f"Enabling service: {service_to_start}")
        if service_to_start:
            if service_to_start.mutable:
                service_to_start.enable()
                self.Session.add(service_to_start)
                self.Session.commit()
            else:
                LOGGER.error(f"Error enabling service: {service_to_start} - not mutable")
                raise Exception(f"Service {service_to_start} is not modifiable")
        else:
            LOGGER.error(f"Unable to find service: {service_to_start}")
            raise Exception(
                f"Error enabling service: {str(service_to_start)} - Cannot find Service in DB!")


class DisableHandler(BaseHandler):
    """
    Handles DISABLE_HANDLER Jobs
    """

    def __init__(self, task: Job) -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the enable
            handler job to process

        """
        BaseHandler.__init__(self, task)

    def _handle(self) -> None:
        """
        Disable a specific service
        """
        service_to_stop: Handler = _get_handler_by_name(
            self.Session,
            self.task.parsed_payload['service']
        )

        LOGGER.info(f"Disabling service: {service_to_stop}")
        if service_to_stop:
            if service_to_stop.mutable:
                service_to_stop.disable()
                self.Session.add(service_to_stop)
                self.Session.commit()
            else:
                LOGGER.error(f"Error disabling service: {service_to_stop} - not mutable")
                raise Exception(f"Service {service_to_stop} is not modifiable")
        else:
            LOGGER.error(f"Unable to find service: {service_to_stop}")
            raise Exception(
                f"Error disabling service: {str(service_to_stop)} - Cannot find Service in DB!")

