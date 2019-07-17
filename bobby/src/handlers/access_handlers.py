from .base_handler import BaseHandler
from .definitions import Job, Handler
import logging

logger = logging.getLogger(__name__)


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
    def __init__(self, task: Job, _handler_name: str = 'ENABLE_HANDLER', _mutable: bool = False):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the enable
            handler job to process

        """
        BaseHandler.__init__(self, task, _handler_name, _mutable)

    def _handle(self) -> None:
        """
        Enable a specific service
        """

        service_to_start: Handler = _get_handler_by_name(self.Session, self.task.parsed_payload['service'])
        logger.info(f"Enabling service: {service_to_start}")
        if service_to_start:
            service_to_start.enable()
            self.Session.add(service_to_start)
            self.Session.commit()
        else:
            logger.error(f"Unable to find service: {service_to_start}")
            raise Exception(f"Error enabling service: {str(service_to_start)} - Cannot find Service in DB!")


class DisableHandler(BaseHandler):
    def __init__(self, task: Job, _handler_name: str = 'DISABLE_HANDLER', _mutable: bool = False):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the disable
            handler job to process

        """
        BaseHandler.__init__(self, task, _handler_name, _mutable)

    def _handle(self) -> None:
        """
        Disable a specific service
        """

        service_to_stop: Handler = _get_handler_by_name(self.Session, self.task.parsed_payload['service'])
        logger.info(f"Disabling service: {service_to_stop}")
        if service_to_stop:
            service_to_stop.disable()
            self.Session.add(service_to_stop)
            self.Session.commit()
        else:
            logger.error(f"Unable to find service: {service_to_stop}")
            raise Exception(f"Error enabling service: {str(service_to_stop)} - Cannot find Service in DB!")
