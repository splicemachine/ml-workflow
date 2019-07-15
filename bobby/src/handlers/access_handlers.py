from .base_handler import BaseHandler
from .definitions import Job, Handler, ManagedDBSession
import logging

logger = logging.getLogger(__name__)


def _get_handler_by_name(Session, handler_name: str) -> Handler:
    """
    Retrieve a Handler object from the database
    by its name

    :param Session: (Session) the scoped
        database session to execute under
    :param handler_name: (str) the name of the
        handler to retrieve
    :return: (Handler) retrieved handler object (Python) from the database
    """
    return Session.query(Handler).filter_by(handler_name=handler_name).first()


class EnableHandler(BaseHandler):
    def __init__(self, task: Job, _handler_name: str = 'ENABLE_SERVICE'):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the enable
            handler job to process

        """
        BaseHandler.__init__(self, task, _handler_name)

    def _handle(self) -> None:
        """
        Enable a specific service
        """
        with ManagedDBSession() as Session:
            service_to_start: Handler = _get_handler_by_name(Session, self.task.payload['service'])
            logger.info(f"Enabling service: {service_to_start}")
            if service_to_start:
                service_to_start.enable()
                Session.add(service_to_start)
            else:
                logger.error(f"Unable to find service: {service_to_start}")
                raise Exception(f"Error enabling service: {str(service_to_start)} - Cannot find Service in DB!")


class DisableHandler(BaseHandler):
    def __init__(self, task: Job, _handler_name: str = 'DISABLE_SERVICE'):
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task: (Job) the disable
            handler job to process

        """
        BaseHandler.__init__(self, task)

    def _handle(self) -> None:
        """
        Disable a specific service
        """
        with ManagedDBSession() as Session:
            service_to_stop: Handler = _get_handler_by_name(Session, self.task.payload['service'])

            if service_to_stop:
                service_to_stop.disable()
                Session.add(service_to_stop)
            else:
                raise Exception(f"Error disabling service: {str(service_to_stop)} - Cannot find Service in DB!")
