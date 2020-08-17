"""
Class for Logging information to the database
by updating the contents of a cell
"""
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.logger.logging_config import logger
from shared.services.database import DatabaseSQL


class JobLoggingManager:
    """
    Externally facing logger that updates the logs
    associated with the specific cells in a database
    """
    LOGGING_FORMAT = "{level: <8} {time:YYYY-MM-DD HH:mm:ss.SSS} - {message}"

    def __init__(self, *, session: Session, task_id: int, logging_format=None):
        """
        :param session: SQLAlchemy Session to use for logging
        :param task_id: the task id to bind the logger to
        """
        self.logging_format = JobLoggingManager.LOGGING_FORMAT or logging_format
        self.task_id = task_id
        self.session = session
        self.handler_id = logger.add(
            self.splice_sink, format=self.logging_format, filter=self.message_filter
        )

    def message_filter(self, record):
        """
        Filter messages going through the handler
        to not send to database unless the task id matches,
        and the send_db parameter has been set to true

        :param record: record to filter
        :return: whether or not to handle it
        """
        record_extras = record['extra']
        return record_extras.get('task_id') == self.task_id and record_extras.get('send_db', False)

    def splice_sink(self, message):
        """
        Splice Sink to send messages to the database

        :param message: record to add to the database
        """
        self.session.execute(
            text(DatabaseSQL.update_job_log),
            params={'message': bytes(str(message), encoding='utf-8'), 'task_id': self.task_id}
        )
        self.session.commit()

    def get_logger(self):
        """
        Get the logger binded to that specific task_id
        :return: logger
        """
        return logger.bind(task_id=self.task_id)

    def destroy_logger(self):
        """
        Destroy the logger handler
        """
        logger.warning(f"Removing Database Logger - {self.task_id}")
        logger.remove(self.handler_id)
        logger.info("Done.")