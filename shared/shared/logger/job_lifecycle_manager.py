"""
Class for Logging information to the database
by updating the contents of a cell
"""
from sqlalchemy import text

from shared.logger.logging_config import logger
from shared.services.database import DatabaseSQL, SQLAlchemyClient
from shared.models.splice_models import Job


class JobLifecycleManager:
    """
    Externally facing logger that updates the Job status
    and logs in the database
    """
    LOGGING_FORMAT = "{level: <8} {time:YYYY-MM-DD HH:mm:ss.SSS} - {message}"
    # SQLAlchemy Manages Sessions on a thread local basis, so we need to create a
    # session here to maintain separate transactions then the queries executing in the
    # job threads.
    Session = SQLAlchemyClient.SessionFactory()

    def __init__(self, *, task_id: int, logging_format=None):
        """
        :param task_id: the task id to bind the logger to
        """
        self.logging_format = JobLifecycleManager.LOGGING_FORMAT or logging_format
        self.task_id = task_id
        self.task = None

        self.handler_id = logger.add(
            self.splice_sink, format=self.logging_format, filter=self.message_filter
        )

    def retrieve_task(self):
        """
        Retrieve the task object from the database
        """
        self.task: Job = JobLifecycleManager.Session.query(Job).filter_by(id=self.task_id).first()
        self.task.parse_payload()

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
        if message.record['extra'].get('update_status'):
            self.task.update(status=message['extra']['update_status'])
            JobLifecycleManager.Session.add(self.task)

        JobLifecycleManager.Session.execute(
            text(DatabaseSQL.update_job_log),
            params={'message': bytes(str(message), encoding='utf-8'), 'task_id': self.task_id}
        )
        JobLifecycleManager.Session.commit()

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
