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

    def __init__(self, *, task_id: int, logging_format=None):
        """
        :param task_id: the task id to bind the logger to
        """
        SQLAlchemyClient.create_job_manager()

        self.logging_format = JobLifecycleManager.LOGGING_FORMAT or logging_format
        self.task_id = task_id
        self.task = None

        self.Session = SQLAlchemyClient.LoggingSessionFactory()

        self.handler_id = logger.add(
            self.splice_sink, format=self.logging_format, filter=self.message_filter
        )

    def retrieve_task(self):
        """
        Retrieve the task object from the database
        """
        self.task: Job = self.Session.query(Job).filter_by(id=self.task_id).first()
        self.task.parse_payload()
        return self.task

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

    # noinspection PyBroadException
    def splice_sink(self, message):
        """
        Splice Sink to send messages to the database

        :param message: record to add to the database
        """
        try:
            updated_status = message.record['extra'].get('update_status')
            if updated_status:
                self.task.update(status=updated_status)
                self.Session.add(self.task)

            self.Session.execute(
                text(DatabaseSQL.update_job_log),
                params={'message': bytes(str(message), encoding='utf-8'), 'task_id': self.task_id}
            )
            self.Session.commit()
        except Exception:
            logger.exception("Encountered Exception while updating logs.")
            logger.warning("Rolling back Job Session.")
            self.Session.rollback()

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
        self.Session.close()
        logger.warning(f"Removing Database Logger - {self.task_id}")
        logger.remove(self.handler_id)
        logger.info("Done.")
