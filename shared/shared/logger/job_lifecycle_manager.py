"""
Class for Logging information to the database
by updating the contents of a cell
"""
from typing import List

from shared.logger.logging_config import logger
from shared.models.splice_models import Job
from shared.services.database import DatabaseSQL, SQLAlchemyClient
from sqlalchemy import text


class JobLifecycleManager:
    """
    Externally facing logger that updates the Job status
    and logs in the database
    """
    LOGGING_FORMAT = "{level: <8} {time:YYYY-MM-DD HH:mm:ss.SSS} - {message}"

    # SQLAlchemy Manages Sessions on a thread local basis, so we need to create a
    # session here to maintain separate transactions then the queries executing in the
    # job threads.

    def __init__(self, *, task_id: int, logging_format: str = None, logging_buffer_size: int = -1):
        """
        :param task_id: the task id to bind the logger to
        """
        # SQLAlchemyClient.create_job_manager()

        self.logging_format = JobLifecycleManager.LOGGING_FORMAT or logging_format
        self.task_id = task_id
        self.task = None

        self.use_buffer = logging_buffer_size != -1

        self.buffer = []
        self.max_buffer_size = logging_buffer_size

        logger.info('creating the logging session factory')
        self.scoped_session = SQLAlchemyClient.LoggingSessionFactory()
        logger.info(str(self.scoped_session))
        logger.info('done')
        logger.info('adding a new sub logger')

        self.handler_id = logger.add(
            self.splice_sink, format=self.logging_format, filter=self.message_filter
        )
        logger.info('done')

    def retrieve_task(self):
        """
        Retrieve the task object from the database
        """

        logger.info('querying for job')
        self.scoped_session.commit()
        logger.info('Done comitting')
        logger.info('values')
        logger.info(str(list(self.scoped_session.execute('values 1'))))
        logger.info('real one')
        self.task: Job = self.scoped_session.query(Job).filter_by(id=self.task_id).first()
        logger.info('parsing job payload')
        self.task.parse_payload()
        logger.info('done')
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

    def write_log(self, *, message: str):
        """
        Write a single log message to the Splice Machine database lazily
        :param message: the log message to write
        :param commit: whether or not to commit to the database
        """
        self.scoped_session.execute(
            text(DatabaseSQL.update_job_log),
            params={'message': bytes(str(message), encoding='utf-8'), 'task_id': self.task_id}
        )

        self.scoped_session.commit()

    def write_buffer(self):
        """
        Write the buffer to database
        """
        if self.buffer:
            self.write_log(message=''.join(map(str, self.buffer)))

    # noinspection PyBroadException
    def splice_sink(self, message):
        """
        Splice Sink to send messages to the database

        :param message: record to add to the database
        """
        updated_status = message.record['extra'].get('update_status')
        if updated_status:
            self.task.update(status=updated_status)
            self.scoped_session.add(self.task)
            self.scoped_session.commit()

        if self.use_buffer:
            if len(self.buffer) == self.max_buffer_size:
                self.buffer.append(message)
                self.write_buffer()
                self.buffer.clear()
            else:
                self.buffer.append(message)
        else:
            self.write_log(message=message)

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
        self.write_buffer()
        try:
            self.safe_commit()
        finally:
            logger.warning(f"Removing Database Logger - {self.task_id}")
            logger.remove(self.handler_id)
            self.destroy_session()
            logger.info("Done.")

    def safe_commit(self):
        """
        Tries to commit all transactions of the Session before deletion. Rolls back on failure
        """
        if self.scoped_session:
            try:
                self.scoped_session.commit()
            except:
                self.scoped_session.rollback()

    def destroy_session(self):
        """
        Destroys the scoped Session at the end of the loggers life
        """
        SQLAlchemyClient.LoggingSessionFactory.remove()
        del self.scoped_session
        self.scoped_session = None

