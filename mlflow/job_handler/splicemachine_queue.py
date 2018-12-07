import logging
import os
import time
from collections import namedtuple
from datetime import datetime
from hashlib import md5
from json import dumps, loads
from retry import retry
import jaydebeapi

# logging.basicConfig()
logger = logging.getLogger('queue')
# logger.setLevel(logging.DEBUG)

__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. Some Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__ = "Apache-2.0"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"


class SpliceMachineQueue(object):
    """
    A Queue that uses the Splice Machine RDBMS as its backend
    """

    def __init__(self):
        self.authenticated = False
        self.table = 'ML.JOBS'
        self.state_table = 'ML.ACTIVE_SERVICES'

        self.jdbc_conn = None
        self.cursor = None
        self.created = False

        self.datetime_format = "%m-%d-%Y %H:%M:%S"
        self.last_authenticated = datetime.today()
        self.Job = namedtuple(
            'Job', [
                'job_id', 'timestamp', 'handler', 'status', 'payload', 'info'])

        self.authenticate()
        created = self.create_queue()
        state = self.create_state_holder()

        if created:
            logger.info('Created Jobs Table')
        else:
            logger.info('Table for Jobs Exists')

        if state:
            logger.info('Created State Table')
        else:
            logger.info('Table for State exists')

    def __del__(self):
        logger.info('Closing Splice Machine Connection')

        if self.jdbc_conn:
            self.jdbc_conn.close()
            self.jdbc_conn = None

        if self.cursor:
            self.cursor.close()
            self.cursor = None

    @staticmethod
    def _time_hrs_difference(date_1, date_2):
        """
        Check the difference in hours between two dates
        :param date_1: date 1
        :param date_2: date 2
        :return: float denoting the number of hours
        """
        difference = date_2 - date_1
        return difference.total_seconds() / 60

    @retry(delay=1, backoff=2)
    def authenticate(self, _force=False):

        """
        If the time difference between the last time we renewed our JDBC connection
        is greater that 510 minutes, renew our connection via environment variable info
        :return: nothing
        """
        t_diff = SpliceMachineQueue._time_hrs_difference(self.last_authenticated, datetime.today())
        if t_diff >= 51 or not self.authenticated or _force:
            self.__del__()
            jdbc_url = os.environ.get('JDBC_URL')
            username = os.environ.get('USER')
            password = os.environ.get('PASSWORD')
            jdbc_conn = jaydebeapi.connect("com.splicemachine.db.jdbc.ClientDriver", jdbc_url,
                                           {'user': username,
                                            'password': password, 'ssl': "basic"},
                                           "../utilities/db-client-2.7.0.1815.jar")

            self.jdbc_conn = jdbc_conn

            # establish a JDBC connection to your database
            self.cursor = jdbc_conn.cursor()  # get a cursor
            self.authenticated = True
            self.last_authenticated = datetime.today()
            if _force:
                logger.info('Renewed JDBC Connection')
            else:
                logger.info('FORCED JDBC RECONNECTION!!!! THIS SHOULD\'T HAPPEN!')

    @staticmethod
    def _generate_timestamp():
        """
        Generate a representation of the current time (rounded off the decimal)
        :return: representation of the timestamp
        """
        return round(time.time(), 3)

    @staticmethod
    def _generate_hash(payload):
        """
        Return a unique key for each request
        :param payload: the job payload
        :return: a unique hash
        """
        return md5(dumps(payload).encode('utf-8')).hexdigest()

    def _execute_sql(self, sql, _auth_error=False):
        logger.info(sql)
        try:
            self.cursor.execute(sql)
            return True

        except Exception as e:
            if 'connection' in str(e).lower() and not _auth_error:
                logger.error(e)
                logger.info("Authenticating...")
                self.authenticate(_force=True)
                logger.info("Retrying function...")
                self._execute_sql(sql, _auth_error=True)

            return False

    def create_queue(self):
        """
        Create a queue if it doesn't already exist. A CREATE TABLE IF NOT EXISTS is not a command in
        Splice machine yet
        :return: True if table created, False if it is not created (or errors)
        """
        # TODO add table versioning
        cmd = """
        CREATE TABLE {table} (
            id VARCHAR(100) PRIMARY KEY,
            timestamp FLOAT,
            handler VARCHAR(100),
            status VARCHAR(80),
            payload VARCHAR(600),
            info VARCHAR(6000)
        )
            """.format(table=self.table)
        self.created = True

        return self._execute_sql(cmd)

    def create_state_holder(self):
        """
        Create a state holder in the database (managing the availability of services)
        :return: true if success, false if not
        """
        cmd = """
        CREATE TABLE {state_table} (
            service_handler VARCHAR(100),
            state VARCHAR(50)
        )
        """.format(state_table=self.state_table)
        return self._execute_sql(cmd)

    def set_unknown_services_to_enabled(self, services):
        """
        Set services not already in the state table to enabled, by default
        :param services: the services to insert if not exists
        :return: True if success
        """
        current_services_cmd = 'SELECT service_handler FROM {state_table}'.format(
            state_table=self.state_table)

        self.cursor.execute(current_services_cmd)
        created_services = [i[0] for i in self.cursor.fetchall()]
        logger.info('found services ' + str(created_services))

        for service in services:
            if service not in created_services:
                cmd = """
                INSERT INTO {state_table} VALUES ('{service}', 'ENABLED')
                """.format(state_table=self.state_table, service=service)
                self._execute_sql(cmd)

        return True

    def update_state(self, service, state):
        cmd = """
        UPDATE {state_table}
        SET state='{state}'
        WHERE service_handler='{service}'
        """.format(state_table=self.state_table, state=state, service=service)

        return self._execute_sql(cmd)

    def disable_service(self, service):
        return self.update_state(service, 'DISABLED')

    def enable_service(self, service):
        return self.update_state(service, 'ENABLED')

    def is_service_allowed(self, service):
        """
        returns whether or not a service is allowed to run
        :param service: service handler name
        :return: boolean indicating whether or not run is allowed
        """

        cmd = """SELECT service_name, state FROM {state_table}
                WHERE service_name='{service}'
        """.format(state_table=self.state_table, service=service)

        self.cursor.execute(cmd)

        results = self.cursor.fetchall()[0]

        if results[1] == 'ENABLED':
            return True

        elif results[1] == 'DISABLED':
            return False

        else:
            logger.error('service not found: ' + service)

    def insert(self, job_hash, status, timestamp, handler, payload):
        """
        Insert a Job into Splice Machine Queue
        :param job_hash: unique hash for this payload
        :param status: status of job (probably pending)
        :param timestamp: timestamp in seconds
        :param handler: target handler in the workers
        :param payload: job payload
        :return: True if success, false if not... probably request serviced already as hash is
        primary key
        """
        default_info = 'Waiting to be serviced'
        cmd = """
        INSERT INTO {table} VALUES (
            '{id}',
             {timestamp},
            '{handler}',
            '{status}',
            '{payload}',
            '{info}'
            )
        """.format(table=self.table, status=status, id=job_hash, timestamp=timestamp,
                   handler=handler, payload=dumps(payload), info=default_info)

        logger.info(cmd)
        return self._execute_sql(cmd)

    def enqueue(self, handler, payload):
        """
        Enqueue a job into Splice Machine with a status of "Pending", until it is serviced
        :param handler: the target handler, e.g. deploy or retrain
        :param payload: parameter metadata
        :return:
        """
        self.authenticate()

        timestamp = self._generate_timestamp()  # get timestamp
        # generate a unique id for each job
        job_hash = self._generate_hash(payload)
        status = 'PENDING'  # set an initial status

        success = self.insert(
            job_hash,
            status,
            timestamp,
            handler,
            payload)  # insert into db
        if success:
            logger.info('Success!')
        else:
            logger.error("Failure :(")

        return job_hash

    def upstat(self, job_id, status):
        """
        Update the status of a specific job
        :param job_id: the unique job id
        :param status: the status you would like the job id to have
        :return: True if success, False if failure... maybe record doesn't exist
        """
        self.authenticate()
        cmd = """
        UPDATE {table}
            SET status='{status}'
            WHERE id='{job_id}'
        """.format(table=self.table, status=status, job_id=job_id)

        return self._execute_sql(cmd)

    def upinfo(self, job_id, message):
        """
        Update the verbose info for each job... Could be useful for a GUI later on :)
        :param job_id: unique job id
        :param message: the message you would like the job id to have
        :return:
        """
        self.authenticate()
        cmd = """
           UPDATE {table}
               SET info='{message}'
               WHERE id='{job_id}'
           """.format(table=self.table, message=message, job_id=job_id)

        return self._execute_sql(cmd)

    def delete_from_queue(self, job_id):
        """
        Delete a specific job. We shouldn't really use this, as it has a high cost with a ton of
        rows... just change the status to "terminated"
        :param job_id: the unique job id
        :return: True if success, False if failure... maybe the record doesn't exist?
        """
        self.authenticate()
        cmd = """
        DELETE FROM {table}
            WHERE id='{job_id}'
        """.format(table=self.table, job_id=job_id)

        return self._execute_sql(cmd)

    def dequeue(self, job_id, failed=False):
        """
        :param job_id: unique job id
        :param failed: whether the process failed
        :return: True if success, False if not... maybe record doesn't exist?
        """
        self.authenticate()
        if failed:
            return self.upstat(job_id, 'FAILED')
        return self.upstat(job_id, 'FINISHED')

    def service_job(self, _auth_error=False):
        """
        Service the first job in the queue
        :return: namedtuple containing task info
        """
        self.authenticate()
        cmd = """
        SELECT TOP 1 * FROM ML.JOBS
            WHERE status='PENDING'
            ORDER BY timestamp
        """
        self.cursor.execute(cmd)
        try:
            db_task = self.cursor.fetchall()[0]
            old_task = self.Job(*db_task)
            task = self.Job(
                old_task.job_id,
                old_task.timestamp,
                old_task.handler,
                old_task.status,
                loads(
                    old_task.payload),
                old_task.info)

            self.upstat(task.job_id, 'RUNNING')
            self.upinfo(task.job_id, 'Service worker has found your request')
            return task

        except Exception as e:
            if 'connection' in str(e).lower() and not _auth_error:
                logger.error(e)
                logger.info("Authenticating...")
                self.authenticate(_force=True)
                logger.info("Retrying function...")
                self.service_job(_auth_error=True)
