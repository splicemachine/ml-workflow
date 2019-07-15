import logging
import os
import time
from collections import namedtuple
from datetime import datetime
from hashlib import md5
from json import dumps, loads

import cachetools.func
import jaydebeapi

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
                    datefmt='%Y.%m.%d %H:%M:%S')
logger = logging.getLogger('queue')

__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__ = "Commerical"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"


class SpliceMachineQueue(object):
    """
    A Queue that uses the Splice Machine RDBMS as its backend
    """
    Job = namedtuple(
        'Job', ['job_id', 'timestamp', 'handler', 'status', 'payload', 'info']
    )
    table = 'ML.JOBS'
    state_table = 'ML.ACTIVE_SERVICES'
    ServiceStatus = namedtuple('ServiceStatus', ['service', 'status'])

    def __init__(self):
        self.authenticated = False
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

    def if_variable_exists(self, string_var):
        """
        Check if a variable exists in the
        context that this function is run
        :param string_var: the variable name (string)
        :return: boolean, whether it exists or not
        """
        try:
            exec (string_var)
            return True
        except:
            return False

    def authenticate(self):
        """
        If the time difference between the last time we renewed our JDBC connection
        is greater that 510 minutes, renew our connection via environment variable info
        :return: nothing
        """
        try:
            if not self.authenticated:
                raise Exception  # go to exception case

            self.cursor.execute("VALUES 1")
            record = self.cursor.fetchone()
            logger.info("Tested DB Connection! Active! " + str(record))
            return
        except:
            logger.info("Found bad/nonexistant connection... terminating")

            if hasattr(self, 'cursor'):
                logger.info("Closed current cursor")
                self.cursor.close()

            try:  # TODO find a replacement for jaydebeapi
                jdbc_conn = jaydebeapi.connect("com.splicemachine.db.jdbc.ClientDriver",
                                               os.environ.get('JDBC_URL'),
                                               {'user': os.environ.get('USER'),
                                                'password': os.environ.get('PASSWORD'),
                                                'ssl': "basic"},
                                               "../scripts/db-client-2.7.0.1815.jar")

                logger.info("Opened new JDBC Connection")
                # establish a JDBC connection to your database
                self.cursor = jdbc_conn.cursor()  # get a cursor
                logger.info("Opened new JDBC Cursor. Success!")
                self.authenticated = True
            except Exception as e:
                logger.error("ERROR! " + str(e))
                time.sleep(2)
                self.authenticate()

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
        """
        Execute given sql
        :param sql: sql to execute
        :param _auth_error: whether there was an authentication
            error or not
        :return: boolean, whether successful or not
        """
        logger.info(sql)
        self.authenticate()
        try:
            self.cursor.execute(sql)
            return True

        except Exception as e:
            if 'connection' in str(e).lower() and not _auth_error:
                logger.error(e)
                logger.info("Authenticating...")
                self.authenticate()
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
            """.format(table=SpliceMachineQueue.table)
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
        """.format(state_table=SpliceMachineQueue.state_table)
        return self._execute_sql(cmd)

    def set_unknown_services_to_enabled(self, services):
        """
        Set services not already in the state table to enabled, by default
        :param services: the services to insert if not exists
        :return: True if success
        """
        current_services_cmd = 'SELECT service_handler FROM {state_table}'.format(
            state_table=SpliceMachineQueue.state_table)

        self.cursor.execute(current_services_cmd)
        created_services = [i[0] for i in self.cursor.fetchall()]
        logger.info('found services ' + str(created_services))

        for service in services:
            if service not in created_services:
                cmd = """
                INSERT INTO {state_table} VALUES ('{service}', 'ENABLED')
                """.format(state_table=SpliceMachineQueue.state_table, service=service)
                self._execute_sql(cmd)

        return True

    def update_state(self, service, state):
        cmd = """
        UPDATE {state_table}
        SET state='{state}'
        WHERE service_handler='{service}'
        """.format(state_table=SpliceMachineQueue.state_table, state=state, service=service)

        return self._execute_sql(cmd)

    @cachetools.func.ttl_cache(maxsize=1024, ttl=10)
    def get_jobs(self):
        """Get Jobs From Splice Machine DB (cache results for 10 secs so page won't load extremely
        slowly every time)

        :return: list of namedtuples containing jobs, or False if exception
        """
        self.authenticate()
        logger.debug('Getting Jobs')
        try:
            self.cursor.execute('SELECT * FROM ML.JOBS ORDER BY TIMESTAMP DESC')
            results = self.cursor.fetchall()
            old_jobs = [SpliceMachineQueue.Job(*job) for job in results]
            jobs = [
                SpliceMachineQueue.Job(o.job_id,
                         datetime.fromtimestamp(o.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                         o.handler,
                         o.status,
                         loads(o.payload),
                         o.info)
                for o in old_jobs
            ]

            return jobs

        except Exception as e:
            logger.error(e)
            return False

    @cachetools.func.ttl_cache(maxsize=2048, ttl=37)
    def get_statuses(self):
        """
        Get Service Handler Statuses from Splice Machine DB (cache results for 37 secs so page won't
        load extremely slowly)
        :return: list of namedtuples containing handler statuses, or False if exception
        """
        self.authenticate()
        logger.debug('Getting Statuses')
        try:
            self.cursor.execute('SELECT * FROM ML.ACTIVE_SERVICES')
            results = self.cursor.fetchall()
            statuses = [SpliceMachineQueue.ServiceStatus(*serv_stat) for serv_stat in results]
            return statuses

        except Exception as e:
            logger.error(e)
            return False

    def disable_service(self, service):
        """
        Disable a given service
        :param service: service to disable
        :return: boolean indicating success
        """
        return self.update_state(service, 'DISABLED')

    def enable_service(self, service):
        """
        Enable a given service
        :param service: service to enable
        :return: boolean indicating success
        """
        return self.update_state(service, 'ENABLED')

    def is_service_allowed(self, service):
        """
        returns whether or not a service is allowed to run
        :param service: service handler name
        :return: boolean indicating whether or not run is allowed
        """

        cmd = """SELECT service_handler, state FROM {state_table}
                WHERE service_handler='{service}'
        """.format(state_table=SpliceMachineQueue.state_table, service=service)

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
        """.format(table=SpliceMachineQueue.table, status=status, id=job_hash, timestamp=timestamp,
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

        cmd = """
        UPDATE {table}
            SET status='{status}'
            WHERE id='{job_id}'
        """.format(table=SpliceMachineQueue.table, status=status, job_id=job_id)

        return self._execute_sql(cmd)

    def upinfo(self, job_id, message):
        """
        Update the verbose info for each job... Could be useful for a GUI later on :)
        :param job_id: unique job id
        :param message: the message you would like the job id to have
        :return:
        """

        cmd = """
           UPDATE {table}
               SET info='{message}'
               WHERE id='{job_id}'
           """.format(table=SpliceMachineQueue.table, message=message, job_id=job_id)

        return self._execute_sql(cmd)

    def delete_from_queue(self, job_id):
        """
        Delete a specific job. We shouldn't really use this, as it has a high cost with a ton of
        rows... just change the status to "terminated"
        :param job_id: the unique job id
        :return: True if success, False if failure... maybe the record doesn't exist?
        """

        cmd = """
        DELETE FROM {table}
            WHERE id='{job_id}'
        """.format(table=SpliceMachineQueue.table, job_id=job_id)

        return self._execute_sql(cmd)

    def dequeue(self, job_id, failed=False):
        """
        :param job_id: unique job id
        :param failed: whether the process failed
        :return: True if success, False if not... maybe record doesn't exist?
        """

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
            old_task = SpliceMachineQueue.Job(*db_task)
            task = SpliceMachineQueue.Job(
                old_task.job_id,
                old_task.timestamp,
                old_task.handler,
                old_task.status,
                loads(
                    old_task.payload),
                old_task.info)

            self.upstat(task.job_id, 'RUNNING')
            self.upinfo(task.job_id, 'Service src has found your request')
            return task

        except Exception as e:
            if 'connection' in str(e).lower() and not _auth_error:
                logger.error(e)
                logger.info("Authenticating...")
                self.authenticate()
                logger.info("Retrying function...")
                self.service_job(_auth_error=True)
