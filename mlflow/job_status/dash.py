import json
import logging
import os
import random
import time
from collections import namedtuple
from datetime import datetime
from hashlib import md5

import cachetools.func
import jaydebeapi
import requests
from flask import Flask, render_template, request

__author__ = "Splice Machine, Inc."
__copyright__ = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__ = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__ = "Commerical"
__version__ = "2.0"
__maintainer__ = "Amrit Baveja"
__email__ = "abaveja@splicemachine.com"
__status__ = "Quality Assurance (QA)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s (%(lineno)s) - %(levelname)s: %(message)s",
                    datefmt='%Y.%m.%d %H:%M:%S')
logger = logging.getLogger('dash')

logger.setLevel(logging.DEBUG)

app = Flask(__name__)


class JDBCUtils(object):
    """A Class containing some utils to make connecting with JDBC (via Python) easier."""
    jdbc_url = os.environ.get('JDBC_URL')
    username = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    last_authenticated = None
    cursor = None
    Job = namedtuple('Job', ['job_id', 'timestamp', 'handler', 'status', 'payload', 'info'])
    ServiceStatus = namedtuple('ServiceStatus', ['service', 'status'])

    @staticmethod
    def authenticate():
        """
        If the time difference between the last time we renewed our JDBC connection
        is greater that 510 minutes, renew our connection via environment variable info
        :return: nothing
        """
        try:
            if not JDBCUtils.cursor:
                raise Exception  # go to except condition

            JDBCUtils.cursor.execute("VALUES 1")
            record = JDBCUtils.cursor.fetchone()
            logger.info("Tested DB Connection! Active! " + str(record))
            return

        except:
            logger.info("Found bad/nonexistant connection... terminating")

            if JDBCUtils.cursor:
                logger.info("Closed current cursor")
                JDBCUtils.cursor.close()

            try:
                jdbc_conn = jaydebeapi.connect("com.splicemachine.db.jdbc.ClientDriver",
                                               os.environ.get('JDBC_URL'),
                                               {'user': os.environ.get('USER'),
                                                'password': os.environ.get('PASSWORD'),
                                                'ssl': "basic"},
                                               "../utilities/db-client-2.7.0.1815.jar")

                logger.info("Opened new JDBC Connection")
                # establish a JDBC connection to your database
                JDBCUtils.cursor = jdbc_conn.cursor()  # get a cursor
                logger.info("Opened new JDBC Cursor. Success!")

            except Exception as e:
                logger.error("ERROR! " + str(e))
                time.sleep(2)
                JDBCUtils.authenticate()

    @staticmethod
    @cachetools.func.ttl_cache(maxsize=1024, ttl=10)
    def get_jobs():
        """Get Jobs From Splice Machine DB (cache results for 10 secs so page won't load extremely
        slowly every time
        :return: list of namedtuples containing jobs, or False if exception

        """
        JDBCUtils.authenticate()
        logger.debug('Getting Jobs')
        try:
            JDBCUtils.cursor.execute('SELECT * FROM ML.JOBS ORDER BY TIMESTAMP DESC')
            results = JDBCUtils.cursor.fetchall()
            old_jobs = [JDBCUtils.Job(*job) for job in results]
            jobs = [
                JDBCUtils.Job(o.job_id,
                              datetime.fromtimestamp(o.timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                              o.handler,
                              o.status,
                              json.loads(o.payload),
                              o.info)
                for o in old_jobs
            ]

            return jobs

        except Exception as e:
            logger.error(e)
            return False

    @staticmethod
    @cachetools.func.ttl_cache(maxsize=2048, ttl=37)
    def get_statuses():
        """
        Get Service Handler Statuses from Splice Machine DB (cache results for 37 secs so page won't
        load extremely slowly
        :return: list of namedtuples containing handler statuses, or False if exception
        """
        JDBCUtils.authenticate()
        logger.debug('Getting Statuses')
        try:
            JDBCUtils.cursor.execute('SELECT * FROM ML.ACTIVE_SERVICES')
            results = JDBCUtils.cursor.fetchall()
            statuses = [JDBCUtils.ServiceStatus(*serv_stat) for serv_stat in results]
            return statuses

        except Exception as e:
            logger.error(e)
            return False


INITIAL_JOBS = JDBCUtils.get_jobs()  # initially get the jobs on startup so that page load is faster
INITIAL_STATUSES = JDBCUtils.get_statuses()  # initially get the statuses on startup so that page
# load is faster
COMPLETED_INITIAL_DATABASE_RETRIEVAL = [False]  # put in a list to comply with PEP 8 of not
# using "global"


@app.route('/', methods=['GET'])
@app.route('/dash', methods=['GET'])
def dash():
    """Flask Dashboard route -- renders HTML template dash.html. Can be accessed via "/" or "/dash"
    on the port it is running
    :return: HTML file rendered by browser

    """
    if not COMPLETED_INITIAL_DATABASE_RETRIEVAL[0]:
        logger.debug("USING INTIAL DATABASE RETRIEVAL METHOD")
        jobs = INITIAL_JOBS
        statuses = INITIAL_STATUSES
        COMPLETED_INITIAL_DATABASE_RETRIEVAL[0] = True
    else:
        jobs = JDBCUtils.get_jobs()
        statuses = JDBCUtils.get_statuses()

    return render_template('dash.html', jobs=jobs,
                           statuses=statuses)


@app.route('/deploy', methods=['GET', 'POST'])
def deploy():
    """
    Wrapper around deployment REST API
    :return: checkmark, deploy or json depending on request or status
    """
    if request.method == 'GET':
        return render_template('deploy.html')
    elif request.method == 'POST':
        host = 'http://0.0.0.0:{api_port}/deploy'.format(api_port=os.environ['API_PORT'])
        assembled_metadata = {
            'handler': 'deploy',
            'experiment_id': request.form['experiment_id'],
            'run_id': request.form['run_id'],
            'region': request.form['region'],
            # 'iam_role': request.form['iam_role'],
            'postfix': 'spark_model',
            'instance_type': request.form['instance_type'],
            'instance_count': request.form['instance_count'],
            'deployment_mode': request.form['deployment_mode'],
            'app_name': request.form['app_name'],
            'random_string': str(md5(str(random.randint(0, 10 ** 5)).encode('utf-8')).hexdigest())
        }

        r = requests.post(host, json=assembled_metadata)
        if r.ok:
            return render_template('check.html')
        else:
            return r.content
    else:
        return '<h1>Request not understood</h1>'


@app.route('/check', methods=['GET'])
def success():
    return render_template("check.html")


@app.route('/toggle', methods=['GET', 'POST'])
def toggle():
    services_allowed_to_change = ['deploy', 'retrain']
    actions_allowed = {
        'enable': 'start',
        'disable': 'stop'
    }

    if request.method == 'GET':
        return render_template('toggle_services.html', services=services_allowed_to_change,
                               actions=actions_allowed.items())

    elif request.method == 'POST':

        service = request.form['service']
        action = request.form['action']
        host = 'http://0.0.0.0:{api_port}/service/{service}/{action}'.format(
            api_port=os.environ['API_PORT'], service=service,
            action=action)

        r = requests.post(host)
        if r.ok:
            return render_template('check.html')
        else:
            return r.content


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)
