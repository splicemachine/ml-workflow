import json
import logging
import os
from collections import namedtuple
from datetime import datetime

import cachetools.func
import jaydebeapi
import requests
from flask import Flask, render_template, request

logging.basicConfig()
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
    def _renew():
        """Establish a new JDBC Connection to the server
        :return: nothing
        """
        jdbc_conn = jaydebeapi.connect("com.splicemachine.db.jdbc.ClientDriver",
                                       JDBCUtils.jdbc_url,
                                       {'user': JDBCUtils.username,
                                        'password': JDBCUtils.password, 'ssl': "basic"},
                                       "../utilities/db-client-2.7.0.1815.jar")
        JDBCUtils.cursor = jdbc_conn.cursor()
        JDBCUtils.last_authenticated = datetime.today()

    @staticmethod
    def renew_jdbc_connection():
        """Refresh every 51 mins, so that we won't time out JDBC connection
        :return: nothing
        """
        if JDBCUtils.last_authenticated:
            logger.debug('Checking JDBC Connection')
            last_renewed_diff = datetime.today() - JDBCUtils.last_authenticated

            logger.debug('minutes since last renewal ' + str(last_renewed_diff.total_seconds() /
                                                             60))
            if last_renewed_diff.total_seconds() / 60 >= 51:
                logger.debug('Renewing JDBC Connection')
                JDBCUtils._renew()

        else:
            logger.debug('Renewing JDBC Connection')
            JDBCUtils._renew()

    @staticmethod
    @cachetools.func.ttl_cache(maxsize=1024, ttl=10)
    def get_jobs():
        """Get Jobs From Splice Machine DB (cache results for 10 secs so page won't load extremely
        slowly every time
        :return: list of namedtuples containing jobs, or False if exception

        """
        JDBCUtils.renew_jdbc_connection()
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
        JDBCUtils.renew_jdbc_connection()
        logger.debug('Getting Statuses')
        try:
            JDBCUtils.cursor.execute('SELECT * FROM ML.ACTIVE_SERVICES')
            results = JDBCUtils.cursor.fetchall()
            statuses = [JDBCUtils.ServiceStatus(*serv_stat) for serv_stat in results]
            return statuses

        except Exception as e:
            logger.error(e)
            return False


@app.route('/', methods=['GET'])
@app.route('/dash', methods=['GET'])
def dash():
    """Flask Dashboard route -- renders HTML template dash.html. Can be accessed via "/" or "/dash"
    on the port it is running
    :return: HTML file rendered by browser

    """
    return render_template('dash.html', jobs=JDBCUtils.get_jobs(),
                           statuses=JDBCUtils.get_statuses())


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
            'path': '/mlruns/{experiment_id}/{run_id}/artifacts/'.format(
                experiment_id=request.form['experiment_id'], run_id=request.form['run_id']),
            'region': request.form['region'],
            'postfix': 'pysparkmodel',
            'instance_type': request.form['instance_type'],
            'instance_count': request.form['instance_count'],
            'iam_role': request.form['iam_role'],
            'deployment_mode': request.form['deployment_mode'],
            'app_name': request.form['app_name'],
        }

        r = requests.post(host, json=assembled_metadata)
        if r.ok:
            return render_template('check.html')
        else:
            return r.content
    else:
        return '<h1>Request not understood</h1>'


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
