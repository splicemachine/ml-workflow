import logging
import os
import random
from hashlib import md5

import requests
from flask import Flask, render_template, request

from splicemachine_queue import SpliceMachineQueue

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

queue = SpliceMachineQueue()

INITIAL_JOBS = queue.get_jobs()  # initially get the jobs on startup so that page load is faster
INITIAL_STATUSES = queue.get_statuses()  # initially get the statuses on startup so that page
#  load is faster
COMPLETED_INITIAL_DATABASE_RETRIEVAL = [
    False]  # put in a list to comply with PEP 8 of not using global


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
        jobs = queue.get_jobs()
        statuses = queue.get_statuses()

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
            'postfix': 'spark_model',
            'instance_type': request.form['instance_type'],
            'instance_count': request.form['instance_count'],
            'deployment_mode': request.form['deployment_mode'],
            'app_name': request.form['app_name']
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
    """
    The page with the checkmark animation after
    job is successfully queued. Should be rendered in
    <iframe>
    :return: template
    """
    return render_template("check.html")


@app.route('/toggle', methods=['GET', 'POST'])
def toggle():
    """
    Toggle the availability of jobs
    :return: JSON if POST, toggle page if GET
    """
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
