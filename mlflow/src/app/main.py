from collections import defaultdict
from json import dumps as serialize_json, loads as parse_json
from os import environ as env_vars
from time import time as timestamp

import requests
from flask import request, url_for, render_template as show_html, redirect, jsonify as create_json, Flask, Response
from flask_executor import Executor
from flask_login import (LoginManager, current_user, login_required,
                         login_user, logout_user)
from shared.api.models import APIStatuses
from shared.api.responses import HTTP
from shared.environments.cloud_environment import (CloudEnvironment,
                                                   CloudEnvironments)
from shared.logger.logging_config import logger
from shared.models.splice_models import Handler, Job, RecurringJob
from shared.services.authentication import Authentication, User
from shared.services.database import DatabaseSQL, SQLAlchemyClient
from shared.services.handlers import HandlerNames, KnownHandlers
from sqlalchemy.orm import load_only

from ui_utils import handle_bootgrid_query

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

APP: Flask = Flask("director")
APP.config['EXECUTOR_PROPAGATE_EXCEPTIONS'] = True
APP.config['SECRET_KEY'] = "B1gd@t@4U!"  # cookie encryption

LOGIN_MANAGER: LoginManager = LoginManager(APP)  # session-based user authentication
CLOUD_ENVIRONMENT: CloudEnvironment = CloudEnvironments.get_current()

Session = None  # db session-- created with every request

EXECUTOR: Executor = Executor(APP)  # asynchronous parallel processing
BOBBY_URI = env_vars.get('BOBBY_URL', 'http://bobby')


# Flask App Configuration
@APP.before_request
def create_session() -> None:
    """
    Create a Session-Local SQLAlchemy Session
    """
    global Session
    Session = SQLAlchemyClient.SessionFactory()


@APP.after_request
def remove_session(response: Response) -> Response:
    """
    Remove Session-local SQLAlchemy Session
    and expunge all associated objects
    :param response: (Response) the response to return

    :return: (Response) response object passed in
    """
    global Session
    SQLAlchemyClient.SessionFactory.remove()
    return response


@APP.context_processor
def create_global_jinja_variables():
    """
    Create a dictionary of global Jinja2
    variables that can be accessed in any
    template

    :return: (dict) Dictionary of key/values
        mapping global variables to the corresponding
        Jinja Variables
    """
    return dict(
        cloud_environment_name=CLOUD_ENVIRONMENT.name,
        known_handlers=KnownHandlers,
        handler_names=HandlerNames,
        can_deploy=CLOUD_ENVIRONMENT.can_deploy
    )


# Login Configuration
@LOGIN_MANAGER.user_loader
def user_loader(username: str) -> User:
    """
    Return a user from the Session
    
    :param username: (str) user to form object from
    :return: (User) constructed user object
    """
    return User(username)


@LOGIN_MANAGER.unauthorized_handler
def unauthorized_user():
    """
    Return redirect to login
    if user is unauthorized
    :return: (redirect) redirect in browser to /login
    """
    if current_user.is_authenticated:
        return redirect('/')
    return redirect(url_for('login'))


# Login Routes
@APP.route('/login', methods=['GET', 'POST'])
def login() -> Response:
    """
    Show Login HTML to users if GET,
    otherwise validate credentials against DB
    :return:
    """
    if request.method == 'GET':
        if current_user.is_authenticated:
            return redirect('/')
        return show_html('login.html')

    username: str = request.form['user']
    if Authentication.validate_auth(username, request.form['pw']):
        user: User = User(username)
        login_user(user)
        return redirect(request.args.get("next") or url_for('home'))

    return show_html('login.html', unauthorized=True)


@APP.route("/logout")
@login_required
def logout() -> redirect:
    """
    Logout the current logged in
    user
    :returns: (redirect) redirect to login
    """
    logout_user()
    return redirect(url_for('login'))


# Api Routes
@APP.route('/api/ui/logs', methods=['POST'])
@login_required
@HTTP.generate_json_response
def get_job_logs_ui():
    """
    Retrieve the Job Logs for the UI
    :return: (dict) job logs
    """
    return dict(logs=_get_logs(task_id=request.json['task_id']))


@APP.route('/api/rest/logs', methods=['POST'])
@Authentication.basic_auth_required
@HTTP.generate_json_response
def get_job_logs_api():
    """
    Retrieve the Job Logs for the API
    :return: (dict) job logs
    """
    return dict(logs=_get_logs(task_id=request.json['task_id']))


def _get_logs(task_id):
    """
    Retrieve the logs for the specified task di
    :param task_id: the task id to retrieve the logs for
    :return: the logs in an array
    """
    job_id = task_id
    job = Session.query(Job).options(load_only("logs")).filter_by(id=job_id).one()
    return job.logs.split('\n')


@APP.route('/api/ui/initiate', methods=['POST'])
@HTTP.generate_html_in_home_response
@login_required
def initiate_job_ui() -> dict:
    """
    Initiate a Job from a web form-- redirects
    to the home page with a message detailing the
    status of the new job.
    :return: (dict) output from queue submission
    """
    handler: Handler = KnownHandlers.MAPPING.get(request.form['handler_name'].upper())
    return handler_queue_job(request.form, handler, user=request.form['user'])


@APP.route('/api/rest/initiate', methods=['POST'])
@HTTP.generate_json_response
@Authentication.basic_auth_required
def initiate_job_rest() -> dict:
    """
    Initiate job from the REST API-- returns
    JSON containing traceback and status
    :return: (dict) JSON to return
    """
    handler: Handler = KnownHandlers.MAPPING.get(request.json['handler_name'].upper())
    if not handler:
        message: str = f"Handler {handler} is an unknown service"
        logger.error(message)
        return HTTP.responses['malformed'](create_json(status=APIStatuses.failure, message=message))

    return handler_queue_job(request.json, handler, user=request.authorization.username)


def handler_queue_job(request_payload: dict, handler: Handler, user: str) -> dict:
    """
    Handler for actions that execute services
    e.g. deploy, retrain.
    :param request_payload: (dict) payload to parse to create job
    :param handler: (Handler) the handler object
    :param user: (str) Username of the person who submitted the job
    :return: (Response) JSON payload for success
    """
    # Format Payload
    payload: dict = {field.name: field.get_value(request_payload.get(field.name) or None) for field in
                     handler.payload_args
                     if field.name != 'payload'}

    job: Job = Job(handler_name=handler.name, user=user, payload=serialize_json(payload))

    Session.add(job)
    Session.commit()
    Session.merge(job)  # get identity col

    try:
        # Tell bobby there's a new job to process
        requests.post(f"{BOBBY_URI}:2375/job")
    except ConnectionError:
        logger.warning('Bobby was not reachable by MLFlow. Ensure Bobby is running. \nThe job has'
                       'been added to the database and will be processed when Bobby is running again.')
    return dict(job_status=APIStatuses.pending,
                job_id=job.id,
                timestamp=timestamp())  # turned into JSON and returned


# UI Routes
@APP.route('/api/ui/get_monthly_aggregated_jobs', methods=['GET'])
@HTTP.generate_json_response
@login_required
def get_monthly_aggregated_jobs() -> dict:
    """
    Get the monthly aggregated job counts
    for the chart on the main page
    :return: (dict) response for the javascript to render
    """

    results: list = list(Session.execute(str(DatabaseSQL.get_monthly_aggregated_jobs)))

    data: defaultdict = defaultdict(lambda: [0] * 12)  # initialize a dictionary
    # that for every new key, creates an array of 12 zeros: one for every month of the year

    total_count: int = 0
    for row in results:
        month, count, user = row
        total_count += count
        data[user][int(month - 1)] = count

    return dict(data=data, total=total_count)


@APP.route('/api/ui/get_handler_data', methods=['GET'])
@HTTP.generate_json_response
@login_required
def get_handler_data() -> dict:
    """
    Get a count of the enabled handlers
    currently in the database
    :return: (dict) response containing
        number of enabled handlers
    """
    enabled_handlers_query = Session.query(
        Handler.name, Handler.enabled
    ).filter(
        Handler.modifiable == 1  # don't want access modifiers
    )
    results: list = Session.execute(enabled_handlers_query)
    return dict(data=[tuple(res) for res in results])


@APP.route('/api/ui/get_jobs', methods=['POST'])
@HTTP.generate_json_response
@login_required
def get_jobs_ui() -> dict:
    """
    Get jobs from UI
    :return: (dict) JSON response rendered in front end
    """
    return handle_bootgrid_query(session=Session, request=request, executor=EXECUTOR)


@APP.route('/api/rest/get_jobs', methods=['POST'])
@Authentication.basic_auth_required
@HTTP.generate_json_response
def get_jobs_rest():
    """
    Get jobs from DB
    :return: (dict) JSON response
    """
    jobs = Session.query(Job).all()

    serialized_jobs = []
    for job in jobs:
        parsed_url = job.parse_url() or {}
        serialized_jobs.append(
            dict(job_id=job.job_id, timestamp=job.timestamp, handler_name=job.handler_name,
                 parent_job_id=job.parent_job_id, status=job.status, payload=parse_json(job.payload),
                 user=job.user, target_service=job.target_service, **parsed_url)
        )
    return {"jobs": serialized_jobs}


@APP.route('/api/rest/get_recurring_jobs', methods=['POST'])
@Authentication.basic_auth_required
@HTTP.generate_json_response
def get_recurring_jobs_rest():
    """
    Get recurring jobs from database
    :return: (dict) JSON response
    """
    recurring_jobs = Session.query(RecurringJob).limit(request.json['limit']).all()

    serialized_jobs = []
    for r_job in recurring_jobs:
        serialized_jobs.append(
            dict(name=r_job.name, creation_timestamp=r_job.creation_timestamp,
                 status=r_job.status, job_id=r_job.job_id, entity_id=r_job.entity_id,
                 payload=r_job.job.payload)
        )

    return {"jobs": serialized_jobs}


# HTML Routes
# doesn't matter which name is used to generate the URL since both handlers
# are started from this page
@APP.route(KnownHandlers.get_url(HandlerNames.enable_service), methods=['GET'])
@login_required
def access() -> Response:
    """
    Return HTML containing Access Modifier Page
    :return: (Response) HTML
    """
    return show_html('access.html', handlers=KnownHandlers.get_modifiable())


@APP.route('/contact', methods=['GET'])
@login_required
def contact() -> Response:
    """
    Return HTML containing Contact
    :return: (Response) HTML
    """
    return show_html('contact.html')


@login_required
def deploy_csp() -> Response:
    """
    Return HTML Containing Cloud Service
    Specific Deployment Form
    :return: (Response)
    """
    # templates for deployment in app/templates  should be formatted like
    # 1) deploy_aws.html, 2) deploy_azure.html, 3) deploy_gcp.html
    # they need to match the names given to the CloudEnvironments
    # given in ml-workflow-lib/shared/database.py/splice_models.py:KnownHandlers
    return show_html(f'deploy_{CLOUD_ENVIRONMENT.name.lower()}.html')


@APP.route(KnownHandlers.get_url(HandlerNames.deploy_k8s))
@login_required
def deploy_k8s() -> Response:
    """
    Return HTML containing Kubernetes
    Deployment form
    :return: (Response)
    """
    return show_html('deploy_kubernetes.html')


@APP.route(KnownHandlers.get_url(HandlerNames.deploy_database))
@login_required
def deploy_database() -> Response:
    """
    Return HTML containing Database Deployment form
    :return: (Response)
    """
    return show_html('deploy_database.html')


@APP.route('/tracker', methods=['GET'])
@login_required
def tracker() -> Response:
    """
    Return HTML containing Tracker table
    :return: (Response) HTML
    """
    return show_html('tracker.html')


@APP.route('/', methods=['GET'])
@login_required
def home() -> Response:
    """
    Serves up home page for MLManager
    Director
    :return: (Response) HTML
    """
    return show_html('index.html')


@APP.route('/watch/<int:task_id>', methods=['GET'])
@login_required
def watch_job(task_id: int) -> Response:
    """
    Serves up the logs watching page
    for MLManager Director
    :param task_id: the id to watch
    :return: (Response) HTML
    """
    return show_html('watch_logs.html', task_id=task_id)


if CLOUD_ENVIRONMENT.can_deploy:
    APP.add_url_rule(KnownHandlers.get_url(HandlerNames.deploy_csp), 'deploy_csp', view_func=deploy_csp)

if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=5000)
