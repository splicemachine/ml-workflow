from collections import defaultdict
from json import dumps as serialize_json
from time import time as timestamp

from flask import Flask, request, Response, jsonify as create_json, render_template as show_html, \
    redirect, url_for
from flask_executor import Executor
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from mlmanager_lib import CloudEnvironment, CloudEnvironments
from mlmanager_lib.database.handlers import KnownHandlers, HandlerNames
from mlmanager_lib.database.models import SessionFactory, Job, Handler
from mlmanager_lib.logger.logging_config import logging
from mlmanager_lib.rest.authentication import Authentication, User
from mlmanager_lib.rest.constants import APIStatuses, TrackerTableMapping
from mlmanager_lib.rest.responses import HTTP
from sqlalchemy import text

# TODO: add basic auth for internal API endpoints

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

APP: Flask = Flask(__name__)
APP.config['EXECUTOR_PROPAGATE_EXCEPTIONS']: bool = True
APP.config['SECRET_KEY']: str = "B1gd@t@4U!"  # cookie encryption

EXECUTOR: Executor = Executor(APP)  # asynchronous parallel processing
LOGIN_MANAGER: LoginManager = LoginManager(APP)  # session-based user authentication

CLOUD_ENVIRONMENT: CloudEnvironment = CloudEnvironments.get_current()

Session = None  # db session-- created with every request

LOGGER = logging.getLogger(__name__)


# Flask App Configuration
@APP.before_request
def create_session() -> None:
    """
    Create a Session-Local SQLAlchemy Session
    """
    global Session
    Session = SessionFactory()


@APP.after_request
def remove_session(response: Response) -> Response:
    """
    Remove Session-local SQLAlchemy Session
    and expunge all associated objects
    :param response: (Response) the response to return

    :return: (Response) response object passed in
    """
    global Session
    SessionFactory.remove()
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
        handler_names=HandlerNames
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
    # check against Zeppelin (Apache Shiro into DB)
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
@APP.route('/api/ui/initiate/', methods=['POST'])
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
    return handler_queue_job(request.form, handler)


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
        LOGGER.error(message)
        return HTTP.responses['malformed'](create_json(status=APIStatuses.failure, message=message))

    return handler_queue_job(request.json, handler)


def handler_queue_job(request_payload: dict, handler: Handler) -> dict:
    """
    Handler for actions that execute services
    e.g. deploy, retrain.
    :param request_payload: (dict) payload to parse to create job
    :param handler: (Handler) the handler object
    :return: (Response) JSON payload for success
    """
    # Format Payload
    payload: dict = {}
    for required_key in handler.required_payload_args:
        payload[required_key] = request_payload[required_key]

    for optional_key in handler.optional_payload_args:
        supplied_value: object = payload.get(optional_key)
        payload[optional_key] = supplied_value if supplied_value \
            else handler.optional_payload_args[optional_key]

    job: Job = Job(handler_name=handler.name,
                   user=request_payload['user'],
                   payload=serialize_json(payload))

    Session.add(job)
    Session.commit()

    return dict(job_status=APIStatuses.pending,
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

    results: list = list(Session.execute(str(f"""
        SELECT MONTH(INNER_TABLE.parsed_date) AS month_1, COUNT(*) AS count_1, user_1
        FROM (
            SELECT TIMESTAMP("timestamp") AS parsed_date, "user" as user_1
            FROM {Job.__table_schema_name__}
        ) AS INNER_TABLE
        WHERE YEAR(INNER_TABLE.parsed_date) = YEAR(CURRENT_TIMESTAMP)
        GROUP BY 1, 3
    """)))

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
def get_jobs() -> dict:
    """
    As a temporary workaround,
    we use SQL instead of SQLAlchemy
    since driver does not support offset yet.
    Since this is also a fairly complex query--
    SQL is more efficient
    :return: (dict) JSON response rendered in front end
    """
    job_table: str = "MLMANAGER.JOBS"

    # Parse Table Order Information
    order_arg: str = list(filter(lambda key: key.startswith('sort'), request.form))[0]
    order_value: str = order_arg.split('[')[1].split(']')[0]  # parse sort[column] -> column

    direction_suffix: str = "DESC" if request.form[order_arg] == 'desc' else ''
    limit: int = int(request.form['rowCount']) if request.form.get('rowCount') != '-1' else 0

    # AJAX from jquery-bootgrid has -1 if user selects no limit
    if request.form['searchPhrase']:  # query is doing a search on HTML table
        int_offset: int = 0  # no offset on searches
        table_query: text = _get_job_search_query(job_table, order_value, direction_suffix, limit,
                                                  request.form['searchPhrase'])
    else:  # query is listing
        int_offset: int = int(request.form['current']) - 1 if request.form.get('current') else 0
        table_query: text = _get_job_list_query(job_table, order_value, direction_suffix, limit,
                                                int_offset)

    total_query: text = f"""SELECT COUNT(*) FROM {job_table}"""  # how many pages to make in js

    # submit to threading and gather results-- we can execute these in || for faster execution
    futures: list = [
        EXECUTOR.submit(lambda: [_preformat_job_row(row) for row in Session.execute(table_query)]),
        EXECUTOR.submit(lambda: list(Session.execute(total_query))[0][0])
    ]

    table_data, total_rows = futures[0].result(), futures[1].result()  # block until we get results

    return dict(rows=table_data,
                current=int_offset + 1,
                total=total_rows,
                rowCount=limit)


def _preformat_job_row(job_row: list) -> dict:
    """
    Format job row object to have some
    columns preformatted (<pre></pre> for table rendering
    :param job_row: (ResultProxy) the Job Row to format
    :return: (dict) formatted row
    """
    job_object: dict = dict(job_row)
    for column in TrackerTableMapping.preformatted_columns:
        if column in job_object:
            job_object[column] = f'<pre>{job_object[column]}</pre>'
    return job_object


def _get_job_search_query(job_table: str, order_col: str, direction: str, limit: int,
                          search_term: str) -> text:
    """
    Get SQL Query to search columns for search string

    :param job_table: (str) Table in DB where jobs are stored
    :param order_col: (str) column to sort by
    :param direction: (str) either desc or nothing (ascending)
    :param limit: (int) number of rows to retrieve
    :param search_term: (str) term to look for in searchable columns
    :return: (text) SELECT statement
    """
    limit_clause: str = f'FETCH FIRST {limit} ROWS ONLY' if limit > 0 else ''

    filter_clause: str = f" LIKE '%{search_term}%' OR "
    filter_expression: str = filter_clause.join(
        TrackerTableMapping.searchable_columns) + filter_clause[:-4]  # cut off OR on last column

    return text(
        f"""
        SELECT {TrackerTableMapping.sql_columns} FROM {job_table}
        WHERE {filter_expression} 
        ORDER BY {TrackerTableMapping.DB_MAPPING[order_col]} {direction}
        {limit_clause}
        """
    )


def _get_job_list_query(job_table: str, order_col: str, direction: str, limit: int,
                        offset: int) -> text:
    """
    Get JQuery Bootgrid formatted JSON (no search) for
    rendering in HTML Table for /tracker.

    :param job_table: (str) Table in DB where jobs are stored
    :param order_col: (str) column to sort by
    :param direction: (str) either desc or nothing (ascending)
    :param limit: (int) number of rows to retrieve
    :param offset: (int) number of rows to skip
    :return: (text) SELECT statement
    """
    limit_clause: str = f'FETCH NEXT {limit} ROWS ONLY' if limit > 0 else ''
    return text(
        f"""
        SELECT {TrackerTableMapping.sql_columns} FROM {job_table}
        ORDER BY {TrackerTableMapping.DB_MAPPING[order_col]} {direction}
        OFFSET {offset} ROWS
        {limit_clause}
        """
    )


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


@APP.route('/api_info', methods=['GET'])
@login_required
def api_info() -> Response:
    """
    Return HTML containing API info
    :return: (Response) HTML
    """
    return show_html('api.html')


@APP.route('/contact', methods=['GET'])
@login_required
def contact() -> Response:
    """
    Return HTML containing Contact
    :return: (Response) HTML
    """
    return show_html('contact.html')


@APP.route(KnownHandlers.get_url(HandlerNames.deploy_csp))
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
    # given in ml-workflow-lib/mlmanager_lib/database/models.py:KnownHandlers
    return show_html(f'deploy_{CLOUD_ENVIRONMENT.name.lower()}.html')


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


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=5000)
