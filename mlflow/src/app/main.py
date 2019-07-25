from json import dumps as serialize_json
from time import time as timestamp

from flask import Flask, request, Response, jsonify as create_json, render_template as show_html
from flask_executor import Executor
from mlmanager_lib.database.models import SessionFactory, Job, Handler, KnownHandlers
from mlmanager_lib.logger.logging_config import logging
from mlmanager_lib.rest.http_utils import HTTP, APIStatuses, Authentication, TrackerTableMapping
from sqlalchemy import func, and_, text

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

APP: Flask = Flask(__name__)
EXECUTOR: Executor = Executor(APP)
APP.config['EXECUTOR_PROPAGATE_EXCEPTIONS'] = True

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


# Api Routes
@APP.route('/api/initiate/<handler_name>', methods=['POST'])
@HTTP.generate_json_response
@Authentication.login_required
def handler_queue_job(handler_name: str) -> dict:
    """
    Handler for actions that execute services
    e.g. deploy, retrain.

    :param handler_name: (str) the target handler
    :return: (Response) JSON payload for success or
    """
    # Data Validation
    handler: Handler = KnownHandlers.MAPPING.get(handler_name.upper())
    if not handler:
        message: str = f"Handler {handler} is an unknown service"
        LOGGER.error(message)
        return HTTP.responses['malformed'](
            create_json(status=APIStatuses.failure,
                        message=message)
        )
    # Queue Job
    request_data: dict = request.form if request.form else request.json

    payload: dict = {}
    for required_key in handler.required_payload_args:
        payload[required_key] = request_data[required_key]

    for optional_key in handler.optional_payload_args:
        supplied_value: object = request_data.get(optional_key)
        payload[optional_key] = supplied_value if supplied_value \
            else handler.optional_payload_args[optional_key]

    current_time: int = int(timestamp())
    job: Job = Job(timestamp=current_time, handler=handler, payload=serialize_json(payload))

    Session.add(job)
    Session.commit()
    return dict(job_status=APIStatuses.pending,
                timestamp=current_time)  # turned into JSON and returned


# UI Routes
@APP.route('/api/ui/get_monthly_aggregated_jobs', methods=['GET'])
@HTTP.generate_json_response
def get_monthly_aggregated_jobs() -> dict:
    """
    Get the monthly aggregated job counts
    for the chart on the main page
    :return: (dict) response for the javascript to render
    """
    parse_dates_query = Session.query(
        func.TIMESTAMP(Job.timestamp).label('parsed_date')
    ).subquery()

    aggregate_query = Session.query(
        func.MONTH(parse_dates_query.c.parsed_date), func.count(),
    ).filter(
        func.YEAR(parse_dates_query.c.parsed_date) == func.YEAR(func.NOW())
    ).group_by(
        func.MONTH(parse_dates_query.c.parsed_date)
    )
    # essentially parse string dates from the database,
    # group them by their month, filter to only this year.
    # this is for the chart on the home page.

    results: list = [0] * 12
    for month, count in Session.execute(aggregate_query):
        results[month - 1] = count
    return dict(data=results, total=sum(results))


@APP.route('/api/ui/get_enabled_handler_count', methods=['GET'])
@HTTP.generate_json_response
def get_enabled_handler_count() -> dict:
    """
    Get a count of the enabled handlers
    currently in the database
    :return: (dict) response containing
        number of enabled handlers
    """
    enabled_handlers_query = Session.query(
        func.count()
    ).filter(
        and_(Handler.enabled == 1, Handler.mutable == 1)  # don't want access modifiers
    )
    results: list = list(Session.execute(enabled_handlers_query))
    return dict(count=results[0][0])


@APP.route('/api/ui/get_jobs', methods=['POST'])
@HTTP.generate_json_response
def get_jobs():
    """
    As a temporary workaround,
    we use SQL instead of SQLAlchemy
    since driver does not support offset yet.
    TODO @amrit: FIX ME!
    :return:
    """
    job_table: str = "MLMANAGER.JOBS"
    # Parse Table Order Information
    order_arg: str = list(filter(lambda key: key.startswith('sort'), request.form))[0]
    order_value: str = order_arg.split('[')[1].split(']')[0]  # parse sort[column] -> column
    direction_suffix: str = "DESC" if request.form[order_arg] == 'desc' else ''
    limit: int = int(request.form['rowCount']) if request.form.get('rowCount') != '-1' else 0
    if not request.form['searchPhrase']:
        int_offset: int = int(request.form['current']) - 1 if request.form.get('current') else 0
        return _paginate_jobs_list(job_table, order_value, direction_suffix, limit, int_offset)
    else:
        search_term: str = request.form['searchPhrase']
        return _paginate_jobs_search(job_table, order_value, direction_suffix, limit, search_term)


def _paginate_jobs_search(job_table: str, order_col: str, direction: str, limit: int,
                          search_term: str) -> dict:
    """
    Get JQuery Bootgrid formatted JSON (no search) for
    rendering in HTML Table for /tracker.

    :param job_table: (str) Table in DB where jobs are stored
    :param order_col: (str) column to sort by
    :param direction: (str) either desc or nothing (ascending)
    :param limit: (int) number of rows to retrieve
    :param search_term: (str) term to look for in searchable columns
    :return: (dict) response for pagination in frontend
    """
    table_query


def _paginate_jobs_list(job_table: str, order_col: str, direction: str, limit: int,
                        offset: int) -> dict:
    """
    Get JQuery Bootgrid formatted JSON (no search) for
    rendering in HTML Table for /tracker.

    :param job_table: (str) Table in DB where jobs are stored
    :param order_col: (str) column to sort by
    :param direction: (str) either desc or nothing (ascending)
    :param limit: (int) number of rows to retrieve
    :param offset: (int) number of rows to skip
    :return: (dict) response for pagination in frontend
    """
    table_query: str = text(f"""
           SELECT {TrackerTableMapping.sql_columns} FROM {job_table}
           ORDER BY {TrackerTableMapping.DB_MAPPING[order_col]} {direction}
           OFFSET {offset} ROWS
           FETCH NEXT {limit} ROWS ONLY
           """)
    # Run the SQL Queries asynchronously in Parallel to save time via threading
    total_query: str = text(f"""SELECT COUNT(*) FROM {job_table}""")
    futures: list = [
        EXECUTOR.submit(lambda: [dict(row) for row in Session.execute(table_query)]),
        EXECUTOR.submit(lambda: list(Session.execute(total_query))[0][0])
    ]
    table_data, total_rows = futures[0].result(), futures[1].result()

    return dict(rows=table_data,
                current=offset + 1,
                total=total_rows,
                rowCount=limit)


# HTML Routes
@APP.route('/api_info', methods=['GET'])
def api_info() -> Response:
    """
    Return HTML containing API info
    :return: (Response) HTML
    """
    return show_html('api.html')


@APP.route('/contact', methods=['GET'])
def contact() -> Response:
    """
    Return HTML containing Contact
    :return: (Response) HTML
    """
    return show_html('contact.html')


@APP.route('/deploy/deploy_aws', methods=['GET'])
def deploy_aws() -> Response:
    """
    Return HTML containing AWS Deploy form
    :return: (Response) HTML
    """
    return show_html('deploy_aws.html')


@APP.route('/tracker', methods=['GET'])
def tracker() -> Response:
    """
    Return HTML containing Tracker table
    :return: (Response) HTML
    """
    return show_html('tracker.html')


@APP.route('/', methods=['GET'])
def home() -> Response:
    """
    Serves up home page for MLManager
    Director
    :return: (Response) HTML
    """
    return show_html('index.html')


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=5000)
