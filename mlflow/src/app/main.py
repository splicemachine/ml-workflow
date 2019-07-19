from time import time as timestamp
from json import dumps as serialize_json
from flask import Flask, request, Response, jsonify as create_json, render_template as show_html

from mlmanager.logger.logging_config import logging
from mlmanager_lib.rest.http_utils import HTTP, APIStatuses, Authentication
from mlmanager_lib.database.models import SessionFactory, Job, Handler
from mlmanager_lib.database.constants import Handlers

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commerical"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

app: Flask = Flask(__name__)

Session = SessionFactory()  # Thread-Local Database Session for Flask Thread

LOGGER = logging.getLogger(__name__)


@app.route('/service/<str:handler_name>', method=['POST'])
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
    handler: Handler = Handlers.MAPPING.get(handler_name.upper())
    if not handler:
        message: str = f"Handler {handler} is an unknown service"
        LOGGER.error(message)
        return HTTP.responses['malformed'](
            create_json(status=APIStatuses.failure,
                        message=message)
        )
    # Queue Job
    payload: dict = {}
    for required_key in handler.required_payload_args:
        payload[required_key] = request.json[required_key]

    for optional_key in handler.optional_payload_args:
        supplied_value: object = request.json.get(optional_key)
        payload[optional_key] = supplied_value if supplied_value else handler.optional_payload_args[
            optional_key]

    current_time: int = int(timestamp())
    job: Job = Job(timestamp=current_time, handler=handler, payload=serialize_json(payload))

    Session.add(job)
    Session.commit()
    return dict(job_status=APIStatuses.pending,
                timestamp=current_time)  # turned into JSON and returned


@app.route('/', methods=['GET'])
@app.route('/tracker', methods=['GET'])
@Authentication.login_required
def job_tracker_gui() -> Response:
    """
    Serves up Job Tracker GUI
    where users can view existing jobs and
    queue new ones.
    :return:
    """
    return show_html('dash.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
