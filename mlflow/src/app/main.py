from collections import defaultdict
from json import dumps as serialize_json
import os
from io import BytesIO
from os import environ as env_vars
from sys import getsizeof
from time import time as timestamp
from tempfile import TemporaryDirectory

import requests
from flask import (request, url_for, render_template as show_html, redirect,
                   jsonify as create_json, Flask, Response, send_file)
from flask_executor import Executor
from flask_login import (LoginManager, current_user, login_required,
                         login_user, logout_user)
from werkzeug.utils import secure_filename
from sqlalchemy.orm import load_only

from shared.api.models import APIStatuses
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.api.responses import HTTP
from shared.environments.cloud_environment import (CloudEnvironment,
                                                   CloudEnvironments)
from shared.logger.logging_config import logger
from shared.models.splice_models import Handler, Job
from shared.models.mlflow_models import SqlArtifact
from shared.services.authentication import Authentication, User
from shared.services.database import SQLAlchemyClient
from shared.services.handlers import HandlerNames, KnownHandlers

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

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

# Temporary folder for artifacts before moving them into the database
UPLOAD_FOLDER = '/tmp/artifacts'
# 250 MB maximum
MAX_SIZE = 250 * 1000 * 1000
APP.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
APP.config['MAX_CONTENT_LENGTH'] = MAX_SIZE
if not os.path.exists(UPLOAD_FOLDER):
    os.system(f'mkdir -p {UPLOAD_FOLDER}')

@APP.errorhandler(SpliceMachineException)
def handle_invalid_usage(error):
    return (error.message, error.status_code)


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
        message: str = f"Handler {request.json['handler_name']} is an unknown service"
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
    payload: dict = {field.name: field.get_value(request_payload.get(field.name) or None) for field in handler.payload_args
                     if field.name != 'payload'}

    job: Job = Job(handler_name=handler.name,
                   user=user,
                   payload=serialize_json(payload))

    Session.add(job)
    Session.commit()
    Session.merge(job) # get identity col

    try:
        # Tell bobby there's a new job to process
        requests.post(f"{BOBBY_URI}:2375/job")
    except ConnectionError:
        logger.warning('Bobby was not reachable by MLFlow. Ensure Bobby is running. \nThe job has'
                       'been added to the database and will be processed when Bobby is running again.')
    return dict(job_status=APIStatuses.pending,
                job_id=job.id,
                timestamp=timestamp())  # turned into JSON and returned

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

########################################################################################################
#                               Splice Machine Artifact Store                                          #
# A HTTP endpoint for uploading artifacts                                                              #
# This will accept artifacts up to 250MB, move them into the Splice DB MLManager.Aritfact table        #
# And delete the file from disk                                                                        #
# Authorization and Authentication included                                                            #
########################################################################################################
def insert_artifact(run_id, file, name, file_extension, artifact_path = None):
    """
    Inserts an artifact into the database as a SqlArtifact. Used as an intermediary step for the Splice Artifact Store

    :param run_id: The run id
    :param file: The file object
    :param name: The name of the file
    :param size: Size of the file
    :param file_extension: File extension
    :param artifact_path: If the file is being stored in a directory, this is that directory path
    """
    a = SqlArtifact(
            run_uuid=run_id,
            name=name,
            size=getsizeof(file),
            binary=file,
            file_extension=file_extension,
            artifact_path=artifact_path
        )
    try:
        Session.add(a)
        Session.commit()
    except Exception as e:
        Session.rollback()
        raise SpliceMachineException(
            message=f'Failure while trying to upload file: {str(e)}',
            status_code=400, # bad request
            code=ExceptionCodes.UNKNOWN
        )


@APP.route('/api/rest/upload-artifact', methods=['POST'])
@Authentication.basic_auth_required
def log_artifact() -> str:
    """
    Acts as an endpoint for users to upload artifacts. This then uploads the artifacts to the Splice DB in the
    artifacts table
    """
    # If the user does not select a file, the browser submits an
    # empty file without a filename.
    if 'file' not in request.files or request.files['file'].filename == '':
        raise SpliceMachineException(message="No file selected", status_code=404, code=ExceptionCodes.DOES_NOT_EXIST)
    file = request.files['file']
    filename = secure_filename(file.filename)
    payload = request.form

    name = payload.get('name') or filename

    insert_artifact(
        payload['run_id'],
        file.read(),
        name,
        payload['file_extension'],
        payload.get('artifact_path')
    )
    return "File Uploaded."

def _download_artifact(run_id, name):
    """
    Downloads an artifact from the database to disk

    :param run_id: Run ID of artifact
    :param name: Name of the artifact
    :return: File name of the downloaded artifact
    """
    artifact = Session.query(SqlArtifact) \
        .filter_by(name=name) \
        .filter_by(run_uuid=run_id).first()
    if not artifact:
        raise SpliceMachineException(message=f"File {name} from run {run_id} not found",
                                     status_code=404, code=ExceptionCodes.DOES_NOT_EXIST)
    # Files name may already have the extension
    if os.path.splitext(artifact.name)[1]:
        filename = artifact.name
    # If there is an artifact path, this is a zip file. In this case, override the file extension
    # and set it to .zip because that is how the file was stored
    elif artifact.artifact_path:
        filename = f'{artifact.name}.zip'
    else:
        filename = f'{artifact.name}.{artifact.file_extension}'

    # with open(file_path, 'wb') as file:
    #     file.write(artifact.binary)
    # return os.path.split(file_path)
    return artifact.binary, filename

@APP.route('/api/rest/download-artifact', methods=['GET'])
@Authentication.basic_auth_required
def download_artifact() -> str:
    """
    Returns requested artifacts.
    """
    if not 'run_id' in request.args and 'name' in request.args:
        raise SpliceMachineException(message=f"Name and Run ID must be provided",
                                     status_code=400, code=ExceptionCodes.BAD_ARGUMENTS)
    file, name = _download_artifact(request.args.get('run_id'), request.args.get('name'))
    # except:
    #     return ('file not found', 404)
    # https://www.iana.org/assignments/media-types/application/octet-stream octet-stream
    return send_file(BytesIO(file), mimetype='application/octet-stream', as_attachment=True, attachment_filename=name)



if CLOUD_ENVIRONMENT.can_deploy:
    APP.add_url_rule(KnownHandlers.get_url(HandlerNames.deploy_csp), 'deploy_csp', view_func=deploy_csp)

if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=5000)
