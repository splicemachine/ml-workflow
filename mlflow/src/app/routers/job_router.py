import json
import os
from time import time as timestamp

import requests
from fastapi import APIRouter, Depends, status
from schemas import InitiateJobRequest, InitiateJobResponse
from sqlalchemy.orm import Session

from shared.db.connection import SQLAlchemyClient
from shared.api.auth_dependency import authenticate
from shared.api.models import APIStatuses, AuthUser
from shared.logger.logging_config import logger
from shared.models.splice_models import Handler, Job
from shared.services.handlers import HandlerNames, KnownHandlers
from shared.api.exceptions import ExceptionCodes, SpliceMachineException

JOB_ROUTER = APIRouter()
DB_SESSION = Depends(SQLAlchemyClient.get_session)

BOBBY_URI = os.environ.get('BOBBY_URL', 'http://bobby')


def _initiate_job(job_payload: dict, handler: Handler, user: str, session):
    """
    Handler for actions that execute services
    e.g. deploy, retrain.
    :param job_payload: (dict) payload to parse to create job
    :param handler: (Handler) the handler object
    :param user: (str) Username of the person who submitted the job
    :param session: SQLAlchemy ORM Session
    :return: (Response) JSON payload for success
    """
    # Format Payload
    payload: dict = {field.name: field.get_value(job_payload.get(field.name) or None) for field in
                     handler.payload_args if field.name != 'payload'}

    job: Job = Job(handler_name=handler.name,
                   user=user,
                   payload=json.dumps(payload))

    session.add(job)
    session.flush()
    session.merge(job)  # get identity column from database with Job ID

    try:
        # Tell bobby there's a new job to process
        requests.post(f"{BOBBY_URI}:2375/job")
    except ConnectionError:
        logger.warning('Bobby was not reachable by MLFlow. Ensure Bobby is running. \nThe job has'
                       'been added to the database and will be processed when Bobby is running again.')
    return dict(job_status=APIStatuses.pending,
                job_id=job.id,
                timestamp=timestamp())  # turned into JSON and returned


@JOB_ROUTER.post('/initiate-job', summary='Initiate an asynchronous job', operation_id='initiate_job',
                 response_model=InitiateJobResponse, status_code=status.HTTP_202_ACCEPTED)
def initiate_job(properties: InitiateJobRequest, user: AuthUser = Depends(authenticate), db: Session = DB_SESSION):
    """
    Initiate an asynchronous job in bobby. Requires a request payload and basic auth credentials.
    """
    handler: Handler = KnownHandlers.MAPPING.get(properties.handler_name.upper())
    if not handler:
        message: str = f"Handler {properties.handler_name} is an unknown service"
        logger.error(message)
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, message=message,
                                     code=ExceptionCodes.BAD_ARGUMENTS)

    return _initiate_job(job_payload=properties.job_payload, handler=handler, user=user.username, session=db)
