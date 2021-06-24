from fastapi import APIRouter, Depends, status
from schemas import GetDeploymentsResponse, GetLogsResponse
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.exc import NoResultFound

from shared.db.connection import SQLAlchemyClient
from shared.db.sql import SQL
from shared.models.splice_models import Job
from shared.logger.logging_config import logger
from shared.api.exceptions import SpliceMachineException, ExceptionCodes

MONITORING_ROUTER = APIRouter()
DB_SESSION = Depends(SQLAlchemyClient.get_session)


def _get_logs(task_id: int, session):
    """
    Retrieve the logs for a given task id
    :param task_id: the celery task id to get the logs for
    """
    try:
        job = session.query(Job).options(load_only("logs")).filter_by(id=task_id).one()
        return job.logs.split('\n')
    except NoResultFound:
        logger.warning("Couldn't find the specified job in the jobs table")
        raise SpliceMachineException(status_code=status.HTTP_404_NOT_FOUND, message="Couldn't find the specified job.",
                                     code=ExceptionCodes.DOES_NOT_EXIST)


@MONITORING_ROUTER.get('/deployments', summary="Get deployments and statuses", operation_id='get_deployments',
                       response_model=GetDeploymentsResponse, status_code=status.HTTP_200_OK)
def get_deployments(db: Session = DB_SESSION):
    """
    Get a list of all deployed models and their associated statuses. Requires Basic Auth validated
    against the database
    """
    response = db.execute(SQL.get_deployment_status)
    cols = [col[0] for col in response.cursor.description]
    rows = response.fetchall()
    return GetDeploymentsResponse(deployments=dict([(col, [row[i] for row in rows]) for i, col in enumerate(cols)]))


@MONITORING_ROUTER.get('/job-logs/{job_id}', summary="Get the logs for a specified job", response_model=GetLogsResponse,
                       status_code=status.HTTP_200_OK, operation_id='get_logs')
def get_logs(job_id: int, db: Session = DB_SESSION):
    """
    Get a list of the logs from the job identified with the specified id. Requires Basic Auth validated against the DB.
    """
    return GetLogsResponse(logs=_get_logs(task_id=job_id, session=db))
