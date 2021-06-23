from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from shared.db.connection import SQLAlchemyClient
from shared.db.sql import SQL
from shared.models.splice_models import Job
from sqlalchemy.orm import load_only
from mlflow.src.app.schemas import GetDeploymentsResponse, GetLogsResponse

MONITORING_ROUTER = APIRouter()
DB_SESSION = Depends(SQLAlchemyClient.get_session)


def _get_logs(task_id: int, session):
    """
    Retrieve the logs for a given task id
    :param task_id: the celery task id to get the logs for
    """
    job = session.query(Job).options(load_only("logs")).filter_by(id=task_id).one()
    return job.logs.split('\n')


@MONITORING_ROUTER.get('/deployments', summary="Get deployments and statuses",
                       response_model=GetDeploymentsResponse, status_code=status.HTTP_200_OK)
async def get_deployments(db: Session = DB_SESSION):
    """
    Get a list of all deployed models and their associated statuses. Requires Basic Auth validated
    against the database
    """
    response = db.execute(SQL.get_deployment_status)
    cols = [col[0] for col in response.cursor.description]
    rows = response.fetchall()
    return GetDeploymentsResponse(deployments=dict([(col, [row[i] for row in rows]) for i, col in enumerate(cols)]))


@MONITORING_ROUTER.get('/job-logs/{job_id}', summary="Get the logs for a specified job", response_model=GetLogsResponse,
                       status_code=status.HTTP_200_OK)
async def get_logs(job_id: int, db: Session = DB_SESSION):
    """
    Get a list of the logs from the job identified with the specified id. Requires Basic Auth validated against the DB.
    """
    return GetLogsResponse(logs=_get_logs(task_id=job_id, session=db))
