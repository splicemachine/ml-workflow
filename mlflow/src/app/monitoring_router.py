from shared.api.decorators import managed_transaction
from shared.db.connection import SQLAlchemyClient
from fastapi import APIRouter, Depends, status
from .schemas import GetDeploymentsResponse, GetLogsResponse
from sqlalchemy.orm import Session
from shared.db.sql import SQL
from .utils.monitoring_utils import get_logs

MONITORING_ROUTER = APIRouter()
DB_SESSION = Depends(SQLAlchemyClient.get_session)


# TODO: remove transactions from get routes that aren't altering

@MONITORING_ROUTER.get('/deployments', summary="Get deployments and statuses",
                       response_model=GetDeploymentsResponse, status_code=status.HTTP_200_OK)
@managed_transaction
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
@managed_transaction
async def get_job_logs(job_id: int, db: Session = DB_SESSION):
    """
    Get a list of the logs from the job identified with the specified id. Requires Basic Auth validated against the DB.
    """
    return GetLogsResponse(logs=get_logs(task_id=job_id, session=db))
