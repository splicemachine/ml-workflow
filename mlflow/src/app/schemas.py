from typing import Dict, List
from time import time as timestamp
from shared.api.models import APIStatuses
from pydantic import BaseModel, Field


class GenericSuccessResponse(BaseModel):
    msg: str = "Success."


class LogArtifactRequest(BaseModel):
    """
    Payload (form data) for artifact logging request
    """
    run_id: str
    file_extension: str
    artifact_path: str = None
    name: str = None


class DownloadArtifactRequest(BaseModel):
    """
    Request properties to download an artifact from DB
    """
    run_id: str
    name: str


class GetDeploymentsResponse(BaseModel):
    """
    Response  for getting deployments
    """
    deployments: Dict[str, str]


class GetLogsResponse(BaseModel):
    """
    Response for getting the logs of a queued job
    """
    logs: List[str]


class InitiateJobRequest(BaseModel):
    """
    Request for initiating a handler job
    """
    handler_name: str
    job_payload: dict


class InitiateJobResponse(BaseModel):
    """
    Response for initiating a handler job
    """
    job_status: str = APIStatuses.pending
    job_id: int
    timestamp: int = Field(default_factory=timestamp)
