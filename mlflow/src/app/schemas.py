from typing import Dict, List
from pydantic import BaseModel


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
