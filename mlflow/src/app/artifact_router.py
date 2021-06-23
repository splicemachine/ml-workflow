"""
API Router for managing MLFlow Artifacts (CRUD)
"""
from io import BytesIO
from fastapi import APIRouter, status, UploadFile, Depends
from starlette.responses import StreamingResponse

from .schemas import GenericSuccessResponse, LogArtifactRequest, DownloadArtifactRequest
from .utils.artifact_utils import insert_artifact, download_artifact
from werkzeug.utils import secure_filename
from sqlalchemy.orm import Session

from shared.api.decorators import managed_transaction
from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger

ARTIFACT_ROUTER = APIRouter()

DB_SESSION = Depends(SQLAlchemyClient.get_session)


@ARTIFACT_ROUTER.post('/upload-artifact', summary="Log an artifact to MLFow via HTTP",
                      response_model=GenericSuccessResponse, status_code=status.HTTP_202_ACCEPTED)
@managed_transaction
async def log_artifact(artifact: UploadFile, properties: LogArtifactRequest, db: Session = DB_SESSION):
    """
    Log a binary artifact to the specified run id. It uploads the artifact over HTTP in multipart form data.
    Requires basic auth that is validated against the Splice Machine database.
    """
    file_data = await artifact.read()
    file_name = secure_filename(artifact.filename)

    name = properties.name or file_name

    insert_artifact(
        run_id=properties.run_id,
        file=file_data,
        name=name,
        file_extension=properties.file_extension,
        artifact_path=properties.artifact_path,
        session=db
    )
    return GenericSuccessResponse()


@ARTIFACT_ROUTER.get('/download-artifact', summary="Download an artifact from MLFlow via HTTP",
                     status_code=status.HTTP_200_OK)
@managed_transaction
async def download_artifact(properties: DownloadArtifactRequest, db: Session = DB_SESSION):
    """
    Download a binary artifact from the Splice Machine database with the specified name and run id.
    It returns an octet stream of binary data. Basic auth is required (validated against Splice Machine DB)
    """
    logger.info(f"Downloading artifact {properties.name} on run {properties.run_id}")
    file, name = download_artifact(
        run_id=properties.run_id,
        name=properties.name,
        session=db
    )
    return StreamingResponse(
        BytesIO(file),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": "attachment;filename=" + name,
        }
    )
