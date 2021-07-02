"""
API Router for managing MLFlow Artifacts (CRUD)
"""
import os
from io import BytesIO
from sys import getsizeof

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Form, status
from schemas import (DownloadArtifactRequest, GenericSuccessResponse,
                     LogArtifactRequest)
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse
from werkzeug.utils import secure_filename

from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger
from shared.models.mlflow_models import SqlArtifact

ARTIFACT_ROUTER = APIRouter()

DB_SESSION = Depends(SQLAlchemyClient.get_session)


def _insert_artifact(run_id: str, file: bytes, name: str, file_extension: str, session, artifact_path: str = None):
    """
    Inserts an artifact into the database as a SqlArtifact. Used as an intermediary step for the Splice Artifact Store

    :param run_id: The run id
    :param file: The file object
    :param name: The name of the file
    :param file_extension: File extension
    :param session: the SQLAlchemy managed session
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
        session.add(a)
        session.commit()
    except Exception as e:
        session.rollback()
        raise HTTPException(
            detail=f'Failure while trying to upload file: {str(e)}',
            status_code=status.HTTP_400_BAD_REQUEST,  # bad request
        )


def _download_artifact(run_id: str, name: str, session):
    """
    Downloads an artifact from the database to disk

    :param run_id: Run ID of artifact
    :param name: Name of the artifact
    :param session: the session
    :return: File name of the downloaded artifact
    """
    artifact = session.query(SqlArtifact) \
        .filter_by(name=name) \
        .filter_by(run_uuid=run_id).first()
    if not artifact:
        raise HTTPException(detail=f"File {name} from run {run_id} not found",
                            status_code=status.HTTP_404_NOT_FOUND)
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


@ARTIFACT_ROUTER.post('/upload-artifact', operation_id='upload_artifact', summary="Log an artifact to MLFow via HTTP",
                      response_model=GenericSuccessResponse, status_code=status.HTTP_202_ACCEPTED)
async def log_artifact(artifact: UploadFile = File(...), name: str = Form(default=None),
                       run_id: str = Form(...), file_extension: str = Form(default=None),
                       artifact_path: str = Form(default=None), db: Session = DB_SESSION):
    """
    Log a binary artifact to the specified run id. It uploads the artifact over HTTP in multipart form data.
    Requires basic auth that is validated against the Splice Machine database.
    """
    file_data = await artifact.read()
    file_name = secure_filename(artifact.filename)

    name = name or file_name

    _insert_artifact(
        run_id=run_id,
        file=file_data,
        name=name,
        file_extension=file_extension,
        artifact_path=artifact_path,
        session=db
    )
    return GenericSuccessResponse()


@ARTIFACT_ROUTER.get('/download-artifact', operation_id='download_artifact',
                     summary="Download an artifact from MLFlow via HTTP",
                     status_code=status.HTTP_200_OK, response_class=StreamingResponse)
def download_artifact(run_id: str, name: str, db: Session = DB_SESSION):
    """
    Download a binary artifact from the Splice Machine database with the specified name and run id.
    It returns an octet stream of binary data. Basic auth is required (validated against Splice Machine DB)
    """
    logger.info(f"Downloading artifact {name} on run {run_id}")
    file, name = _download_artifact(
        run_id=run_id,
        name=name,
        session=db
    )
    return StreamingResponse(
        BytesIO(file),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": "attachment;filename=" + name,
        }
    )
