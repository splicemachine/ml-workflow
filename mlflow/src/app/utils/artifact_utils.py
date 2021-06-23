import os

from fastapi import HTTPException, status
from shared.models.mlflow_models import SqlArtifact
from sys import getsizeof


def insert_artifact(run_id: str, file: bytes, name: str, file_extension: str, session, artifact_path: str = None):
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


def download_artifact(run_id: str, name: str, session):
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
