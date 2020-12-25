"""
Retrieve Model from the database
"""
import logging
from io import BytesIO
from os import environ, system
from sys import stderr
from zipfile import ZipFile

from splicemachinesa.pyodbc import splice_connect


def build_logger():
    logger = logging.getLogger(__name__)
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter('%(levelname)s %(asctime)s - %(message)s')
    logger.addHandler(console_handler)
    return logger


logger = build_logger()


class Retriever:
    """
    Retrieve a model from the database
    and persist to the shared volume
    """

    def __init__(self):
        self.mount_path = environ.get("MOUNT_PATH", "/var/run/model")
        self.mode = environ.get('MODE', 'production')
        self.db_user = environ['DB_USER']
        self.db_password = environ['DB_PASSWORD']
        self.db_host = environ['DB_HOST']

        self.run_id = environ['RUN_ID']
        self.model_name = environ.get('MODEL_NAME')
        self.retraining = environ.get('RETRAINING')
        self.conda_env_name = environ.get('CONDA_ENV_NAME')

    def _get_and_write_binary(self, cnxn, name, is_zip):
        """
        Gets the binary from the database and Writes a binary to disk in the mount path
        :param cnxn: The database connection
        :param name: The binary object name
        :param is_zip: Whether this is a zip file or not
        """
        logger.info(f"Reading Artifact {name} from Store")

        with cnxn.cursor() as cursor:
            cursor.execute(f'SELECT "binary" from MLMANAGER.ARTIFACTS WHERE RUN_UUID=\'{self.run_id}\''
                           f'AND NAME=\'{name}\'')
        b = list(cursor.fetchone())[0]

        if is_zip:
            logger.info(f"Decompressing Zipped Artifact")
            # Write to BytesIO buffer
            buffer = BytesIO(b)
            buffer.seek(0)
            ZipFile(buffer).extractall(path=self.mount_path)
        else:
            with open(f'{self.mount_path}/{name}', 'wb') as f:
                f.write(b)

    def write_artifact(self):
        """
        Write the retraining artifact to EmptyDir Mounted Volume (for model retraining)
        """
        logger.info("Connecting to Splice Machine Database")
        cnxn = splice_connect(UID=self.db_user, PWD=self.db_password,
                              URL=self.db_host, SSL=None)

        is_zip = not self.retraining  # Zip file if it's a model deployment because we save the model as zip
        files = ['retrainer.pkl', self.conda_env_name] if self.retraining else [self.model_name]
        for file_name in files:
            logger.info(f"Retrieving Artifact {file_name} from Artifacts Table")
            self._get_and_write_binary(cnxn, file_name, is_zip)

        logger.info("Mounted Volume Contents:")


def main():
    """
    Main logic of entrypoint
    """
    try:
        retriever = Retriever()
        system(f"rm -rf {retriever.mount_path}/*")
        retriever.write_artifact()
    except Exception as e:
        # Logger message is polled by job watcher to see whether or not jobs are completed in the UI
        print("LOADER_CONTAINER_FAILED", file=stderr)
        raise e
    finally:
        print("COMPLETED_LOADER_CONTAINER")


if __name__ == "__main__":
    main()
