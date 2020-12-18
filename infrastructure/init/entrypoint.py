"""
Retrieve Model from the database
"""
from io import BytesIO
from os import environ, system
from os.path import exists
from zipfile import ZipFile

from splicemachinesa.pyodbc import splice_connect


class Retriever:
    """
    Retrieve a model from the database
    and persist to the shared volume
    """
    MOUNT_PATH = environ.get("MOUNT_PATH", "/var/run/model")
    MODE = environ.get('MODE', 'production')
    DB_USER = environ['DB_USER']
    DB_PASSWORD = environ['DB_PASSWORD']
    DB_HOST = environ['DB_HOST']

    RUN_ID = environ['RUN_ID']
    MODEL_NAME = environ.get('MODEL_NAME')
    RETRAINING = environ.get('RETRAINING')
    CONDA_ENV_NAME = environ.get('CONDA_ENV_NAME')


    @staticmethod
    def _get_and_write_binary(cnxn, name, is_zip):
        """
        Gets the binary from the database and Writes a binary to disk in the mount path
        :param cnxn: The database connection
        :param name: The binary object name
        :param is_zip: Whether this is a zip file or not
        """
        with cnxn.cursor() as cursor:
            cursor.execute(f'SELECT "binary" from MLMANAGER.ARTIFACTS WHERE RUN_UUID=\'{Retriever.RUN_ID}\''
                           f'AND NAME=\'{name}\'')
        b = list(cursor.fetchone())[0]

        if is_zip:
            # Write to BytesIO buffer
            buffer = BytesIO(b)
            buffer.seek(0)
            ZipFile(buffer).extractall(path=Retriever.MOUNT_PATH)
        else:
            with open(f'{Retriever.MOUNT_PATH}/{name}', 'wb') as f:
                f.write(b)


    @staticmethod
    def write_artifact():
        """
        Write the retraining artifact to EmptyDir Mounted Volume (for model retraining)
        """
        cnxn = splice_connect(UID=Retriever.DB_USER, PWD=Retriever.DB_PASSWORD,
                              URL=Retriever.DB_HOST, SSL=None)

        is_zip = not Retriever.RETRAINING # Zip file if it's a model deployment because we save the model as zip
        files = ['retrainer.pkl', Retriever.CONDA_ENV_NAME] if Retriever.RETRAINING else [Retriever.MODEL_NAME]
        for file_name in files:
            Retriever._get_and_write_binary(cnxn, file_name, is_zip)

        print("Mounted Volume Contents:")
        system(f"ls {Retriever.MOUNT_PATH}")

def main():
    """
    Main logic of entrypoint
    """
    system(f"rm -rf {Retriever.MOUNT_PATH}/*")
    Retriever.write_artifact()


if __name__ == "__main__":
    main()
