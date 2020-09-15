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
    MODEL_NAME = environ['MODEL_NAME']

    @staticmethod
    def write_artifact():
        """
        Write the artifact to EmptyDir Mounted Volume
        """
        cnxn = splice_connect(UID=Retriever.DB_USER, PWD=Retriever.DB_PASSWORD,
                              URL=Retriever.DB_HOST, SSL=None)
        with cnxn.cursor() as cursor:
            cursor.execute(f'SELECT "binary" from MLMANAGER.ARTIFACTS WHERE RUN_UUID=\'{Retriever.RUN_ID}\''
                           f'AND NAME=\'{Retriever.MODEL_NAME}\'')
            binary = list(cursor.fetchone())[0]
            # Write to BytesIO buffer
            buffer = BytesIO(binary)
            buffer.seek(0)
            ZipFile(buffer).extractall(path=Retriever.MOUNT_PATH)
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
