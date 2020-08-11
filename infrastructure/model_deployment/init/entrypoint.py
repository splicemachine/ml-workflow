"""
Retrieve Model from the database
"""
from io import BytesIO
from os import environ
from os.path import exists
from zipfile import ZipFile

from pyspark.sql import SparkSession

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
    MLFLOW_URL = environ.get('MLFLOW_URL', 'splicedb-mlflow:5001')

    RUN_ID = environ['RUN_ID']
    MODEL_NAME = environ['MODEL_NAME']

    SPARK_SESSION = SparkSession.builder.getOrCreate()

    @staticmethod
    def write_artifact():
        cnxn = splice_connect(UID=Retriever.DB_USER, PWD=Retriever.DB_PASSWORD,
                              URL=Retriever.DB_HOST)
        with cnxn.cursor() as cursor:
            cursor.execute(f'SELECT "binary", file_ext from MLMANAGER.ARTIFACTS WHERE RUN_UUID={Retriever.RUN_ID}'
                           f'AND MODEL_')
            binary, file_ext = cursor.fetchone()
            # Write to BytesIO buffer
            buffer = BytesIO(binary)
            buffer.seek(0)
            ZipFile(buffer).extractall(path=Retriever.MOUNT_PATH)


def main():
    """
    Main logic of entrypoint
    """
    if exists(Retriever.MOUNT_PATH):
        print("Model has already been loaded... Exiting.")
    else:
        print("Retrieving model as it has not been loaded")
        Retriever.write_artifact()


if __name__ == "__main__":
    main()
