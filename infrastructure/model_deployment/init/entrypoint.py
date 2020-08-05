"""
Retrieve Model from the database
"""
from os import environ
from os.path import exists

import h2o
from h2o.estimators.estimator_base import ModelBase as H2OModel
from pyspark.ml.base import Model as SparkModel
from pyspark.sql import SparkSession
from sklearn.base import BaseEstimator as ScikitModel
from splicemachine.mlflow_support import *
from splicemachine.spark import ExtPySpliceContext
from tensorflow.keras import Model as KerasModel


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
    KAFKA_URL = environ.get('KAFKA_URL', 'splicedb-kafka:9092')  # should work as long as pod is in the same namespace
    # as kafka broker

    RUN_ID = environ['RUN_ID']
    MODEL_NAME = environ['MODEL_NAME']

    def __init__(self):
        self.spark = None
        self.splice = None

        self.create_contexts()

    def create_contexts(self):
        """
        Create a spark session
        :return: a new spark session
        """
        print("Creating Spark Session...")
        spark = SparkSession.builder \
            .config('spark.jars', f'/opt/init/spark_jars/splice_spark2-{environ["nsds_version"]}.jar') \
            .config('spark.driver.extraClassPath', '/opt/init/kafka_jars/*').getOrCreate()

        self.spark = spark
        print("Creating External PySplice Context...")
        jdbc_url = f'jdbc:splice://{Retriever.DB_HOST}:1527/splicedb;user={Retriever.DB_USER};password=' \
                   f'{Retriever.DB_PASSWORD};'

        jdbc_url += "ssl=basic" if Retriever.MODE == "production" else ""  # add SSL if not running in dev

        self.splice = ExtPySpliceContext(sparkSession=self.spark,
                                         JDBC_URL=jdbc_url,
                                         kafkaServers=Retriever.KAFKA_URL)

        print("Registering model with mlflow...")
        mlflow.register_splice_context(self.splice)

    @staticmethod
    def load_and_save_model():
        """
        Load a model from the database
        :return:
        """
        print("Loading model from database")
        model = mlflow.load_model(run_id=Retriever.RUN_ID, name=Retriever.MODEL_NAME)
        if isinstance(model, SparkModel):
            print("Detected Model Type to be Spark")
            from mlflow.spark import save_model as save_spark_model
            save_spark_model(spark_model=model, path=Retriever.MOUNT_PATH)
        elif isinstance(model, H2OModel):
            print("Detected Model Type to be h2o")
            from mlflow.h2o import save_model as save_h2o_model
            h2o.init()
            save_h2o_model(h2o_model=model, path=Retriever.MOUNT_PATH)
        elif isinstance(model, ScikitModel):
            print("Detected Model Type to be Scikit")
            from mlflow.sklearn import save_model as save_sklearn_model
            save_sklearn_model(sk_model=model, path=Retriever.MOUNT_PATH)
        elif isinstance(model, KerasModel):
            print("Detected Model Type to be Keras.")
            from mlflow.keras import save_model as save_keras_model
            save_keras_model(keras_model=model, path=Retriever.MOUNT_PATH)
        else:
            raise Exception("Cannot read model from database as model type is unsupported")


def main():
    """
    Main logic of entrypoint
    """
    if exists(Retriever.MOUNT_PATH):
        print("Model has already been loaded... Exiting.")
    else:
        print("Retrieving model as it has not been loaded")
        model_retriever = Retriever()
        model_retriever.load_and_save_model()


if __name__ == "__main__":
    main()
