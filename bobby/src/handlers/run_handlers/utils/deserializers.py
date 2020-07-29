from io import BytesIO
from pickle import loads as load_pickle_string
from tempfile import NamedTemporaryFile

from h2o import load_model as load_h2o_model
from h5py import File as h5_file
from pyspark.ml import PipelineModel
from tensorflow.keras.models import (Model as KerasModel,
                                     load_model as load_keras_model)

from mlflow import (h2o as mlflow_h2o, keras as mlflow_keras,
                    sklearn as mlflow_sklearn, spark as mlflow_spark)

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


class Deserializers:
    """
    Deserialize Models from their binary streams to be on the disk
    """

    @staticmethod
    def spark(jvm, artifact_stream: bytearray, download_path: str, conda_env: str):
        """
        Deserialize a spark artifact
        :param artifact_stream: artifact stream byte array
        :param download_path: local path on disk
        :param conda_env: conda environments
        :param jvm: py4j jvm
=        """
        binary_input_stream = jvm.java.io.ByteArrayInputStream(artifact_stream)
        object_input_stream = jvm.java.io.ObjectInputStream(binary_input_stream)

        deserialized_pipeline: PipelineModel = PipelineModel._from_java(object_input_stream.readObject())
        mlflow_spark.save_model(deserialized_pipeline, download_path,
                                conda_env=conda_env)

    @staticmethod
    def h2o(artifact_stream: bytearray, download_path: str, conda_env: str):
        """
        Deserialize a h2o artifact
        :param artifact_stream: artifact stream byte array
        :param download_path: local path on disk
        :param conda_env: conda environments
        """

        with NamedTemporaryFile() as tmp:
            tmp.write(artifact_stream)
            tmp.seek(0)
            model: load_h2o_model = load_h2o_model(tmp.name)

        mlflow_h2o.save_model(model, download_path,
                              conda_env=conda_env)

    @staticmethod
    def keras(artifact_stream: bytearray, download_path: str, conda_env: str):
        """
        Deserialize a keras artifact
        :param artifact_stream: artifact stream byte array
        :param download_path: local path on disk
        :param conda_env: conda environments
        """
        hfile = h5_file(BytesIO(artifact_stream), 'r')
        model: KerasModel = load_keras_model(hfile)

        mlflow_keras.save_model(model, download_path,
                                conda_env=conda_env)

    @staticmethod
    def sklearn(artifact_stream: bytearray, download_path: str, conda_env: str):
        """
        Deserialize a sklearn artifact
        :param artifact_stream: artifact stream byte array
        :param download_path: local path on disk
        :param conda_env: conda environments
        """
        sklearn_model = load_pickle_string(artifact_stream)
        mlflow_sklearn.save_model(sklearn_model, download_path,
                                  conda_env=conda_env)
