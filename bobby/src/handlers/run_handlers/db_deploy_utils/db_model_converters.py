"""
Database Deployment Utility Functions
"""
from tempfile import TemporaryDirectory

from mleap.pyspark.spark_support import SimpleSparkSerializer
from shared.models.enums import FileExtensions
from importlib import import_module
from io import BytesIO
from yaml import safe_load

import mlflow

from py4j.java_gateway import java_import


class DatabaseModelConverter:
    """
    Classes for serializing an MLModel to a
    database readable object
    """

    def __init__(self, file_ext: str, java_jvm, df_schema=None):
        """
        :param file_ext: the model file extension
        :param java_jvm: Py4J Java Gateway
        :param df_schema: serialized JSON dataframe schema
        """
        self.java_jvm = java_jvm
        self.df_schema = df_schema

        self.serializer = {
            FileExtensions.h2o: self._serialize_h2o,
            FileExtensions.spark: self._serialize_spark,
            FileExtensions.sklearn: self._serialize_sklearn,
            FileExtensions.keras: self._serialize_keras
        }[file_ext]

        self.raw_model = None

    def retrieve_model(self, path: str):
        """
        Read the raw machine learning
        model from the MLModel zipped in the database
        :param path: local path to MLmodel
        :return: raw model
        """
        # Load the model from MLModel
        loader_module = safe_load(open(f'{path}/MLmodel').read())['flavors']['python_function']['loader_module']
        mlflow_module = loader_module.split('.')[1]  # mlflow.spark --> spark
        import_module(loader_module)

        model_obj = getattr(mlflow, mlflow_module).load_model(path)
        self.raw_model = model_obj
        return model_obj

    def convert_to_bytes(self, path: str = None):
        """
        Convert an MLModel to a database artifact stream
        :param path: [Optional] path to local MLmodel if model has not been retrieved
        :return: serialized artifact stream
        """
        if not self.raw_model:
            self.retrieve_model(path)
        return self.serializer(self.raw_model)

    def _load_into_java_bytearray(self, obj) -> bytearray:
        """
        Load an object into a java bytestream
        :param obj: the object to write
        :return: the java bytearray
        """
        byte_output_stream = self.java_jvm.java.io.ByteArrayOutputStream()
        object_output_stream = self.java_jvm.java.io.ObjectOutputStream(byte_output_stream)
        object_output_stream.write(obj)
        object_output_stream.flush()
        object_output_stream.close()
        return byte_output_stream.toByteArray()

    def _serialize_h2o(self, model):
        """
        Serialize H2O model to bytearray
        :param model: model to serialize
        :return: bytearray
        """
        java_import(self.java_jvm, "java.io.{BinaryOutputStream, ObjectOutputStream, ByteArrayInputStream}")
        java_import(self.java_jvm, "hex.genmodel.easy.EasyPredictModelWrapper")
        java_import(self.java_jvm, "hex.genmodel.MojoModel")
        with TemporaryDirectory() as tmpdir:
            model_path = model.download_mojo(f'/{tmpdir}/h2o_model.zip')
            raw_mojo = self.java_jvm.MojoModel.load(model_path)
            java_mojo_config = self.java_jvm.EasyPredictModelWrapper.Config().setModel(raw_mojo)
            java_mojo = self.java_jvm.EasyPredictModelWrapper(java_mojo_config)
            return java_mojo, raw_mojo

    def _serialize_sklearn(self, model):
        """
        Serialize a Scikit model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
        from cloudpickle import dumps as save_cloudpickle
        return save_cloudpickle(model)

    def _serialize_keras(self, model):
        """
        Serialize a Keras model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
        from tensorflow.keras.models import save_model
        h5_buffer = BytesIO()
        save_model(model=model, filepath=h5_buffer)
        h5_buffer.seek(0)
        return h5_buffer.read()

    def _convert_spark_to_mleap(self, model):
        """
        Convert a Spark Model to the Mleap format
        for fast prediction in the database
        :param model: PipelineModel or SparkModel
        :return: MLeap model
        """
        SimpleSparkSerializer()
        with TemporaryDirectory() as tmpdir:
            model.serialize_

    def _serialize_spark(self, model):
        """
        Serialize a Spark model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
