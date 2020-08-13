"""
Database Deployment Utility Functions
"""
from tempfile import TemporaryDirectory

from shared.models.enums import FileExtensions
from shared.models.model_types import Representations, Metadata
from importlib import import_module
from io import BytesIO
from yaml import safe_load

import mlflow

from py4j.java_gateway import java_import
from bobby.src.handlers.run_handlers.db_deploy_utils.entities.db_model import Model


class DatabaseModelConverter:
    """
    Classes for serializing an MLModel to a
    database readable object
    """

    def __init__(self, file_ext: str, java_jvm=None, df_schema=None):
        """
        :param file_ext: the model file extension
        :param java_jvm: Py4J Java Gateway for serializing objects to byte streams
        :param df_schema: serialized JSON dataframe schema
        """
        self.java_jvm = java_jvm
        self.df_schema = df_schema

        self.representation_generator = {
            FileExtensions.h2o: self._create_alternate_h2o,
            FileExtensions.spark: self._create_alternate_spark,
            FileExtensions.sklearn: self._create_alternate_sklearn,
            FileExtensions.keras: self._create_alternate_keras
        }[file_ext]

        self.model: Model = Model()
        self.model.add_metadata(Metadata.FILE_EXT, file_ext)

    def create_raw_representations(self, *, from_dir: str):
        """
        Read the raw machine learning
        model from the MLModel zipped in the database
        :param from_dir: local path to MLmodel
        :return: raw model
        """
        # Load the model from MLModel
        with open(f'{from_dir}/MLmodel') as ml_model:
            loader_module = safe_load(ml_model.read())['flavors']['python_function']['loader_module']

        mlflow_module = loader_module.split('.')[1]  # mlflow.spark --> spark
        import_module(loader_module)  # import the specified mlflow retriever module

        model_obj = getattr(mlflow, mlflow_module).load_model(from_dir)

        self.model.add_representation(name=Representations.LIBRARY, representation=model_obj)
        return model_obj

    def create_alternate_representations(self):
        """
        Create alternate representations of the machine learning
        model, including serialized and library specific ones
        """
        if not self.model.get_representation(name='library'):
            raise Exception("Raw model must be retrieved before alternate representations can be generated")
        self.representation_generator()

    def _load_into_java_bytearray(self, obj) -> bytearray:
        """
        Load an object into a java bytestream
        :param obj: the object to write
        :return: the java bytearray
        """
        if not self.java_jvm:
            raise Exception("Model Serialization requires a JVM, but none was specified in the constructor")

        byte_output_stream = self.java_jvm.java.io.ByteArrayOutputStream()
        object_output_stream = self.java_jvm.java.io.ObjectOutputStream(byte_output_stream)
        object_output_stream.write(obj)
        object_output_stream.flush()
        object_output_stream.close()
        return byte_output_stream.toByteArray()

    def _create_alternate_h2o(self, model):
        """
        Serialize H2O model to bytearray
        :param model: model to serialize
        :return: bytearray
        """
        java_import(self.java_jvm, "java.io.{BinaryOutputStream, ObjectOutputStream, ByteArrayInputStream}")
        java_import(self.java_jvm, "hex.genmodel.easy.EasyPredictModelWrapper")
        java_import(self.java_jvm, "hex.genmodel.MojoModel")
        with TemporaryDirectory() as tmpdir:
            model_path = self.model.get_representation('library').download_mojo(f'/{tmpdir}/h2o_model.zip')
            raw_mojo = self.java_jvm.MojoModel.load(model_path)
            java_mojo_config = self.java_jvm.EasyPredictModelWrapper.Config().setModel(raw_mojo)
            java_mojo = self.java_jvm.EasyPredictModelWrapper(java_mojo_config)

            # Register H2O Model Representations
            self.model.add_representation(Representations.JAVA_MOJO, java_mojo)
            self.model.add_representation(Representations.RAW_MOJO, raw_mojo)
            self.model.add_representation(Representations.BYTES, self._load_into_java_bytearray(java_mojo))

    def _create_alternate_sklearn(self, model):
        """
        Serialize a Scikit model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
        from cloudpickle import dumps as save_cloudpickle
        self.model.add_representation(Representations.BYTES, save_cloudpickle(model))

    def _create_alternate_keras(self, model):
        """
        Serialize a Keras model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
        from tensorflow.keras.models import save_model
        h5_buffer = BytesIO()
        save_model(model=model, filepath=h5_buffer)
        h5_buffer.seek(0)
        self.model.add_representation(Representations.BYTES, h5_buffer.read())

    def _create_alternate_spark(self, model):
        """
        Serialize a Spark model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
        pass
