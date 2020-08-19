"""
Database Deployment Utility Functions
"""
from importlib import import_module
from io import BytesIO
from tempfile import TemporaryDirectory

from py4j.java_gateway import java_import
from yaml import safe_load

import mlflow
from shared.logger.logging_config import logger
from shared.models.enums import FileExtensions
from shared.models.model_types import Metadata, Representations

from .entities.db_model import Model


class DatabaseRepresentationCreator:
    """
    Classes for serializing an MLModel to a
    database readable object
    """

    def __init__(self, file_ext: str, java_jvm=None, df_schema=None, logger=logger):
        """
        :param file_ext: the model file extension
        :param java_jvm: Py4J Java Gateway for serializing objects to byte streams
        :param df_schema: serialized JSON dataframe schema
        :param logger: override the default logger
        """
        self.java_jvm = java_jvm
        self.df_schema = df_schema

        self.representation_generator = {
            FileExtensions.h2o: self._create_alternate_h2o,
            FileExtensions.spark: self._create_alternate_spark,
            FileExtensions.sklearn: self._create_alternate_sklearn,
            FileExtensions.keras: self._create_alternate_keras
        }[file_ext]

        self.logger = logger

        self.model: Model = Model()
        self.model.add_metadata(Metadata.FILE_EXT, file_ext)

    def get_library_representation(self, *, from_dir: str):
        """
        Read the raw machine learning
        model from the MLModel zipped in the database
        :param from_dir: local path to MLmodel
        :return: raw model
        """
        # Load the model from MLModel
        with open(f'{from_dir}/MLmodel') as ml_model:
            self.logger.info("Reading MLModel Flavor from Extracted Archive", send_db=True)
            loader_module = safe_load(ml_model.read())['flavors']['python_function']['loader_module']

        # TODO use regex
        mlflow_module = loader_module.split('.')[1]  # mlflow.spark --> spark
        import_module(loader_module)  # import the specified mlflow retriever module

        model_obj = getattr(mlflow, mlflow_module).load_model(from_dir)
        self.logger.info("Registering Raw Model Representation...", send_db=True)
        self.model.add_representation(name=Representations.LIBRARY, representation=model_obj)
        return model_obj

    def create_alternate_representations(self):
        """
        Create alternate representations of the machine learning
        model, including serialized and library specific ones
        """
        if not self.model.get_representation(name='library'):
            raise Exception("Raw model must be retrieved before alternate representations can be generated")
        self.logger.info("Creating Alternative/Library Specific Model Representations")
        self.representation_generator()

    def _load_into_java_bytearray(self, obj) -> bytearray:
        """
        Load an object into a java bytestream
        :param obj: the object to write
        :return: the java bytearray
        """
        if not self.java_jvm:
            raise Exception("Model Serialization requires a JVM, but none was specified in the constructor")

        self.logger.info("Loading Object into Java Bytes")
        byte_output_stream = self.java_jvm.java.io.ByteArrayOutputStream()
        object_output_stream = self.java_jvm.java.io.ObjectOutputStream(byte_output_stream)
        object_output_stream.write(obj)
        object_output_stream.flush()
        object_output_stream.close()
        return byte_output_stream.toByteArray()

    def _create_alternate_h2o(self, model):
        """
        Serialize H2O model to bytearray
        :return: bytearray
        """
        self.logger.info("Creating Alternative Representations for H2O...")
        java_import(self.java_jvm, "java.io.{BinaryOutputStream, ObjectOutputStream, ByteArrayInputStream}")
        java_import(self.java_jvm, "hex.genmodel.easy.EasyPredictModelWrapper")
        java_import(self.java_jvm, "hex.genmodel.MojoModel")
        with TemporaryDirectory() as tmpdir:
            model_path = self.model.get_representation(Representations.LIBRARY). \
                download_mojo(f'/{tmpdir}/h2o_model.zip')
            raw_mojo = self.java_jvm.MojoModel.load(model_path)
            java_mojo_config = self.java_jvm.EasyPredictModelWrapper.Config().setModel(raw_mojo)
            java_mojo = self.java_jvm.EasyPredictModelWrapper(java_mojo_config)

            # Register H2O Model Representations
            self.logger.info("Adding Library Specific Representations...", send_db=True)
            self.model.add_representation(Representations.JAVA_MOJO, java_mojo)
            self.model.add_representation(Representations.RAW_MOJO, raw_mojo)
            self.logger.info("Adding Serialized Representation", send_db=True)
            self.model.add_representation(Representations.BYTES, self._load_into_java_bytearray(java_mojo))

    def _create_alternate_sklearn(self, model):
        """
        Serialize a Scikit model to a bytearray
        :return: bytearray
        """
        from cloudpickle import dumps as save_cloudpickle
        self.logger.info("Creating Alternative Scikit Representations", send_db=True)
        self.logger.info("Registering Serialized Representation", send_db=True)
        self.model.add_representation(Representations.BYTES, save_cloudpickle(
            self.model.get_representation(Representations.LIBRARY)
        ))

    def _create_alternate_keras(self):
        """
        Serialize a Keras model to a bytearray
        :return: bytearray
        """
        from tensorflow.keras.models import save_model
        self.logger.info("Creating Alternative Keras Representations", send_db=True)
        h5_buffer = BytesIO()
        save_model(model=self.model.get_representation(Representations.LIBRARY), filepath=h5_buffer)
        h5_buffer.seek(0)
        self.logger.info("Registering Serialized Representation", send_db=True)
        self.model.add_representation(Representations.BYTES, h5_buffer.read())

    def _create_alternate_spark(self):
        """
        Serialize a Spark model to a bytearray
        :param model: model to serialize
        :return: bytearray
        """
        from mleap.pyspark.spark_support import SimpleSparkSerializer
        SimpleSparkSerializer()
        library_representation = self.model.get_representation(Representations.LIBRARY)
        with TemporaryDirectory() as tmpdir:
            try:
                self.logger.info("Saving Spark Representation to MLeap Format", send_db=True)
                library_representation.serializeToBundle(
                    f"jar:file://{tmpdir}/mleap_model.zip",
                    library_representation.transform(self.model.get_metadata(Metadata.DATAFRAME_EXAMPLE))
                )
                self.logger.info("Done.")
            except Exception as e:
                self.logger.exception("Encountered Exception while converting model to bundle")
                self.logger.error("Encountered Unknown Exception while processing...", send_db=True)
                raise e from None

            java_import(self.java_jvm, 'com.splicemachine.fileretriever.FileRetriever')  # TODO need the MLeap Jars
            java_mleap_bundle = self.java_jvm.FileRetriever.loadBundle(f"jar:file://{tmpdir}/mleap_model.zip")

            self.logger.info("Adding Library Specific Representations...", send_db=True)
            self.model.add_representation(Representations.MLEAP, java_mleap_bundle)
            self.logger.info("Adding Serialized Representations...", send_db=True)
            self.model.add_representation(Representations.BYTES, self._load_into_java_bytearray(java_mleap_bundle))
