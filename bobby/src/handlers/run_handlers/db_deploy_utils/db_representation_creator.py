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

        java_import(self.java_jvm, "java.io.{BinaryOutputStream, ObjectOutputStream, ByteArrayInputStream}")

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
            mlmodel = safe_load(ml_model.read())
        try:
            loader_module = mlmodel['flavors']['python_function']['loader_module']
        except KeyError: # If the python_function isn't available, fallback and try the raw model flavor
            # We will look through the other flavors in the MLModel yaml
            loader_module = None
            for flavor in mlmodel['flavors'].keys():
                if hasattr(mlflow, flavor):
                    loader_module = f'mlflow.{flavor}'
                    self.logger.info(f'Found module {loader_module}', send_db=True)
                    break
            if not loader_module:
                self.logger.exception("Unable to load the mlflow loader. "
                                      "Ensure this ML model has been saved using an mlflow module", send_db=True)
                raise Exception(f"Unable to load the mlflow loader. Ensure this ML model has been "
                                f"saved using an mlflow module") from None

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

    def _load_into_java_bytearray(self, obj, as_object=False) -> bytearray:
        """
        Load an object into a java bytestream
        :param obj: the object to write
        :param as_object: whether or not to write it as an object to the stream
        :return: the java bytearray
        """
        if not self.java_jvm:
            raise Exception("Model Serialization requires a JVM, but none was specified in the constructor")

        self.logger.info("Loading Object into Java Bytes")
        byte_output_stream = self.java_jvm.java.io.ByteArrayOutputStream()
        object_output_stream = self.java_jvm.java.io.ObjectOutputStream(byte_output_stream)
        object_output_stream.writeObject(obj)
        object_output_stream.flush()
        object_output_stream.close()
        return byte_output_stream.toByteArray()

    def _create_alternate_h2o(self):
        """
        Serialize H2O model to bytearray
        :return: bytearray
        """
        self.logger.info("Creating Alternative Representations for H2O...")
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

    def _create_alternate_sklearn(self):
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
        self.logger.info("Creating Alternative Keras Representations", send_db=True)
        with TemporaryDirectory() as tmpdir:
            self.model.get_representation(Representations.LIBRARY).save(f'{tmpdir}/keras.h5')

            with open(f'{tmpdir}/keras.h5', 'rb') as model:
                buffer = bytearray(bytes(model.read()))

        self.logger.info("Registering Serialized Representation", send_db=True)
        self.model.add_representation(Representations.BYTES, buffer)

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
