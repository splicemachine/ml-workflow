"""
Class to prepare database models for deployment
to Splice Machine
"""
from typing import List

from .entities.db_model import Model
from shared.logger.logging_config import logger
from shared.models.enums import FileExtensions
from shared.models.model_types import (H2OModelType, KerasModelType,
                                              Metadata, ModelTypeMapper,
                                              Representations,
                                              SklearnModelType, SparkModelType)

from .preparation.h2o_utils import H2OUtils
from .preparation.keras_utils import KerasUtils
from .preparation.sklearn_utils import ScikitUtils
from .preparation.spark_utils import SparkUtils


class DatabaseModelMetadataPreparer:
    """
    Prepare models for deployment
    """

    def __init__(self, file_ext, model: Model, classes: List, library_specific: dict, logger=logger):
        """
        :param file_ext: File extension associated with stored model
        :param model: model representation holder
        :param classes: classes that can be predicted
        :param library_specific: dictionary of library specific parameters
        :param logger: logger override
        """
        self._classes = classes
        self.library_specific = library_specific
        self.model = model
        self.model_type = None
        self.logger = logger

        self.preparer = {
            FileExtensions.sklearn: self._prepare_sklearn,
            FileExtensions.keras: self._prepare_keras,
            FileExtensions.h2o: self._prepare_h2o,
            FileExtensions.spark: self._prepare_spark
        }[file_ext]

    def add_metadata(self):
        """
        Prepare model metadata for database deployment
        """
        self.logger.info("Preparing Model Metadata for Deployment...", send_db=True)
        self.preparer()
        # Register Model Metadata
        self.model.add_metadata(Metadata.CLASSES, self._classes)
        self.model.add_metadata(Metadata.TYPE, self.model_type)
        self.model.add_metadata(Metadata.GENERIC_TYPE, ModelTypeMapper.get_model_type(self.model_type))
        # Constant for every model
        self.model.add_metadata(Metadata.RESERVED_COLUMNS, ['EVAL_TIME', 'CUR_USER', 'RUN_ID', 'PREDICTION'])

    def _prepare_spark(self):
        """
        Prepare spark metadata model for deployment
        """
        self.logger.info("Preparing Spark Metadata for deployment", send_db=True)
        library_representation = self.model.get_representation(Representations.LIBRARY)
        model_stage = SparkUtils.locate_model(library_representation)
        self.model_type = SparkUtils.get_model_type(model_stage)
        self._classes = self._classes or SparkUtils.try_get_class_labels(
            library_representation, prediction_col=model_stage.getOrDefault('predictionCol'))

        self.logger.info(f"Classes: {self._classes} were specified", send_db=True)

        if self._classes:
            if self.model_type not in {SparkModelType.SINGLE_PRED_INT, SparkModelType.MULTI_PRED_INT}:
                self.logger.warning("Labels were specified, but the model type deployed does not support them. "
                                    "Ignoring...", send_db=True)
                self._classes = None
            else:
                self._classes = [cls.replace(' ', '_') for cls in self._classes]  # remove spaces in class names
                self.logger.info(f"Labels found. Using {self._classes} as labels for predictions 0-{len(self._classes)-1}"
                                 " respectively", send_db=True)
            if len(self._classes) != SparkUtils.get_num_classes(model_stage):
                self.logger.warning(f"You've passed in {len(self._classes)} classes but it looks like your model expects "
                                    f"{SparkUtils.get_num_classes(model_stage)} classes. You will likely see issues with "
                                    f"model inference.", send_db=True)
        else:
            if self.model_type in {SparkModelType.SINGLE_PRED_INT, SparkModelType.MULTI_PRED_INT}:
                self._classes = [f'C{label_idx}' for label_idx in range(SparkUtils.get_num_classes(model_stage))]
                self.logger.warning(f"No classes were specified, so using {self._classes} as fallback...", send_db=True)

    def _prepare_keras(self):
        """
        Prepare keras model metadata for deployment
        """
        self.logger.info("Preparing Keras Metadata for Deployment...", send_db=True)
        library_model = self.model.get_representation(Representations.LIBRARY)
        KerasUtils.validate_keras_model(library_model)
        pred_threshold = self.library_specific.get('pred_threshold')

        self.model_type = KerasUtils.get_keras_model_type(
            model=library_model, pred_threshold=pred_threshold
        )

        if self.model_type == KerasModelType.MULTI_PRED_DOUBLE:
            output_shape = library_model.layers[-1].output_shape
            if not self._classes:
                self._classes = ['prediction'] + [f'C{i}' for i in range(output_shape[-1])]
                self.logger.info(f"Classes were not specified... using {self._classes} as fallback", send_db=True)
            else:
                self.logger.info(f"Classes were specified... using {self._classes}", send_db=True)
                if len(self._classes) != output_shape[-1]:
                    self.logger.warning(f"You've passed in {len(self._classes)} classes but it looks like your model expects "
                                    f"{output_shape[-1]} classes. You will likely see issues with model inference.",send_db=True)
                self._classes.insert(0, 'prediction')

            if len(self._classes) > 2 and pred_threshold:
                self.logger.warning("Found multiclass model with prediction threshold specified... Ignoring "
                                    "threshold.", send_db=True)

    def _prepare_sklearn(self):
        """
        Prepare Scikit-learn metadata for deployment
        """
        self.logger.info("Prepare Scikit-learn metadata for deployment", send_db=True)
        library_model = self.model.get_representation(Representations.LIBRARY)
        sklearn_args = ScikitUtils.validate_scikit_args(model=library_model,
                                                        lib_specific_args=self.library_specific)
        self.model_type = ScikitUtils.get_model_type(model=library_model, lib_specific_args=sklearn_args)
        if self._classes:
            if self.model_type == SklearnModelType.MULTI_PRED_DOUBLE:
                self._classes = [cls.replace(' ', '_') for cls in self._classes]
                if hasattr(library_model, 'classes_') and library_model.classes_.size != 0:
                    if library_model.classes_.size != len(self._classes):
                        self.logger.error(f"Provided {len(self._classes)} classes but model has "
                                          f"{library_model.classes_.size}", send_db=True)
                        raise Exception(f"Provided {len(self._classes)} classes but model has {library_model.classes_.size}")
                    else:
                        self.logger.info(f"Classes provided. Using {self._classes} for model classes "
                                         f"{library_model.classes_}", send_db=True)
                else:
                    self.logger.info(f"Using classes {self._classes} for classes")
            else:
                self.logger.warning("Prediction classes were specified, but model is not classification... "
                                    "Ignoring classes", send_db=True)
                self._classes = None
        else:
            if self.model_type == SklearnModelType.MULTI_PRED_DOUBLE:
                model_params = library_model.get_params()
                if sklearn_args.get('predict_call') == 'transform' and hasattr(library_model, 'transform'):
                    self._classes = [f'C{cls}' for cls in range(model_params.get('n_clusters') or
                                                                model_params.get('n_components') or 2)]
                    self.logger.info(f"Using transform operation with classes {self._classes}", send_db=True)
                elif 'predict_args' in sklearn_args:
                    self._classes = ['prediction', sklearn_args.get('predict_args').lstrip('return_')]
                    self.logger.info(f"Found predict arguments... classes are {self._classes}", send_db=True)
                elif hasattr(library_model, 'classes_') and library_model.classes_.size != 0:
                    self._classes = [f'C{cls}' for cls in library_model.classes_]
                    self.logger.info(f"Using model provided classes: {self._classes}", send_db=True)
                elif hasattr(library_model, 'get_params') and (hasattr(library_model, 'n_components') or
                                                               hasattr(library_model, 'n_clusters')):
                    self._classes = [f'C{cls}' for cls in range(model_params.get('n_clusters') or
                                                                model_params.get('n_components'))]
                    self.logger.info(f"Using Dimensional Reduction Classes: {self._classes}", send_db=True)
                else:
                    self.logger.error("Could not locate classes. Pass in classes manually.", send_db=True)
                    raise Exception("Could not locate classes from the model. Pass in classes parameter.")

        if sklearn_args.get('predict_call') == 'predict_proba':
            self._classes.insert(0, 'prediction')

    def _prepare_h2o(self):
        """
        Prepare H2O Metadata for Model Deployment
        """
        self.logger.info("Preparing H2O Model metadata for deployment", send_db=True)
        model_category = H2OUtils.get_model_category(
            self.model.get_representation(Representations.JAVA_MOJO)
        )
        self.model_type = H2OUtils.get_category_type(model_category)

        if self._classes:
            if self.model_type not in {H2OModelType.MULTI_PRED_DOUBLE, H2OModelType.MULTI_PRED_INT}:
                self.logger.warning("Classes were specified, but model does not support them... Ignoring.",
                                    send_db=True)
                self._classes = None
            else:
                self._classes = [cls.replace(' ', '_') for cls in self._classes]
                self.logger.info(f"Using {self._classes} as labels for predictions", send_db=True)
        else:
            raw_mojo = self.model.get_representation(Representations.RAW_MOJO)
            if self.model_type == H2OModelType.MULTI_PRED_INT:
                self._classes = [f'C{cls}' for cls in list(raw_mojo.getDomainValues(raw_mojo.getResponseIdx()))]
            elif self.model_type == H2OModelType.MULTI_PRED_DOUBLE:
                self._classes = {
                    'AutoEncoder': lambda: [f'{cls}_reconstr' for cls in list(raw_mojo.getNames()) + (
                        ['MSE'] if 'DeeplearningMojoModel' in raw_mojo.getClass().toString() else [])],
                    'TargetEncoder': lambda: [f'{cls}_te' for cls in list(raw_mojo.getNames()) if
                                              cls != raw_mojo.getResponseName()],
                    'DimReduction': lambda: [
                        f'PC{k}' for k in range(
                            self.model.get_representation(Representations.LIBRARY).actual_params['k'])
                    ],
                    'AnomalyDetection': lambda: ['score', 'normalizedScore'],
                    'WordEmbedding': lambda: [f'{name}_C{idx}' for idx in range(raw_mojo.getVecSize()) for name in
                                              raw_mojo.getNames()]

                }[model_category]()
            self.logger.info(f"Using Classes: {self._classes}")
