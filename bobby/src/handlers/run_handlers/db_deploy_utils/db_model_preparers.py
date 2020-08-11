"""
Class to prepare database models for deployment
to Splice Machine
"""
from typing import List
from collections import namedtuple

from shared.models.enums import FileExtensions
from shared.models.model_types import SparkModelType, KerasModelType, SklearnModelType
from shared.logger.logging_config import logger

from .preparation.spark_utils import SparkUtils
from .preparation.keras_utils import KerasUtils
from .preparation.sklearn_utils import ScikitUtils


class DatabaseModelMetadataPreparer:
    """
    Prepare models for deployment
    """

    def __init__(self, file_ext, raw_model, classes: List, library_specific: dict):
        """
        :param file_ext: File extension associated with stored model
        :param raw_model: the raw model (not MLModel)
        :param classes: classes that can be predicted
        :param library_specific: dictionary of library specific parameters
        """
        self.raw_model = raw_model
        self.classes = classes
        self.library_specific = library_specific
        self.model_type = None

        self.preparer = {
            FileExtensions.sklearn: self._prepare_sklearn,
            FileExtensions.keras: self._prepare_keras,
            FileExtensions.h2o: self._prepare_h2o,
            FileExtensions.spark: self._prepare_spark
        }[file_ext]

    def prepare(self):
        """
        Prepare model metadata for database deployment
        """
        self.preparer()
        return self  # only mutating instance variables here

    def _prepare_spark(self):
        """
        Prepare spark metadata model for deployment
        """
        model_stage = SparkUtils.locate_model(self.raw_model)
        self.model_type = SparkUtils.get_model_type(model_stage)

        if self.classes:
            if self.model_type not in SparkModelType.get_class_supporting_types():
                logger.warning("Labels were specified, but the model type deployed does not support them. Ignoring...")
                self.classes = None
            else:
                self.classes = [cls.replace(' ', '_') for cls in self.classes]  # remove spaces in class names
                logger.info(f"Labels found. Using {self.classes} as labels for predictions 0-{len(self.classes)}"
                            " respectively")
        else:
            if self.model_type in SparkModelType.get_class_supporting_types():  # Add columns for each class (unnamed)
                self.classes = [f'C{label_idx}' for label_idx in range(SparkUtils.get_num_classes(model_stage))]

    def _prepare_keras(self):
        """
        Prepare keras model metadata for deployment
        """
        KerasUtils.validate_keras_model(self.raw_model)
        pred_threshold = self.library_specific.get('keras_pred_threshold')
        self.model_type = KerasUtils.get_keras_model_type(
            model=self.raw_model, pred_threshold=pred_threshold)

        if self.model_type == KerasModelType.KEY_VALUE:
            output_shape = self.raw_model.layers[-1].output_shape
            if not self.classes:
                self.classes = ['prediction'] + [f'C{i}' for i in range(len(output_shape[-1]))]
            else:
                self.classes += ['prediction']

            if len(self.classes) > 2 and pred_threshold:
                logger.warning("Found multiclass model with prediction threshold specified... Ignoring threshold.")

    def _prepare_sklearn(self):
        """
        Prepare a Scikit-learn
        :return:
        """
        sklearn_args = ScikitUtils.validate_scikit_args(model=self.raw_model,
                                                        lib_specific_args=self.library_specific)
        self.model_type = ScikitUtils.get_model_type(model=self.raw_model, lib_specific_args=sklearn_args)

        if self.classes:
            if self.model_type == SklearnModelType.KEY_VALUE:
                self.classes = [cls.replace(' ', '_') for cls in self.classes]
            else:
                logger.warning("Prediction classes were specified, but model is not classification... Ignoring classes")
                self.classes = None
        else:
            if self.model_type == SklearnModelType.KEY_VALUE:
                model_params = self.raw_model.get_params()
                if sklearn_args.get('predict_call') == 'transform' and hasattr(self.raw_model, 'transform'):
                    self.classes = [f'C{i}' for i in range(model_params.get('n_clusters') or
                                                           model_params.get('n_components') or 2)]
                elif 'predict_args' in sklearn_args:
                    self.classes = ['prediction', sklearn_args.get('predict_args').lstrip('return_')]
                elif hasattr(self.raw_model, 'classes_') and self.raw_model.classes_.size != 0:
                    self.classes = [f'C{i}' for i in range(self.raw_model.classes_)]
                elif hasattr(self.raw_model, 'get_params') and (
                        hasattr(self.raw_model, 'n_components') or hasattr(self.raw_model, 'n_clusters')):
                    self.classes = [f'C{i}' for i in range(model_params.get('n_clusters') or
                                                           model_params.get('n_components'))]
                else:
                    raise Exception("Could not locate classes from the model. Pass in classes parameter.")

        if sklearn_args.get('predict_call') == 'predict_proba':
            self.classes.insert(0, 'prediction')

    def _prepare_h2o(self):
        """

        :return:
        """
