"""
Class to prepare database models for deployment
to Splice Machine
"""
from typing import List

from shared.models.enums import FileExtensions
from shared.shared.models.model_types import SparkModelType, KerasModelType, SklearnModelType, H2OModelType, \
    ModelTypeMapper, Representations, Metadata
from shared.logger.logging_config import logger

from .preparation.spark_utils import SparkUtils
from .preparation.keras_utils import KerasUtils
from .preparation.sklearn_utils import ScikitUtils
from .preparation.h2o_utils import H2OUtils
from bobby.src.handlers.run_handlers.db_deploy_utils.entities.db_model import Model


class DatabaseModelMetadataPreparer:
    """
    Prepare models for deployment
    """

    def __init__(self, file_ext, model: Model, classes: List, library_specific: dict):
        """
        :param file_ext: File extension associated with stored model
        :param model: model representation holder
        :param classes: classes that can be predicted
        :param library_specific: dictionary of library specific parameters
        """
        self._classes = classes
        self.library_specific = library_specific
        self.model = model
        self.model_type = None

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
        self.preparer()
        # Register Model Metadata
        self.model.add_metadata(Metadata.CLASSES, self._classes)
        self.model.add_metadata(Metadata.TYPE, self.model_type)
        self.model.add_metadata(Metadata.GENERIC_TYPE, ModelTypeMapper.get_model_type(self.model_type))

    def _prepare_spark(self):
        """
        Prepare spark metadata model for deployment
        """
        model_stage = SparkUtils.locate_model(self.model.get_representation(Representations.LIBRARY))
        self.model_type = SparkUtils.get_model_type(model_stage)

        if self._classes:
            if self.model_type not in SparkModelType.get_class_supporting_types():
                logger.warning("Labels were specified, but the model type deployed does not support them. Ignoring...")
                self._classes = None
            else:
                self._classes = [cls.replace(' ', '_') for cls in self._classes]  # remove spaces in class names
                logger.info(f"Labels found. Using {self._classes} as labels for predictions 0-{len(self._classes)}"
                            " respectively")
        else:
            if self.model_type in SparkModelType.get_class_supporting_types():  # Add columns for each class (unnamed)
                self._classes = [f'C{label_idx}' for label_idx in range(SparkUtils.get_num_classes(model_stage))]

    def _prepare_keras(self):
        """
        Prepare keras model metadata for deployment
        """
        library_model = self.model.get_representation(Representations.LIBRARY)
        KerasUtils.validate_keras_model(library_model)
        pred_threshold = self.library_specific.get('pred_threshold')
        self.model_type = KerasUtils.get_keras_model_type(
            model=library_model, pred_threshold=pred_threshold)

        if self.model_type == KerasModelType.MULTI_PRED_DOUBLE:
            output_shape = library_model.layers[-1].output_shape
            if not self._classes:
                self._classes = ['prediction'] + [f'C{i}' for i in range(len(output_shape[-1]))]
            else:
                self._classes += ['prediction']

            if len(self._classes) > 2 and pred_threshold:
                logger.warning("Found multiclass model with prediction threshold specified... Ignoring threshold.")

    def _prepare_sklearn(self):
        """
        Prepare Scikit-learn metadata for deployment
        """
        library_model = self.model.get_representation(Representations.LIBRARY)
        sklearn_args = ScikitUtils.validate_scikit_args(model=library_model,
                                                        lib_specific_args=self.library_specific)
        self.model_type = ScikitUtils.get_model_type(model=library_model, lib_specific_args=sklearn_args)

        if self._classes:
            if self.model_type == SklearnModelType.MULTI_PRED_DOUBLE:
                self._classes = [cls.replace(' ', '_') for cls in self._classes]
            else:
                logger.warning("Prediction classes were specified, but model is not classification... Ignoring classes")
                self._classes = None
        else:
            if self.model_type == SklearnModelType.MULTI_PRED_DOUBLE:
                model_params = library_model.get_params()
                if sklearn_args.get('predict_call') == 'transform' and hasattr(library_model, 'transform'):
                    self._classes = [f'C{cls}' for cls in range(model_params.get('n_clusters') or
                                                                model_params.get('n_components') or 2)]
                elif 'predict_args' in sklearn_args:
                    self._classes = ['prediction', sklearn_args.get('predict_args').lstrip('return_')]
                elif hasattr(library_model, 'classes_') and library_model.classes_.size != 0:
                    self._classes = [f'C{cls}' for cls in range(library_model.classes_)]
                elif hasattr(library_model, 'get_params') and (hasattr(library_model, 'n_components') or
                                                               hasattr(library_model, 'n_clusters')):
                    self._classes = [f'C{cls}' for cls in range(model_params.get('n_clusters') or
                                                                model_params.get('n_components'))]
                else:
                    raise Exception("Could not locate classes from the model. Pass in classes parameter.")

        if sklearn_args.get('predict_call') == 'predict_proba':
            self._classes.insert(0, 'prediction')

    def _prepare_h2o(self):
        """
        Prepare H2O Metadata for Model Deployment
        """
        model_category = H2OUtils.get_model_category(
            self.model.get_representation(Representations.JAVA_MOJO)
        )
        self.model_type = H2OUtils.get_category_type(model_category)

        if self._classes:
            if self.model_type not in {H2OModelType.MULTI_PRED_DOUBLE, H2OModelType.MULTI_PRED_INT}:
                logger.warning("Classes were specified, but model does not support them... Ignoring.")
                self._classes = None
            else:
                self._classes = [cls.replace(' ', '_') for cls in self._classes]
                logger.info(f"Using {self._classes} as labels for predictions")
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
                    'DimReduction': lambda: [f'PC{k}' for k in
                                             range(self.model.get_representation(Representations.LIBRARY))],
                    'AnomalyDetection': lambda: ['score', 'normalizedScore']
                }[model_category]()
