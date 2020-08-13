from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString
from pyspark.ml import classification as spark_classification, regression as spark_regression, \
    clustering as spark_clustering, recommendation as spark_recommendation

from shared.shared.models.model_types import SparkModelType


class SparkUtils:
    """
    Utilities for preparing Spark MLlib
    models for database deployment
    """
    MODEL_MODULES = [spark_classification.__name__, spark_recommendation.__name__, spark_clustering.__name__,
                     spark_regression.__name__]

    @staticmethod
    def locate_model(pipeline_or_model):
        """
        Get the Model stage of a fit pipeline, or return the model
        :param pipeline_or_model: pipeline to locate stage in
        :return: model in pipeline if it exists
        """
        if not isinstance(pipeline_or_model, PipelineModel):
            return pipeline_or_model

        try:
            filter_func = lambda stage: getattr(stage, '__module__', None) in SparkUtils.MODEL_MODULES
            return list(filter(filter_func, pipeline_or_model.stages))[0]
        except IndexError:
            raise AttributeError("Could not locate model within the logged pipeline")

    @staticmethod
    def try_get_class_labels(pipeline: PipelineModel, prediction_col: str):
        """
        Try to get the class labels if we can find an index to string
        :param pipeline: fitted piepline
        :param prediction_col: the model prediction column in pipeline
        :return: labels if possible
        """
        if not isinstance(pipeline, PipelineModel):
            return

        for stage in reversed(pipeline.stages):
            if isinstance(stage, IndexToString) and stage.getOrDefault('inputCol') == prediction_col:
                return stage.getOrDefault('labels')

    @staticmethod
    def get_num_classes(model):
        """
        Tries to find the number of classes in a Pipeline or Model object
        :param model: spark model
        :return the number of classes
        """
        if hasattr(model, 'numClasses') or model.hasParam('numClasses'):
            return model.numClasses
        return model.summary.k

    @staticmethod
    def get_model_type(model):
        """
        Get the model type of a spark model
        :param model: the specified model get the type of
        :return: the model type
        """
        return {
            'pyspark.ml.classification': lambda sm: SparkModelType.MULTI_PRED_INT,
            'pyspark.ml.regression': lambda sm: SparkModelType.SINGLE_PRED_DOUBLE,
            'pyspark.ml.clustering': lambda sm: SparkModelType.MULTI_PRED_INT if
            'probabilityCol' in model.explainParams() else SparkModelType.SINGLE_PRED_INT
        }[model.__module__](model)
