from pyspark.ml import PipelineModel
from pyspark.ml import classification as spark_classification
from pyspark.ml import clustering as spark_clustering
from pyspark.ml import recommendation as spark_recommendation
from pyspark.ml import regression as spark_regression
from pyspark.ml.feature import IndexToString
from shared.models.model_types import Metadata, Representations
from typing import List, Dict
from shared.models.model_types import SparkModelType


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
        Get the Model stage of a fit pipeline
        :param pipeline_or_model: pipeline to locate stage in
        :return: model in pipeline if it exists
        """
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
        elif hasattr(model, 'k'):
            try:
                return model.summary.k
            except RuntimeError:
                return model.getOrDefault('k')
        else:
            raise Exception('Unable to determine the number of classes of this model. If this is a model that should '
                            'have classes, please raise an issue at https://github.com/splicemachine/pysplice/issues')


    @staticmethod
    def get_model_type(model):
        """
        Get the model type of a spark model
        :param model: the specified model get the type of
        :return: the model type
        """
        probability_check = SparkModelType.MULTI_PRED_INT if 'probabilityCol' in model.explainParams() \
            else SparkModelType.SINGLE_PRED_INT

        return {
            'pyspark.ml.classification': probability_check,
            'pyspark.ml.regression': SparkModelType.SINGLE_PRED_DOUBLE,
            'pyspark.ml.clustering': probability_check
        }[model.__module__]

    @staticmethod
    def do_run_model(sparkmodel, dataframe):
        sparkmodel.transform(dataframe)

    @staticmethod
    def try_run_model(model):
        """
        Try to run the spark model on the provided df example (selecting model cols if provided).
        We do this for spark because spark models are case sensitive on columns :( so failure is easy. Better to fail
        here than after deployment.
        This function will try 3 things, and throw an exception if all fail:
            * Transform the created dataframe (selecting the model cols if provided)
            * Transform the created dataframe with all columns upper cased
            * Transform the created dataframe with all columns lower cased
        If 2 or 3 works, this function will then modify the provided dataframe and model_cols to match case. If all fail
        This function will throw an exception.

        :param model: model representation holder
        :return: None
        """
        # Try to run the model
        df = model.get_metadata(Metadata.DATAFRAME_EXAMPLE)
        cols: List[str] = model.get_metadata(Metadata.MODEL_COLUMNS) or df.columns
        spark_model: PipelineModel = model.get_representation(Representations.LIBRARY)
        try:
            spark_model.transform(df.select(cols))
            return # baseline worked
        except:
            pass
        # Regular failed, try uppercase
        cols = [i.upper() for i in cols]
        for i in df.columns:
            df = df.withColumnRenamed(i, i.upper())
        try:
            spark_model.transform(df.select(cols))
            # modify model to change representation to uppercase
            sql_schema: Dict[str,str] = model.get_representation(Metadata.SQL_SCHEMA)
            model.add_representation(Metadata.SQL_SCHEMA, {i.upper(): sql_schema[i] for i in sql_schema})
            model.add_representation(Metadata.MODEL_COLUMNS, cols)
        except:
            pass
        # Last chance, lowercase
        cols = [i.lower() for i in cols]
        for i in df.columns:
            df = df.withColumnRenamed(i, i.upper())
        try:
            spark_model.transform(df.select(cols))
            # modify model to change representation to uppercase
            sql_schema: Dict[str,str] = model.get_representation(Metadata.SQL_SCHEMA)
            model.add_representation(Metadata.SQL_SCHEMA, {i.lower(): sql_schema[i] for i in sql_schema})
            model.add_representation(Metadata.MODEL_COLUMNS, cols)
        except:
            raise Exception("You've tried to deploy a Spark Model, but the case sensitivity of the inputs don't match"
                            "the models expected inputs. Spark Models are case sensitive, so either the model_cols parameter,"
                            "spark DF example, or reference schema.table must have the matching case. Or, retrain the model"
                            "using the reference (df or table) you provided in the function.")





